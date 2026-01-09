//
// Created by 19327 on 2025/12/31/星期三.
//
#include "server.h"
#include "log.h"
#include "resp.h"
#include "aof.h"
#include "rdb.h"
#include "kv.h"
#include "replicate_client.h"


#include <csignal>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/uio.h>
#include <fcntl.h>

namespace train_set {

    namespace {
        int set_noBlocking(int fd) {
            int flags = fcntl(fd, F_GETFL, 0);
            if (flags < 0) {
                return -1;
            }
            if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
                return -1;
            }
            return 0;
        }

        int add_epoll(int epoll_fd, int fd, uint32_t events) {
            epoll_event ev{};
            ev.data.fd = fd;
            ev.events = events;
            return epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev);
        }

        int mod_epoll(int epoll_fd, int fd, uint32_t events) {
            epoll_event ev{};
            ev.events = events;
            ev.data.fd = fd;
            return epoll_ctl(epoll_fd, EPOLL_CTL_MOD, fd, &ev);
        }

        struct Conn {
            int fd = -1;
            std::string in = "";
            // 待发送块队列;
            std::vector<std::string> out_chunks = {};
            // 当前发送到第几个块;
            size_t out_iov_idx = 0;
            // 当前块内偏移; (n * size)+offset;
            size_t out_off = 0;
            RespParser parser = {};
            bool is_replica = false;
        };
    }

    Server::Server(ServerConfig &config) : config(config) {}

    Server::~Server() {
        if (listen_fd >= 0) {
            close(listen_fd);
        }
        if (epoll_fd >= 0) {
            close(epoll_fd);
        }
    }


    int Server::setListener() {
        listen_fd = ::socket(AF_INET,
                             SOCK_STREAM, 0);
        if (listen_fd < 0) {
            std::perror("socket");
            return -1;
        }

        int yes = 1;
        setsockopt(listen_fd, SOL_SOCKET,
                   SO_REUSEADDR, &yes, sizeof(yes));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t) config.port);
        if (inet_pton(AF_INET, config.host.c_str(),
                      &addr.sin_addr) != 1) {
            MR_LOG("ERROR", "Invalid bind address: " << config.host);
            return -1;
        }
        if (bind(listen_fd, (sockaddr *) &addr,
                 sizeof(addr)) < 0) {
            std::perror("bind");
            return -1;
        }
        if (set_noBlocking(listen_fd) < 0) {
            std::perror("set_noBlocking");
            return -1;
        }
        if (listen(listen_fd, 1024) < 0) {
            std::perror("listen");
            return -1;
        }
        return 1;
    }

    int Server::setEpoll() {
        epoll_fd = epoll_create1(0);
        if (epoll_fd < 0) {
            std::perror("epoll_create1");
            return -1;
        }
        if (add_epoll(epoll_fd, listen_fd,
                      EPOLLIN | EPOLLET) < 0) {
            std::perror("add_epoll");
            return -1;
        }
        time_fd = timerfd_create(
                CLOCK_MONOTONIC,
                TFD_NONBLOCK | TFD_CLOEXEC);
        if (time_fd < 0) {
            std::perror("timerfd_create");
            return -1;
        }
        itimerspec its{};
        its.it_interval.tv_sec = 0;
        its.it_interval.tv_nsec = 200 * 1000 * 1000; // 200ms;
        its.it_value = its.it_interval;
        if (timerfd_settime(time_fd, 0, &its, nullptr) < 0) {
            std::perror("timerfd_settime failed");
            return -1;
        }
        if (add_epoll(epoll_fd, time_fd, EPOLLIN | EPOLLET) < 0) {
            std::perror("epoll_ctl add timer");
            return -1;
        }
        MR_LOG("INFO", "Server started at " << config.host << ":" << config.port);
        return 1;
    }


    KVStorage g_store;
     AOFManager g_aof;
     RDBManager g_rdb;
    // 主节点记录全局命令队列, 随时发往从节点;
    std::vector<std::vector<std::string>> g_repl_queue;

    bool has_pending(Conn &conn) {
        return conn.out_iov_idx < conn.out_chunks.size() ||
               (conn.out_iov_idx == conn.out_chunks.size() && conn.out_off != 0);
    }


    void conn_queue_add(Conn &conn, std::string out) {
        // 将 s 添加到 c 链接 的输出队列中;
        if (!out.empty()) {
            conn.out_chunks.emplace_back(std::move(out));
        }
    }

    // 循环记录: 部分复制日志,只保留最新的部分;
    std::string g_repl_back_part_log;
    size_t g_repl_back_part_cap = 4 * 1024 * 1024;
    int64_t g_back_log_off = 0;
    int64_t g_backlog_start_offset = 0;

    void append_cmd_to_back_log(std::string &cmd) {
        if (g_repl_back_part_log.size() + cmd.size() <=
            g_repl_back_part_cap) {
            g_repl_back_part_log.append(cmd);
        } else {
            size_t need = cmd.size();
            if (need >= g_repl_back_part_cap) {
                g_repl_back_part_log.assign(cmd.data() + (need - g_repl_back_part_cap), g_repl_back_part_cap);
            } else {
                size_t drop = (g_repl_back_part_log.size() + need) - g_repl_back_part_cap;
                g_repl_back_part_log.erase(0, drop);
                g_repl_back_part_log.append(cmd);
            }
        }
        g_backlog_start_offset = g_back_log_off - (int64_t) cmd.size();
    }

    void conn_try_flush_now(int fd, Conn &conn, uint32_t &ev) {
        while (has_pending(conn)) {
            const size_t max_iov = 64;
            iovec iov[max_iov];
            int iov_count = 0;
            size_t idx = conn.out_iov_idx;
            size_t off = conn.out_off;
            while (iov_count < (int) max_iov && idx < conn.out_chunks.size()) {
                std::string &s = conn.out_chunks[idx];
                char *base = s.data();
                size_t len = s.size();
                if (off >= len) {
                    idx++;
                    off = 0;
                    continue;
                } else {
                    iov[iov_count].iov_base = (void *) (base + off);
                    iov[iov_count].iov_len = len - off;
                    off = 0;
                    iov_count++;
                    idx++;
                }
            }// while iov over
            if (iov_count == 0) {
                break;
            }
            ssize_t w = ::writev(fd, iov, iov_count);
            if (w > 0) {
                auto write_len = size_t(w);
                while (write_len > 0 && conn.out_iov_idx < conn.out_chunks.size()) {
                    if (write_len >= conn.out_chunks[conn.out_iov_idx].size()) {
                        write_len -= conn.out_chunks[conn.out_iov_idx].size();
                        conn.out_iov_idx++;
                    } else {
                        conn.out_chunks[conn.out_iov_idx].erase(0, write_len);
                        conn.out_off = 0;
                        write_len = 0;
                    }
                }// while write over
                if (conn.out_iov_idx >= conn.out_chunks.size()) {
                    conn.out_chunks.clear();
                    conn.out_iov_idx = 0;
                    conn.out_off = 0;
                }
            } else if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                // need EPOLLOUT to continue later
                break;
            } else {
                std::perror("writev");
                ev |= EPOLLRDHUP;
                break;
            }
        }// while (has_pending(conn)) over
    }

    std::string handle_cmd(RespValue &r, std::string *raw) {
        if (r.type != RespType::Array || r.array.empty()) {
            return respError("ERR protocol error");;
        }
        auto head = r.array[0];
        if (head.type != RespType::BulkString && head.type != RespType::SimpleString) {
            return respError("ERR unknown command '" + head.bulk_string + "'");
        }
        std::string cmd;
        cmd.reserve(head.bulk_string.size());
        for (char &s: head.bulk_string) {
            cmd.push_back((char) toupper(s));
        }

        if (cmd == "PING") {
            if (r.array.size() <= 1) {
                return respSimpleString("PONG");
            } else if (r.array.size() == 2 && r.array[1].type == RespType::BulkString) {
                return respBulk(r.array[1].bulk_string);
            } else {
                return respError("ERR wrong number of arguments for '" + cmd + "' command");
            }
        }
        if (cmd == "ECHO") {
            if (r.array.size() == 2 && r.array[1].type == RespType::BulkString)
                return respBulk(r.array[1].bulk_string);
            return respError("ERR wrong number of arguments for 'ECHO'");
        }

        if (cmd == "SET") {
            if (r.array.size() < 3) {
                return respError("ERR wrong number of arguments for 'SET'");
            }
            if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString) {
                return respError("ERR value is not a valid string");
            }
            std::optional<int64_t> ttls;
            size_t index = 3;
            while (index < r.array.size()) {
                if (r.array[index].type != RespType::SimpleString) {
                    return respError("ERR syntax error");
                }
                std::string opt;
                opt.resize(r.array[index].bulk_string.size());
                for (char &c: r.array[index].bulk_string) {
                    opt.push_back((char) toupper(c));
                }
                if (opt == "EX") {
                    if (index + 1 >= r.array.size() || r.array[index + 1].type != RespType::BulkString) {
                        return respError("ERR value is not an integer");
                    }
                    try {
                        int64_t sec = std::stoll(r.array[index + 1].bulk_string);
                        if (sec < 0) {
                            return respError("ERR invalid expire time in 'EX'");
                        }
                        ttls = sec * 1000;
                    } catch (...) {
                        return respError("ERR value is not an integer or out of range");
                    }
                    index += 2;
                    continue;
                } else if (opt == "PX") {
                    if (index + 1 >= r.array.size() || r.array[index + 1].type != RespType::BulkString) {
                        return respError("ERR value is not an integer");
                    }
                    try {
                        int64_t ms = std::stoll(r.array[index + 1].bulk_string);
                        if (ms < 0) {
                            return respError("ERR invalid expire time in 'EX'");
                        }
                        ttls = ms;
                    } catch (...) {
                        return respError("ERR value is not an integer or out of range");
                    }
                    index += 2;
                    continue;
                } else if (opt == "NX") {

                } else if (opt == "XX") {

                } else if (opt == "KEEPTTL") {

                } else {
                    return respError("ERR syntax error");
                }
            }// while opt over

            g_store.set(r.array[1].bulk_string, r.array[2].bulk_string, ttls);
            if (raw) {
                g_aof.appendCmdRaw(*raw);
            } else {
                std::vector<std::string> parts;
                for (auto &s: r.array) {
                    parts.emplace_back(s.bulk_string);
                }
                // parts ->
                //  write_queue.push_back(AofItem{std::move(cmd), my_seq});
                //    ->  local.emplace_back(std::move(write_queue.front()));
                g_aof.appendCmd(parts, false);
            }
            {
                std::vector<std::string> parts;
                for (auto &s: r.array) {
                    parts.emplace_back(s.bulk_string);
                }
                g_repl_queue.push_back(std::move(parts));
            }
            return respSimpleString("OK");
        }

        if (cmd == "GET") {
            if (r.array.size() != 2) {
                return respError("ERR wrong number of arguments for 'GET'");
            } else if (r.array[1].type != RespType::BulkString) {
                return respError("ERR value is not a valid string");
            }
            std::optional<std::string> v = g_store.get(r.array[1].bulk_string);
            return v ? respBulk(*v) : respNullBulk();
        }

        if (cmd == "KEYS") {
            std::string pattern;
            if (r.array.size() == 2) {
                if (r.array[1].type == RespType::BulkString || r.array[1].type == RespType::SimpleString) {
                    pattern = r.array[1].bulk_string;
                }
            } else if (r.array.size() != 1) {
                return respError("ERR wrong number of arguments for 'KEYS'");
            }
            std::vector<std::string> keys = g_store.listKeys();
            if (pattern != "*") {
                keys.clear();
                keys.shrink_to_fit();
            }
            std::string out = "*" + std::to_string(keys.size()) + "\r\n";
            for (auto &key: keys) {
                out += respBulk(key);
            }
            return out;
        }

        if (cmd == "FLUSHALL") {
            if (r.array.size() != 1) {
                return respError("ERR wrong number of arguments for 'FLUSHALL'");
            }
            {
                auto kvs = g_store.stringSnapshot();
                std::vector<std::string> keys;
                for (auto &kv: kvs) {
                    keys.push_back(kv.first);
                }
                g_store.del(keys);
            }
            {
                auto ks = g_store.hashSnapshot();
                for (auto &kv: ks) {
                    std::vector<std::string> flds;
                    for (auto &fv: kv.second.field_vals) {
                        flds.push_back(fv.first);
                    }
                    g_store.hdel(kv.first, flds);
                }
            }
            {
                auto ks = g_store.zSetSnapshot();
                for (auto &kv: ks) {
                    std::vector<std::string> ms;
                    for (auto &mv: kv.items) {
                        ms.push_back(mv.second);
                    }
                    g_store.zremove(kv.key, ms);
                }
            }
            if (raw) {
                g_aof.appendCmdRaw(*raw);
            } else {
                g_aof.appendCmd({"FLUSHALL"}, false);
            }
            g_repl_queue.push_back({"FLUSHALL"});
            return respSimpleString("OK");
        }

        if (cmd == "DEL") {
            if (r.array.size() < 2) {
                return respError("ERR wrong number of arguments for 'DEL'");
            }
            std::vector<std::string> keys;
            keys.reserve(r.array.size() - 1);
            for (int i = 1; i < r.array.size(); ++i) {
                if (r.array[i].type != RespType::BulkString) {
                    return respError("ERR value is not a valid string");
                }
                keys.emplace_back(r.array[i].bulk_string);
            }
            int removed = g_store.del(keys);
            if (removed > 0) {
                std::vector<std::string> parts;
                parts.reserve(keys.size() + 1);
                parts.emplace_back("DEL");
                for (auto &key: keys) {
                    keys.emplace_back(key);
                }
                if (raw) {
                    g_aof.appendCmdRaw(*raw);
                } else {
                    g_aof.appendCmd(parts, false);
                }
                g_repl_queue.push_back(parts);
            }
            return respInteger(removed);
        }

        if (cmd == "EXISTS") {
            if (r.array.size() != 2) {
                return respError("ERR wrong number of arguments for 'EXISTS'");

            }
            if (r.array[1].type != RespType::BulkString) {
                return respError("ERR syntax");

            }
            bool ex = g_store.exists(r.array[1].bulk_string);
            return respInteger(ex ? 1 : 0);
        }

        if (cmd == "EXPIRE") {
            if (r.array.size() != 3) {
                return respError("ERR wrong number of arguments for 'EXPIRE'");
            }
            if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            try {
                int64_t seconds = std::stoll(r.array[2].bulk_string);
                bool ok = g_store.expire(r.array[1].bulk_string, seconds);
                if (ok) {
                    if (raw) {
                        g_aof.appendCmdRaw(*raw);
                    } else {
                        g_aof.appendCmd({"EXPIRE", r.array[1].bulk_string, std::to_string(seconds)}, false);
                    }
                }
                if (ok) {
                    g_repl_queue.push_back({"EXPIRE", r.array[1].bulk_string, std::to_string(seconds)});
                }
                return respInteger(ok ? 1 : 0);
            }
            catch (...) {
                return respError("ERR value is not an integer or out of range");
            }
        }

        if (cmd == "TTL") {
            if (r.array.size() != 2) {
                return respError("ERR wrong number of arguments for 'TTL'");
            }
            if (r.array[1].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            int64_t t = g_store.ttl(r.array[1].bulk_string);
            return respInteger(t);
        }

        if (cmd == "HSET") {
            // hset key f1 v1
            if (r.array.size() != 4) {
                return respError("ERR wrong number of arguments for 'HSET'");
            }

            if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString ||
                r.array[3].type != RespType::BulkString) {
                return respError("ERR syntax");
            }

            int created = g_store.hset(r.array[1].bulk_string, r.array[2].bulk_string, r.array[3].bulk_string);
            if (raw) {
                g_aof.appendCmdRaw(*raw);
            } else {
                g_aof.appendCmd({"HSET", r.array[1].bulk_string, r.array[2].bulk_string, r.array[3].bulk_string}, false);
            }
            g_repl_queue.push_back({"HSET", r.array[1].bulk_string, r.array[2].bulk_string, r.array[3].bulk_string});
            return respInteger(created);
        }

        if (cmd == "HGET") {
            if (r.array.size() != 3) {
                return respError("ERR wrong number of arguments for 'HGET'");
            }
            if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            auto val = g_store.hget(r.array[1].bulk_string, r.array[2].bulk_string);
            if (!val.has_value()) {
                return respNullBulk();
            }
            return respBulk(*val);
        }

        if (cmd == "HDEL") {
            if (r.array.size() < 3) {
                return respError("ERR wrong number of arguments for 'HDEL'");
            }
            if (r.array[1].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            std::vector<std::string> fields;
            for (size_t i = 2; i < r.array.size(); ++i) {
                if (r.array[i].type != RespType::BulkString) {
                    return respError("ERR syntax");
                }
                fields.emplace_back(r.array[i].bulk_string);
            }

            int removed = g_store.hdel(r.array[1].bulk_string, fields);
            if (removed > 0) {
                std::vector<std::string> parts;
                parts.reserve(2 + fields.size());
                parts.emplace_back("HDEL");
                parts.emplace_back(r.array[1].bulk_string);
                for (auto &f: fields) {
                    parts.emplace_back(f);

                }
                if (raw) {
                    g_aof.appendCmdRaw(*raw);
                } else {
                    g_aof.appendCmd(parts, false);
                }
                g_repl_queue.push_back(parts);
            }
            return respInteger(removed);
        }
        if (cmd == "HEXISTS") {
            if (r.array.size() != 3)
                return respError("ERR wrong number of arguments for 'HEXISTS'");
            if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            bool ex = g_store.hexists(r.array[1].bulk_string, r.array[2].bulk_string);
            return respInteger(ex ? 1 : 0);
        }

        if (cmd == "HGETALL") {
            if (r.array.size() != 2) {
                return respError("ERR wrong number of arguments for 'HGETALL'");
            }
            if (r.array[1].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            auto flat = g_store.hgetAllByKey(r.array[1].bulk_string);
            RespValue arr;
            arr.type = RespType::Array;
            arr.array.reserve(flat.size());
            std::string out = "*" + std::to_string(flat.size()) + "\r\n";
            for (const auto &s: flat) {
                out += respBulk(s);
            }
            return out;
        }

        if (cmd == "HLEN") {
            if (r.array.size() != 2)
                return respError("ERR wrong number of arguments for 'HLEN'");
            if (r.array[1].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            int n = g_store.hlen(r.array[1].bulk_string);
            return respInteger(n);
        }

        if (cmd == "ZADD") {
            if (r.array.size() != 4) {
                return respError("ERR wrong number of arguments for 'ZADD'");
            }
            if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString ||
                r.array[3].type != RespType::BulkString) {
                return respError("ERR syntax");
            }

            try {
                double sc = std::stod(r.array[2].bulk_string);
                int added = g_store.zadd(r.array[1].bulk_string, sc, r.array[3].bulk_string);
                if (raw) {
                    g_aof.appendCmdRaw(*raw);
                } else {
                    g_aof.appendCmd({"ZADD", r.array[1].bulk_string, r.array[2].bulk_string, r.array[3].bulk_string},false);
                }
                g_repl_queue.push_back(
                        {"ZADD", r.array[1].bulk_string, r.array[2].bulk_string, r.array[3].bulk_string});
                return respInteger(added);
            } catch (...) {
                return respError("ERR value is not a valid float");
            }
        }
        if (cmd == "ZREM") {
            if (r.array.size() < 3) {
                return respError("ERR wrong number of arguments for 'ZREM'");
            }
            if (r.array[1].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            std::vector<std::string> members;
            for (size_t i = 2; i < r.array.size(); ++i) {
                if (r.array[i].type != RespType::BulkString) {
                    return respError("ERR syntax");
                }
                members.emplace_back(r.array[i].bulk_string);
            }
            int removed = g_store.zremove(r.array[1].bulk_string, members);
            if (removed > 0) {
                std::vector<std::string> parts;
                parts.reserve(2 + members.size());
                parts.emplace_back("ZREM");
                parts.emplace_back(r.array[1].bulk_string);
                for (auto &m: members) {
                    parts.emplace_back(m);
                }
                if (raw) {
                    g_aof.appendCmdRaw(*raw);
                } else {
                    g_aof.appendCmd(parts, false);
                }
                g_repl_queue.push_back(parts);
            }
            return respInteger(removed);
        }
        if (cmd == "ZRANGE") {
            if (r.array.size() != 4) {
                return respError("ERR wrong number of arguments for 'ZRANGE'");
            }
            if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString ||
                r.array[3].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            try {
                int64_t start = std::stoll(r.array[2].bulk_string);
                int64_t stop = std::stoll(r.array[3].bulk_string);
                auto members = g_store.zrange(r.array[1].bulk_string, start, stop);
                std::string out = "*" + std::to_string(members.size()) + "\r\n";
                for (const auto &m: members) {
                    out += respBulk(m);
                }
                return out;
            }
            catch (...) {
                return respError("ERR value is not an integer or out of range");
            }
        }
        if (cmd == "ZSCORE") {
            if (r.array.size() != 3) {
                return respError("ERR wrong number of arguments for 'ZSCORE'");
            }
            if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            auto s = g_store.zscore(r.array[1].bulk_string, r.array[2].bulk_string);
            if (!s.has_value())
                return respNullBulk();
            return respBulk(std::to_string(*s));
        }

        if (cmd == "BGSAVE" || cmd == "SAVE") {
            if (r.array.size() != 1)
                return respError("ERR wrong number of arguments for 'BGSAVE'");
            std::string err;
            if (!g_rdb.dump(g_store, err)) {
                return respError(std::string("ERR rdb save failed: ") + err);
            }
            return respSimpleString("OK");
        }

        if (cmd == "BGREWRITEAOF") {
            if (r.array.size() != 1){
                return respError("ERR wrong number of arguments for 'BGREWRITEAOF'");
            }
            std::string err;
            if (!g_aof.isEnabled())
                return respError("ERR AOF disabled");
            if (!g_aof.bgWrite(g_store, err)) {
                return respError(std::string("ERR ") + err);
            }
            return respSimpleString("OK");
        }
        if (cmd == "CONFIG") {
            if (r.array.size() < 2)
                return respError("ERR wrong number of arguments for 'CONFIG'");
            if (r.array[1].type != RespType::BulkString && r.array[1].type != RespType::SimpleString) {
                return respError("ERR syntax");
            }
            std::string sub;
            for (char c: r.array[1].bulk_string) {
                sub.push_back(static_cast<char>(::toupper(c)));
            }

            if (sub == "GET") {
                // 允许 CONFIG GET 与 CONFIG GET <pattern>（未提供时默认 "*")
                std::string pattern = "*";
                if (r.array.size() >= 3) {
                    if (r.array[2].type != RespType::BulkString && r.array[2].type != RespType::SimpleString) {
                        return respError("ERR wrong number of arguments for 'CONFIG GET'");
                    }
                    pattern = r.array[2].bulk_string;
                } else if (r.array.size() != 2) {
                    return respError("ERR wrong number of arguments for 'CONFIG GET'");
                }
                auto match = [&](const std::string &k) -> bool {
                    if (pattern == "*") return true;
                    return pattern == k;
                };
                std::vector<std::pair<std::string, std::string>> kvs;
                // minimal set to satisfy tooling
                kvs.emplace_back("appendonly", g_aof.isEnabled() ? "yes" : "no");
                std::string appendfsync;
                switch (g_aof.mode()) {
                    case AofMode::None:
                        appendfsync = "none";
                        break;
                    case AofMode::EverySec:
                        appendfsync = "everysec";
                        break;
                    case AofMode::Always:
                        appendfsync = "always";
                        break;
                }
                kvs.emplace_back("appendfsync", appendfsync);
                kvs.emplace_back("dir", g_aof.getConfig().dir);
                kvs.emplace_back("dbfilename", g_aof.getConfig().file_name);
                kvs.emplace_back("timeout", std::to_string(g_aof.getConfig().consume_aof_queue_us));
                kvs.emplace_back("databases", "16");
                kvs.emplace_back("maxmemory", "0");
                std::string body;
                size_t elems = 0;

                if (pattern == "*") {
                    for (auto &p: kvs) {
                        body += respBulk(p.first);
                        body += respBulk(p.second);
                        elems += 2;
                    }
                } else {
                    for (auto &p: kvs) {
                        if (match(p.first)) {
                            body += respBulk(p.first);
                            body += respBulk(p.second);
                            elems += 2;
                        }
                    }
                }
                return "*" + std::to_string(elems) + "\r\n" + body;
            } else if (sub == "RESETSTAT") {
                if (r.array.size() != 2)
                    return respError("ERR wrong number of arguments for 'CONFIG RESETSTAT'");
                return respSimpleString("OK");
            } else {
                return respError("ERR unsupported CONFIG subcommand");
            }
        }
        if (cmd == "INFO") {
            // INFO [section] -> ignore section for now
            std::string info;
            info.reserve(512);
            info += "# TrainSet\r\nversion:0.1.0\r\nrole:master\r\n";
            info += "# Clients\r\nconnected_clients:0\r\n";
            info += "# Stats\r\ntotal_connections_received:0\r\ntotal_commands_processed:0\r\ninstantaneous_ops_per_sec:0\r\n";
            info += "# Persistence\r\naof_enabled:";
            info += (g_aof.isEnabled() ? "1" : "0");
            info += "\r\naof_rewrite_in_progress:0\r\nrdb_bg_save_in_progress:0\r\n";
            info += "# Replication\r\nconnected_slaves:0\r\nmaster_repl_offset:" +
                    std::to_string(g_back_log_off) +"\r\n";
            return respBulk(info);
        }
        return respError("ERR unknown command");
    }


    int Server::run() {
        if (setListener() < 0) {
            return -1;
        }
        if (setEpoll() < 0) {
            return -1;
        }
        if (config.rdb_conf.enabled) {
            g_rdb.setConfig(config.rdb_conf);
            std::string err;
            if (!g_rdb.load(g_store, err)) {
                MR_LOG("ERROR", "RDB load failed: " << err);
                return -1;
            }
        }

        if (config.aof_conf.enabled) {
            std::string err;
            if (!g_aof.init(config.aof_conf, err)) {
                MR_LOG("ERROR", "AOF init failed: " << err);
                return -1;
            }
            if (!g_aof.load(g_store, err)) {
                MR_LOG("ERROR", "AOF load failed: " << err);
                return -1;
            }
        }

        ReplicateClient rep_client(config, g_store);
        rep_client.start();
        int rc = epoll_loop();
        rep_client.stop();
        return rc;
    }

    int Server::epoll_loop() {
        std::unordered_map<int, Conn> conns;
        std::vector<epoll_event> events(128);
        while (true) {
            int n = epoll_wait(epoll_fd,
                               events.data(), (int) events.size(), -1);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                std::perror("epoll_wait");
                return -1;
            }
            for (int i = 0; i < n; ++i) {
                auto fd_ = events[i].data.fd;
                uint32_t ev = events[i].events;

                // 连接事件;
                if (fd_ == listen_fd) {
                    while (true) {
                        sockaddr_in in{};
                        socklen_t len = sizeof(in);
                        int cfd = accept(listen_fd, reinterpret_cast<sockaddr *>(&in), &len);
                        if (cfd < 0) {
                            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                                break;
                            }
                            std::perror("accept err");
                            break;
                        }
                        set_noBlocking(cfd);
                        int yes = 1;
                        setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
                        add_epoll(epoll_fd, cfd, EPOLLIN | EPOLLET);
                        conns.emplace(cfd, Conn{cfd, std::string(), std::vector<std::string>(),
                                                0, 0, RespParser(), false});
                    }
                    continue;
                }

                // 时间周期事件;
                if (fd_ == time_fd) {
                    while (true) {
                        uint16_t ticks;
                        ssize_t r = read(time_fd,
                                         &ticks, sizeof(ticks));
                        if (r < 0) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            } else {
                                break;
                            }
                        }
                        if (r == 0) {
                            break;
                        }
                    }
                    // 扫描过期键;
                    g_store.expireScanStep(64);
                    continue;
                }

                auto it = conns.find(fd_);
                if (it == conns.end()) {
                    continue;
                }
                Conn &conn = it->second;

                // epollhup || epollerr
                // 连接挂起, 连接错误时, 迅速删除掉;
                if ((ev & EPOLLHUP) || (ev & EPOLLERR)) {
                    MR_LOG("INFO", "Connection closed: " << conn.fd);
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd_, nullptr);
                    close(fd_);
                    conns.erase(it);
                    continue;
                }// epollhup || epollerr over

                // 可读事件;
                if (ev & EPOLLIN) {
                    char buf[4096];
                    while (true) {
                        ssize_t rn = read(fd_, buf, sizeof(buf));
                        if (rn > 0) {
                            conn.parser.append(std::string_view(buf, (size_t) rn));
                        } else if (rn == 0) {
                            ev |= EPOLLRDHUP;
                            break;
                        } else {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            std::perror("epoll_in read err");
                            ev |= EPOLLRDHUP;
                            break;
                        }
                    }// while read over

                    while (true) {
                        auto maybe = conn.parser.tryParseOneWithRaw();
                        if (!maybe.has_value()) {
                            break;
                        }
                        RespValue &rv = maybe->first;
                        std::string &raw = maybe->second;
                        if (rv.type == RespType::Error) {
                            conn_queue_add(conn, respError("ERR protocol error"));
                        } else {
                            if (rv.type == RespType::Array && !rv.array.empty() &&
                                (rv.array[0].type == RespType::BulkString ||rv.array[0].type == RespType::SimpleString)) {
                                std::string cmd;
                                cmd.reserve(rv.array[0].bulk_string.size());
                                for (auto &c: rv.array[0].bulk_string) {
                                    cmd.push_back((char) ::toupper(c));
                                }

                                // 接收来自 从节点的 PSYNC/SYNC 命令同步请求;
                                if (cmd == "PSYNC") {
                                    // 部分匹配, 从 g_repl_backlog[] 中截取发送;
                                    // PSYNC <offset>
                                    if (rv.array.size() == 2 && rv.array[1].type == RespType::BulkString) {
                                        int64_t last_offset = 0;
                                        try {
                                            last_offset = std::stoll(rv.array[1].bulk_string);
                                        } catch (...) {
                                            MR_LOG("ERROR", "Invalid PSYNC offset: " << rv.array[1].bulk_string);
                                            last_offset = -1;
                                        }
                                        // hit backlog
                                        if (last_offset >= g_backlog_start_offset && last_offset <= g_back_log_off) {
                                            size_t start_offset = (size_t) (last_offset - g_backlog_start_offset);
                                            if (start_offset < g_repl_back_part_log.size()) {
                                                conn.is_replica = true;
                                                std::string off = "+OFFSET " + std::to_string(g_back_log_off) + "\r\n";
                                                conn_queue_add(conn, off);
                                                conn_queue_add(conn, g_repl_back_part_log.substr(start_offset));
                                                conn_try_flush_now(fd_, conn, ev);
                                                continue;
                                            }
                                        }
                                    }
                                }

                                if (cmd == "SYNC") {
                                    std::string err;
                                    RdbConfig rdb_temp = config.rdb_conf;
                                    if (!rdb_temp.enabled) {
                                        rdb_temp.enabled = true;
                                    }
                                    RDBManager rdb(rdb_temp);
                                    if (!rdb.dump(g_store, err)) {
                                        conn_queue_add(conn, respError("ERR sync save failed"));
                                    } else {
                                        std::string path = rdb.path();
                                        FILE *f = fopen(path.c_str(), "rb");
                                        if (!f) {
                                            conn_queue_add(conn, respError("ERR open rdb file failed"));
                                        } else {
                                            std::string content;
                                            content.resize(0);
                                            char string[8192];
                                            size_t r;
                                            while ((r = fread(string, 1, sizeof(string), f)) > 0) {
                                                content.append(string, r);
                                            }
                                            fclose(f);
                                            // 把 rdb 全量数据 当做 普通 bulk 字符串处理;
                                            conn_queue_add(conn, respBulk(content));
                                            conn.is_replica = true;
                                            // 发送当前 offset（简单实现：用 RESP 简单字符串）
                                            std::string off = "+OFFSET " + std::to_string(g_back_log_off) + "\r\n";
                                            conn_queue_add(conn, std::move(off));
                                        }
                                    }
                                    conn_try_flush_now(fd_, conn, ev);
                                    continue;
                                }// if PSYNC/SYNC over

                                // 处理 用户 请求, 并把处理请求结果放到输出队列中;
                                conn_queue_add(conn, handle_cmd(rv, &raw));
                                // 尝试着把 c 结果输出队列;
                                conn_try_flush_now(fd_, conn, ev);
                            } else {
                                // other...
                            }
                        }
                    }// while parse over

                    // 广播数据;
                    if (!g_repl_queue.empty()) {
                        for (auto &kv: conns) {
                            Conn &conn = kv.second;
                            if (!conn.is_replica) {
                                continue;
                            }
                            for (auto &parts: g_repl_queue) {
                                std::string cmd = toArrayType(parts);
                                int64_t next_offset = g_back_log_off + (int64_t) cmd.size();
                                std::string off = "+OFFSET " + std::to_string(next_offset) + "\r\n";
                                g_back_log_off = next_offset;
                                append_cmd_to_back_log(off);
                                append_cmd_to_back_log(cmd);
                                conn_queue_add(conn, std::move(off));
                                conn_queue_add(conn, std::move(cmd));
                            }
                            if (has_pending(conn)) {
                                mod_epoll(epoll_fd, conn.fd, EPOLLIN | EPOLLET | EPOLLOUT | EPOLLRDHUP | EPOLLHUP);
                            }
                        }// for replicas con over
                        g_repl_queue.clear();
                    }// if replication queue over

                    // If peer half-closed and nothing pending, close now
                    if ((ev & EPOLLRDHUP) && !has_pending(conn)) {
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd_, nullptr);
                        close(fd_);
                        conns.erase(it);
                        continue;
                    }
                }// if EPOLLIN over

                // 套接字的发送缓冲区有可用空间;
                if (ev & EPOLLOUT) {
                    conn_try_flush_now(fd_, conn, ev);
                    if (!has_pending(conn)) {
                        mod_epoll(epoll_fd, fd_, EPOLLIN | EPOLLRDHUP | EPOLLET);
                        if (ev & EPOLLRDHUP) {
                            epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd_, nullptr);
                            close(fd_);
                            conns.erase(it);
                            continue;
                        }
                    }
                }// if EPOLLOUT over
            }
        }
    }
}