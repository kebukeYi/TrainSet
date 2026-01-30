//
// Created by 19327 on 2025/12/31/星期三.
//
#include <string>
#include <vector>
#include <csignal>
#include <filesystem>
#include <fcntl.h>
#include <sys/uio.h>
#include <algorithm>
#include <iostream>
#include "aof.h"

namespace train_set {
    bool writeAllToFd(int fd, const char *data, size_t len) {
        size_t offset = 0;
        while (offset < len) {
            auto w = ::write(fd, data + offset, len - offset);
            if (w > 0) {
                offset += (size_t) w;
                continue;
            }
            if (w < 0 && (errno == EINTR || errno == EAGAIN)) {
                continue;
            }
            return false;
        }
        return true;
    }

    std::string joinPath(const std::string &dir, const std::string &fileName) {
        if (dir.empty()) {
            return fileName;
        }
        if (dir.back() == '/') {
            return dir + (fileName);
        }
        return dir + "/" + fileName;
    }

    // [set, kk , val]
    // *3 \r\n $3 \r\n set \r\n $2 \r\n kk \r\n $3 \r\n val \r\n
    std::string toArrayType(const std::vector<std::string> &parts) {
        std::string out;
        out.reserve(parts.size() * 16);
        out.append("*").append(std::to_string(parts.size())).append("\r\n");
        for (auto &part: parts) {
            out.append("$").append(std::to_string(part.size())).append("\r\n");
            out.append(part).append("\r\n");
        }
        return out;
    }

    std::string AOFManager::path() const {
        return joinPath(config.dir, config.file_name);
    }

    AOFManager::AOFManager() = default;

    AOFManager::~AOFManager() {
        shutdown();
    }

    bool AOFManager::init(const AofConfig &conf, std::string &err) {
        config = conf;
        if (!config.enabled) {
            return true;
        }
        std::error_code ec;
        std::filesystem::create_directories(config.dir, ec);
        if (ec) {
            err = "mkdir failed: " + config.dir + ec.message();
            return false;
        }
        fd = ::open(path().c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
        if (fd < 0) {
            err = "open failed: " + path();
            return false;
        }
#ifdef __linux__
        if (config.file_pre_alloc_size > 0) {
            posix_fallocate(fd, 0, static_cast<off_t>(config.file_pre_alloc_size));
        }
#endif
        running.store(true);
        write_deque_thread = std::thread(&AOFManager::writeLoop, this);
        return true;
    }

    void AOFManager::shutdown() {
        running.store(false);
        // 唤醒 消费queue拉取线程, 赶紧去消费;
        write_cond.notify_all();
        if (write_deque_thread.joinable()) {
            write_deque_thread.join();
        }
        if (fd >= 0) {
            ::fdatasync(fd);
            ::close(fd);
            fd = -1;
        }
    }

    bool AOFManager::appendCmd(const std::vector<std::string> &cmds, bool isRaw) {
        if (!config.enabled || fd < 0) {
            return true;
        }
        std::string cmd;
        if (isRaw) {
            cmd = cmds[0];
        } else {
            // cmd: set key val
            cmd = toArrayType(cmds);
        }
        // cmd: *3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n
        bool need_incr = rewriting.load();
        std::string incr_copy;
        if (need_incr) {
            incr_copy = cmd;
        }
        int64_t my_seq = 0;
        {
            std::lock_guard<std::mutex> lock(deque_mutex);
            pending_write += cmd.size();
            my_seq = this->seq.fetch_add(1);
            write_queue.push_back(AofItem{std::move(cmd), my_seq});
        }
        if (need_incr) {
            std::lock_guard<std::mutex> lock(incr_mutex);
            incr_cmd.emplace_back(std::move(incr_copy));
        }
        // 唤醒 刷盘 线程, 去消费队列中的元素;
        write_cond.notify_one();
        // 每次写 aof 触发保存; 强一致性;
        if (config.mode == AofMode::Always) {
            std::unique_lock<std::mutex> lock(deque_mutex);
            // 当前线程等待刷盘完成处理;
            write_commit.wait(lock, [&]() {
                return last_seq >= my_seq || !running.load();
            });
        }
        return true;
    }

    bool AOFManager::appendCmdRaw(const std::string &cmd) {
        std::vector<std::string> parts;
        parts.emplace_back(cmd);
        return appendCmd(parts, true);
    }

    bool AOFManager::load(KVStorage &storage, std::string &err) {
        if (!config.enabled) {
            return true;
        }
        auto fd_ = ::open(path().c_str(), O_RDONLY);
        if (fd_ < 0) {
            return true;
        }
        std::string buf;
        buf.resize(1 << 20);
        std::string data;
        while (true) {
            ssize_t r = ::read(fd_, buf.data(), buf.size());
            if (r < 0) {
                err = "read aof file failed: " + path();
                ::close(fd_);
                return false;
            }
            if (r == 0) {
                break;
            }
            data.append(buf.data(), static_cast<size_t>(r));
        }
        ::close(fd_);
        size_t pos = 0;
        auto readline = [&](std::string &out) -> bool {
            size_t end = data.find("\r\n", pos);
            if (end == std::string::npos) {
                return false;
            }
            out.assign(data.data() + pos, end - pos);
            pos = end + 2;
            return true;
        };

        while (pos < data.size()) {
            if (data[pos] != '*') {
                break;
            }
            pos++;
            std::string line;
            if (!readline(line)) {
                err = "aof bad bulk len";
                return false;
            }
            int token_size = std::stoi(line);
            std::vector<std::string> tokens;
            // token_size 代表几个 token 数量; set kk mm; 3个;
            tokens.reserve(token_size);
            for (int i = 0; i < token_size; ++i) {
                if (data[pos] != '$') {
                    err = "aof bad bulk, expect $;";
                    return false;
                }
                pos++;
                if (!readline(line)) {
                    err = "aof bad bulk len";
                    return false;
                }
                int bulk_len = std::stoi(line);
                if (pos + (size_t) bulk_len + 2 > data.size()) {
                    err = "aof bad trunc";
                    return false;
                }
                tokens.emplace_back(data.data() + pos, bulk_len);
                pos += (size_t) bulk_len + 2;
            }// for count over

            if (tokens.empty()) {
                continue;
            }

            std::string cmd;
            cmd.reserve(tokens[0].size());
            // *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
            for (char c: tokens[0]) {
                cmd.push_back((char) ::toupper(c));
            }

            if (cmd == "SET" && tokens.size() == 3) {
                // set key val
                storage.set(tokens[1], tokens[2]);
            } else if (cmd == "DEL" && tokens.size() >= 2) {
                // del key1 key2 key3 ...
                std::vector<std::string> keys(tokens.begin() + 1, tokens.end());
                storage.del(keys);
            } else if (cmd == "ZADD" && tokens.size() >= 4) {
                // zadd key 1.1 f1  11 f2
                storage.zadd(tokens[1], std::stod(tokens[2]), tokens[3]);
            } else if (cmd == "HDEL" && tokens.size() >= 3) {
                // hdel key f1 f2 f3 ...
                std::vector<std::string> fields(tokens.begin() + 2, tokens.end());
                storage.hdel(tokens[1], fields);
            } else if (cmd == "HSET" && tokens.size() >= 4) {
                // hset key f1 v1
                storage.hset(tokens[1], tokens[2], tokens[3]);
            } else if (cmd == "EXPIRE" && tokens.size() == 3) {
                // expire key 10
                storage.expire(tokens[1], std::stoll(tokens[2]));
            } else {
                //
            }
        }// while read over
        return true;
    }

    bool AOFManager::bgWrite(KVStorage &storage, std::string &err) {
        if (!config.enabled) {
            err = "aod disabled";
            return false;
        }
        bool expected = false;
        if (!rewriting.compare_exchange_strong(expected, true)) {
            err = "rewrite already running";
            return false;
        }
        rewriter_thread = std::thread(&AOFManager::rewriteLoop, this, &storage);
        return true;
    }
}

namespace train_set {
    void AOFManager::writeLoop() {
        size_t batch_bytes = config.max_write_buffer_size > 0 ? config.max_write_buffer_size : 64 * 1024;
        auto wait_us = std::chrono::microseconds(config.consume_aof_queue_us > 0 ? config.consume_aof_queue_us : 1000);
        const int iovMax = 64;

        std::vector<AofItem> local;
        local.reserve(256);
        while (running.load()) {
            if (pause_write.load()) {
                std::unique_lock<std::mutex> lock(pause_mutex);
                pause_write_flag = true;
                // 通知 aof 重写线程, 我已经暂停了, 你可以继续重写;
                pause_cond.notify_all();
                // 暂停写入操作; 等待 aof 重写完毕, 来唤醒;
                pause_cond.wait(lock, [&]() {
                    return !pause_write.load() || !running.load();
                });
                // 被唤醒;
                pause_write_flag = false;
                if (!running.load()) {
                    break;
                }
            }
            // 会调用内部元素的析构函数;
            local.clear();
            size_t bytes = 0;
            {
                std::unique_lock<std::mutex> lock(deque_mutex);
                // 待刷盘队列为空;
                if (write_queue.empty()) {
                    write_cond.wait_for(lock, wait_us, [&] {
                        return !write_queue.empty() || !running.load();
                    });
                }
                // 循环从 queue -> local 中; 只拿 kMaxIov 个;
                while (!write_queue.empty() && bytes < batch_bytes && (int) local.size() < iovMax) {
                    local.emplace_back(std::move(write_queue.front()));
                    bytes += local.back().data.size();
                    write_queue.pop_front();
                }
                // aof 内存中目前还剩多少字节待刷盘;
                pending_write -= bytes;
                if (pending_write < 0) {
                    pending_write = 0;
                }
            }// while local over

            if (local.empty()) {
                // everysec 模式周期性刷盘;
                if (config.mode == AofMode::EverySec) {
                    auto now = std::chrono::steady_clock::now();
                    auto interval = std::chrono::milliseconds(
                            config.file_sync_interval_ms > 0 ? config.file_sync_interval_ms : 1000);
                    if (now - last_write_time >= interval) {
                        if (fd >= 0) {
                            // pageCache 刷盘;
                            fdatasync(fd);
                            last_write_time = now;
                        }
                    }
                }
                // 进入 always 模式, 但是目前没有数据可写, 继续尝试消费即可;
                continue;
            }
            // local 不为空;
            iovec iov[iovMax];
            int iov_count = 0;
            for (auto &item: local) {
                if (iov_count >= iovMax) {
                    break;
                }
                iov[iov_count].iov_base = (char *) item.data.data();
                iov[iov_count].iov_len = item.data.size();
                iov_count++;
            }
            // 聚合写入，处理部分写;
            int write_idx = 0;
            while (write_idx < iov_count) {
                // w 成功写入字节数：表示实际写入到文件的字节数;
                // writev 可以原子地发送多个不连续内存块，但它不保证一次就把你给的全部数据发完（尤其在非阻塞或流量压力大时）;
                ssize_t w = ::writev(fd, &iov[write_idx], iov_count - write_idx);
                if (w < 0) {
                    // 出错：简单退让，避免忙等
                    usleep(1000);
                    break;
                }
                // 本次写入字节数;
                size_t write_len = static_cast<size_t>(w);
                // 调整下次 iov[] 写索引;
                while (write_len > 0 && write_idx < iov_count) {
                    // 写入的字节长度 大于当前iov[]中当前索引的块长度;
                    // 那就累加, 判断下一个块;
                    if (write_len >= iov[write_idx].iov_len) {
                        write_len -= iov[write_idx].iov_len;
                        write_idx++;
                    } else {
                        // 当前块中 只写入了部分数据, 调整索引, 等待下次写入;
                        iov[write_idx].iov_base = (char *) iov[write_idx].iov_base + write_len;
                        iov[write_idx].iov_len -= write_len;
                        write_len = 0;
                    }
                }
                if (w == 0) {
                    break;
                }
            }// while iov[] over

            // Linux 可选: 触发后台回写，平滑尾部写放大;
#ifdef __linux__
            if (bytes >= config.file_use_sync_range_size && config.file_use_sync_range) {
                // 对最近写入的区间进行提示。这里为了简化，使用整个文件范围（可能较重），可进一步优化记录 offset
                off_t cur = ::lseek(fd, 0, SEEK_END);
                if (cur > 0) {
                    off_t start = cur - (off_t) bytes;
                    if (start < 0) {
                        start = 0;
                    }
                    (void) ::sync_file_range(fd, start,
                                             (off_t) bytes,
                                             SYNC_FILE_RANGE_WRITE);
                }
            }
#endif
            // 写完后, 模式处理;
            if (config.mode == AofMode::Always) {
                fdatasync(fd);
#ifdef __linux__
                if (config.file_advise_page_cache_after_sync) {
                    off_t cur2 = lseek(fd, 0, SEEK_END);
                    if (cur2 > 0) {
                        (void) ::posix_fadvise(fd, 0, cur2, POSIX_FADV_DONTNEED);
                    }
                }
#endif
                // 更新已提交序号并唤醒等待者
                int64_t max_seq = 0;
                for (auto &item: local) {
                    max_seq = std::max(max_seq, item.seq);
                }
                {
                    std::lock_guard<std::mutex> lock(deque_mutex);
                    last_seq = std::max(last_seq, max_seq);
                }
                // 唤醒等待者-用户线程;
                write_commit.notify_all();
            } else if (config.mode == AofMode::EverySec) {
                auto now = std::chrono::steady_clock::now();
                auto interval = std::chrono::milliseconds(
                        config.file_sync_interval_ms > 0 ? config.file_sync_interval_ms : 1000);
                if (now - last_write_time >= interval) {
                    ::fdatasync(fd);
                    last_write_time = now;
#ifdef __linux__
                    if (config.file_advise_page_cache_after_sync) {
                        off_t cur3 = ::lseek(fd, 0, SEEK_END);
                        if (cur3 > 0) {
                            (void) ::posix_fadvise(fd, 0, cur3, POSIX_FADV_DONTNEED);
                        }
                    }
#endif
                }
            }
        }// while running over

        // 退出前 flush;
        if (fd >= 0) {
            // 把剩余队列写完
            while (true) {
                std::vector<AofItem> reset;
                size_t bytes = 0;
                {
                    std::lock_guard<std::mutex> lock(deque_mutex);
                    while (!write_queue.empty() && (int) reset.size() < iovMax) {
                        reset.emplace_back(std::move(write_queue.front()));
                        bytes += reset.back().data.size();
                        write_queue.pop_front();
                    }
                    pending_write -= bytes;
                    if (pending_write < 0) {
                        pending_write = 0;
                    }
                }
                if (reset.empty()) {
                    break;
                }
                iovec iov[iovMax];
                int iov_count = 0;
                for (auto &item: reset) {
                    if (iov_count >= iovMax) {
                        break;
                    }
                    iov[iov_count].iov_base = (char *) item.data.data();
                    iov[iov_count].iov_len = item.data.size();
                    iov_count++;
                }

                int write_idx = 0;
                while (write_idx < iov_count) {
                    ssize_t w = ::writev(fd, &iov[write_idx], iov_count - write_idx);
                    if (w < 0) {
                        if (errno == EINTR || errno == EAGAIN) {
                            continue;
                        }
                        usleep(1000);
                        break;
                    }
                    size_t write_len = static_cast<size_t>(w);
                    // 调整iov[];
                    while (write_len > 0 && write_idx < iov_count) {
                        if (write_len >= iov[write_idx].iov_len) {
                            write_len -= iov[write_idx].iov_len;
                            write_idx++;
                        } else {
                            iov[write_idx].iov_base = (char *) iov[write_idx].iov_base + write_len;
                            iov[write_idx].iov_len -= write_len;
                            write_len = 0;
                        }
                    }
                    if (w == 0) {
                        break;
                    }
                }// while iov over
            }// while reset over
            ::fdatasync(fd);
        }
    }

    void AOFManager::rewriteLoop(KVStorage *storage) {
        // 1) 生成临时文件路径
        std::string temp_path = joinPath(config.dir, config.file_name + ".rewrite.tmp");
        auto temp_fd = ::open(path().c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (temp_fd < 0) {
            rewriting.store(false);
            return;
        }

        // 2) 遍历快照，输出最小命令集;
        // String
        {
            auto snap = storage->stringSnapshot();
            for (auto &item: snap) {
                auto key = item.first;
                auto &r = item.second;
                std::vector<std::string> parts = {"SET", key, r.value};
                std::string line = toArrayType(parts);
                writeAllToFd(temp_fd, line.data(), line.size());

                if (r.expire > 0) {
                    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch()).count();
                    int64_t ttl = (r.expire - now) / 1000;
                    if (ttl < 1) {
                        ttl = 1;
                    }
                    std::vector<std::string> e_parts = {"EXPIRE", key, std::to_string(ttl)};
                    auto e_line = toArrayType(e_parts);
                    writeAllToFd(temp_fd, e_line.data(), e_line.size());
                }
            }
        }

        // Hash
        {
            auto snap = storage->hashSnapshot();
            for (auto &kv: snap) {
                auto key = kv.first;
                auto &r = kv.second;
                for (const auto &fv: r.field_vals) {
                    std::vector<std::string> parts = {"HSET", key, fv.first, fv.second};
                    // *4\r\n $4\r\nHSET $3\r\nkey $5\r\nfiled $3\r\nval \r\n
                    std::string line = toArrayType(parts);
                    writeAllToFd(temp_fd, line.data(), line.size());
                }

                if (r.expire > 0) {
                    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch()).count();
                    int64_t ttl = (r.expire - now) / 1000;
                    if (ttl < 1) {
                        ttl = 1;
                    }
                    std::vector<std::string> e_parts = {"EXPIRE", key, std::to_string(ttl)};
                    std::string e_line = toArrayType(e_parts);
                    writeAllToFd(temp_fd, e_line.data(), e_line.size());
                }
            }
        }

        // ZSet
        {
            auto snap = storage->zSetSnapshot();
            for (auto &zv: snap) {
                auto key = zv.key;
                for (auto &z: zv.items) {
                    std::vector<std::string> parts = {"ZADD", key, std::to_string(z.first), z.second};
                    // *4\r\n $4\r\nZADD $3\r\nkey $2\r\nscore $3\r\nval \r\n
                    std::string line = toArrayType(parts);
                    writeAllToFd(temp_fd, line.data(), line.size());
                }

                if (zv.expire > 0) {
                    int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch()).count();
                    int64_t ttl = (zv.expire - now) / 1000;
                    if (ttl < 1) {
                        ttl = 1;
                    }
                    std::vector<std::string> e_parts = {"EXPIRE", key, std::to_string(ttl)};
                    std::string e_line = toArrayType(e_parts);
                    writeAllToFd(temp_fd, e_line.data(), e_line.size());
                }
            }
        }

        // 3) 进入切换阶段: 暂停 writer, 等待上一批write写完;
        pause_write.store(true);
        {
            std::unique_lock<std::mutex> lock(pause_mutex);
            // 阻塞等待 writer 暂停;
            pause_cond.wait(lock, [&] {
                return pause_write_flag;
            });
        }

        // 在 writer 暂停期间，阻塞增量缓冲区, 将现存的增量数据追加到aof中, 确保没有遗漏;
        // 那么假如在此刻宕机, 会发生数据丢失; 也就是在原子更改aof名字时, 发生宕机;
        {
            std::unique_lock<std::mutex> lock(incr_mutex);
            for (auto &cmd: incr_cmd) {
                writeAllToFd(temp_fd, cmd.data(), cmd.size());
            }
            incr_cmd.clear();
        }

        fdatasync(temp_fd);

        // 原子替换并切换 fd;
        {
            std::string final_path = path();
            close(fd);
            close(temp_fd);
            rename(temp_path.c_str(), final_path.c_str());
            fd = ::open(final_path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
            // fsync 目录，保证 rename 持久;
            int dfd = open(config.dir.c_str(), O_RDONLY);
            if (dfd >= 0) {
                fsync(dfd);
                close(dfd);
            }
        }

        pause_write.store(false);
        // 唤醒 writer 写线程;
        write_cond.notify_all();

        {
            // 清理在 rename 期间内的增量数据;
            // 为什么可以被清理? 因为数据在deque中 始终保持一份;
            std::unique_lock<std::mutex> lock(incr_mutex);
            incr_cmd.clear();
        }
        rewriting.store(false);
    }
}
