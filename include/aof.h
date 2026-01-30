//
// Created by 19327 on 2025/12/30/星期二.
//
#pragma once

#include <vector>
#include <deque>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <string>
#include <memory>

#include "config.h"
#include "kv.h"
#include "mpsc_ring_buffer.h"

namespace train_set {

struct AofQueueItem { std::string data; int64_t seq; };

    class AOFManager {
    private:
        int fd = -1;
        AofConfig config;
        std::atomic<bool> running{false};

        // 自动释放空间;
        struct AofItem {
            // 无动态指针;
            std::string data;
            int64_t seq;
        };
        // 刷盘-后台线程;
        std::thread write_deque_thread;
        std::mutex deque_mutex; // 队列锁;
        std::condition_variable write_cond; // 刷盘阻塞-唤醒;
        std::condition_variable write_commit;// 刷盘完成-唤醒;
 
        // 等待刷盘队列;
        std::deque<AofItem> write_queue;
        // 待刷盘字节大小;
        size_t pending_write = 0;
        std::atomic<int64_t> seq{1};
        int64_t last_seq = 0;
        std::chrono::steady_clock::time_point last_write_time{std::chrono::steady_clock::now()};
 
        // 无锁环形缓冲区
        std::unique_ptr<train_set::MPSCRingBuffer<AofQueueItem>> lockfree_queue_;
        bool using_lockfree_queue_ = false;


        std::atomic<bool> rewriting{false};
        std::thread rewriter_thread;
        std::mutex incr_mutex;
        std::vector<std::string> incr_cmd;

        // 这段代码是一个注释，说明当前正在执行文件描述符的安全交换操作。在多线程环境中，
        // 为了确保文件描述符交换过程的原子性和数据一致性，需要暂停写入线程的操作，
        // 防止在交换过程中出现数据竞争或不一致的状态
        std::atomic<bool> pause_write{false};
        std::mutex pause_mutex;
        std::condition_variable pause_cond;
        bool pause_write_flag = false;

        void writeLoop();

        void rewriteLoop(KVStorage *storage);

    public:
        AOFManager();

        ~AOFManager();

        AofConfig getConfig() const {
            return config;
        }

        bool init(const AofConfig &config, std::string &errM);

        bool load(KVStorage &storage, std::string &errM);

        bool appendCmd(const std::vector<std::string> &cmds, bool isRaw);

        bool appendCmdRaw(const std::string &cmd);

        bool isEnabled() { return config.enabled; };

        void shutdown();

        bool bgWrite(KVStorage &storage, std::string &errM);

        AofMode mode() const {
            return config.mode;
        }

        std::string path() const;

    };

    std::string toArrayType(const std::vector<std::string> &parts);
}