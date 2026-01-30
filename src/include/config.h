//
// Created by 19327 on 2025/12/31/星期三.
//
#pragma once

#include <string>
#include <cstdint>

namespace train_set {

    enum class AofMode {
        No,
        None,
        EverySec,
        Always
    };

    struct AofConfig {
        bool enabled = false;
        AofMode mode = AofMode::EverySec;
        std::string dir = "./data";
        std::string file_name = "appendonly.aof";
        // 每次aof 批量写入磁盘字节数;
        size_t max_write_buffer_size = 256 * 1024;
        // 1500us; 聚合等待上限（微秒）
        int consume_aof_queue_us = 1500;
        // 无锁队列开关
        bool use_lockfree_queue = false;
        // 无锁队列容量（2的幂次）
        size_t lockfree_queue_capacity = 65536;
        // 预分配 aof 文件大小
        size_t file_pre_alloc_size = 64 * 1024 * 1024;
        // 每秒触发; everysec 实际同步周期（毫秒），可调平滑尾延迟
        int file_sync_interval_ms = 1000; // everySec ms;
        // 写入后, 触发后台回写（SFR_WRITE）
        bool file_use_sync_range = false;
        // 512KB; 达到该批量再调用 sync_file_range，避免过于频繁;
        size_t file_use_sync_range_size = 512 * 1024;
        // // 每次 fdatasync 后对已同步范围做 DONNED
        bool file_advise_page_cache_after_sync = false;
    };

    struct RdbConfig {
        bool enabled = false;
        std::string dir = "./data";
        std::string file_name = "dump.rdb";
    };

    struct ReplicateConfig {
        bool enabled = false;
        std::string master_host="127.0.0.1";
        uint16_t master_port = 6379;
    };

    struct ServerConfig {
        std::string host = "127.0.0.1";
        uint16_t port = 6379;
        AofConfig aof_conf;
        RdbConfig rdb_conf;
        ReplicateConfig rep_conf;
    };
}