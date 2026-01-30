//
// Created by 19327 on 2026/01/04/星期日.
//

#pragma once

#include <thread>
#include <string>
#include "config.h"
#include "kv.h"

namespace train_set {

    class ReplicateClient {
    private:
        KVStorage &storage;
        const ServerConfig &cof;
        std::thread th;
        int fd;
        bool running = false;
        int64_t last_replicate_offset = 0;
    public:
        explicit ReplicateClient(ServerConfig &config, KVStorage &storage);

        ~ReplicateClient();

        void start();

        void stop();

        int conn_to_master();

    private:
        void run_replicate();
    };
}