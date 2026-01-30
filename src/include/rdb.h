//
// Created by 19327 on 2025/12/31/星期三.
//

#pragma once

#include "config.h"
#include "kv.h"
#include <string>

namespace train_set {

    class RDBManager {
    private:
        RdbConfig config_;
    public:
        RDBManager() = default;

        explicit RDBManager(const RdbConfig &config) : config_(config) {};

        ~RDBManager() = default;

        bool load(KVStorage &storage, std::string &err);

        bool dump(KVStorage &storage, std::string &err);

        void setConfig(const RdbConfig &config) {
            this->config_ = config;
        };

        std::string path();
    };
}