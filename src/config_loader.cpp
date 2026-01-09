//
// Created by 19327 on 2025/12/31/星期三.
//
#include "config_loader.h"
#include <fstream>
#include <cstdint>

namespace train_set {

    std::string trim(const std::string &str) {
        size_t i = 0;
        size_t j = str.size();
        while (i < j && std::isspace(str[i])) {
            i++;
        }
        while (j > i && std::isspace(str[j - 1])) {
            j--;
        }
        return str.substr(i, j - i);
    }

    bool load_config(const std::string &file_path, ServerConfig &config, std::string &err) {
        std::ifstream in(file_path);
        if (!in.good()) {
            err = "open config failed: " + file_path;
            return false;
        }
        std::string line;
        int line_num = 0;
        while (std::getline(in, line)) {
            line_num++;
            std::string t = trim(line);
            if (t.empty() || t[0] == '#') {
                continue;
            }
            size_t pos = t.find('=');
            if (pos == std::string::npos) {
                err = "invalid line " + std::to_string(line_num);
                return false;
            }
            std::string key = trim(t.substr(0, pos));
            std::string value = trim(t.substr(pos + 1));
            if (key == "port") {
                try {
                    config.port = static_cast<uint16_t>(std::stoi(value));
                } catch (...) {
                    err = "invalid port at line " + std::to_string(line_num);
                    return false;
                }
            } else if (key == "bind_address") {
                config.host = value;
            } else if (key == "aof.enabled") {
                config.aof_conf.enabled = value == "true";
            } else if (key == "aof.mode") {
                if (value == "everysec") {
                    config.aof_conf.mode = AofMode::EverySec;
                } else if (value == "always") {
                    config.aof_conf.mode = AofMode::Always;
                } else if (value == "none") {
                    config.aof_conf.mode = AofMode::None;
                } else if (value == "no") {
                    config.aof_conf.mode = AofMode::No;
                } else {
                    err = "invalid aof.mode at line " + std::to_string(line_num);
                    return false;
                }
            } else if (key == "aof.dir") {
                config.aof_conf.dir = value;
            } else if (key == "aof.filename") {
                config.aof_conf.file_name = value;
            } else if (key == "aof.batch_bytes") {
                try {
                    config.aof_conf.max_write_buffer_size = static_cast<size_t>(std::stoi(value));
                } catch (...) {
                    err = "invalid batch_bytes";
                    return false;
                }
            } else if (key == "aof.batch_wait_us") {
                try {
                    config.aof_conf.consume_aof_queue_us = (std::stoi(value));
                } catch (...) {
                    err = "invalid batch_wait_us";
                    return false;
                }
            } else if (key == "aof.pre_alloc_bytes") {
                try {
                    config.aof_conf.file_pre_alloc_size = static_cast<size_t>(std::stoi(value));
                } catch (...) {
                    err = "invalid pre_alloc_bytes";
                    return false;
                }
            } else if (key == "aof.sync_interval_ms") {
                try {
                    config.aof_conf.file_sync_interval_ms = (std::stoi(value));
                } catch (...) {
                    err = "invalid sync_interval_ms";
                    return false;
                }
            } else if (key == "aof.use_sync_file_range") {
                config.aof_conf.file_use_sync_range = (value == "1" || value == "true" || value == "yes");
            } else if (key == "aof.sfr_min_bytes") {
                config.aof_conf.file_use_sync_range_size = static_cast<size_t>(std::stoi(value));
            } else if (key == "aof.fadvise_dontneed_after_sync") {
                config.aof_conf.file_advise_page_cache_after_sync = (value == "1" || value == "true" || value == "yes");
            } else if (key == "rdb.enabled") {
                config.rdb_conf.enabled = (value == "1" || value == "true" || value == "yes");
            } else if (key == "rdb.dir") {
                config.rdb_conf.dir = value;
            } else if (key == "rdb.filename") {
                config.rdb_conf.file_name = value;
            } else if (key == "replicate.enabled") {
                config.rep_conf.enabled = (value == "1" || value == "true" || value == "yes");
            } else if (key == "replicate.master_host") {
                config.rep_conf.master_host = value;
            } else if (key == "replicate.master_port") {
                try {
                    config.rep_conf.master_port = static_cast<uint16_t>(std::stoi(value));
                } catch (...) {
                    err = "invalid replicate.master_port at line " + std::to_string(line_num);
                    return false;
                }
            } else {
                err = "invalid key at line " + std::to_string(line_num);
                return false;
            }
        }
        return true;
    }
}

