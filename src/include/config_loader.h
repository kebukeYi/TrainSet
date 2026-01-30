//
// Created by 19327 on 2025/12/31/星期三.
//

#pragma once

#include "config.h"

namespace train_set {

    bool load_config(const std::string &file_path, ServerConfig &config, std::string &err);
}