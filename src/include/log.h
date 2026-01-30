//
// Created by 19327 on 2026/01/04/星期日.
//

#pragma once

#include <chrono>
#include <ctime>
#include <iostream>
#include <string>

namespace train_set {
    inline std::string nowTime() {
        using namespace std::chrono;
        auto t = system_clock::to_time_t(system_clock::now());
        char buf[64];
        std::strftime(buf, sizeof(buf), "%F %T", std::localtime(&t));
        return {buf};
    }

    void Log(int level, const std::string &msg) {
        std::cerr << "[" << nowTime() << "] [" << level << "] " << msg << std::endl;
    }

#define MR_LOG(level, msg)                                                       \
  do                                                                             \
  {                                                                              \
    std::cerr << "[" << nowTime() << "] [" << level << "] " << msg << std::endl; \
  } while (0)

}