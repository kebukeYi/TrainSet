//
// Created by 19327 on 2025/12/31/星期三.
//

#pragma once

#include "config.h"

namespace train_set {

    class Server {
    private:
        ServerConfig &config;
        int listen_fd = -1;
        int epoll_fd = -1;
        int time_fd = -1;
    private:
        int setListener();

        int setEpoll();

        int epoll_loop();

    public:
        explicit Server(ServerConfig &config);

        ~Server();

        int run();
    };

}