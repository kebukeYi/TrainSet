cmake_minimum_required(VERSION 3.15)

project(train_set LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)
add_compile_options(-O3 -DNDEBUG)

add_executable(train_set
        src/aof.cpp
        src/main.cpp
        src/kv.cpp
        src/rdb.cpp
        src/config_loader.cpp
        src/resp.cpp
        src/server.cpp
        src/replicate_client.cpp
        src/resp_test.cpp
        src/config_loader_test.cpp
        src/kv_test.cpp
        src/rdb_test.cpp
        src/server_test.cpp
        src/aof_test.cpp
)

# 测试可执行文件
add_executable(mpsc_ring_buffer_test
        src/mpsc_ring_buffer_test.cpp
)

target_include_directories(train_set PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(train_set PRIVATE Threads::Threads)

target_include_directories(mpsc_ring_buffer_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(mpsc_ring_buffer_test PRIVATE Threads::Threads)
