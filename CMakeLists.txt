cmake_minimum_required(VERSION 3.15)

project(trainset LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)
add_compile_options(-O3 -DNDEBUG)

add_executable(trainset
        src/aof.cpp
        src/main.cpp
        src/kv.cpp
        src/rdb.cpp
        src/config_loader.cpp
        src/resp.cpp
        src/server.cpp
)

target_include_directories(trainset PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)

target_link_libraries(trainset PRIVATE Threads::Threads)
