cmake_minimum_required(VERSION 3.15)

set(Target train_dis)

project(${Target} LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/bin)

find_package(Threads REQUIRED)
add_compile_options(-O3 -DNDEBUG)

# 先获取源文件列表
add_subdirectory(src)
include_directories(src/include)

# 添加 src_list 到父级作用域，或者直接在子目录中创建可执行文件
add_executable(${Target} ${src_list})

target_link_libraries(${Target} PRIVATE Threads::Threads)