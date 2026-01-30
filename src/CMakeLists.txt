file(GLOB src "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

# 打印变量值 src
#message(STATUS "Source files: ${src}")

set(src_list ${src} PARENT_SCOPE)