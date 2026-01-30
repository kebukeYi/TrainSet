file(GLOB src "${CMAKE_CURRENT_SOURCE_DIR}/*.cpp")

message(STATUS "Source files: ${src}")

set(src_list ${src} PARENT_SCOPE)