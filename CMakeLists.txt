cmake_minimum_required(VERSION 3.10)

project(LMDB-CPP)

set (CMAKE_CXX_STANDARD 17)

set (CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread -g")

include_directories(.)

file(GLOB SOURCES "*.cpp")

find_package( Threads )

add_executable(Test ${SOURCES})

target_link_libraries( Test ${CMAKE_THREAD_LIBS_INIT} )