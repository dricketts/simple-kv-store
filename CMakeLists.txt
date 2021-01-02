cmake_minimum_required(VERSION 3.10)

project(LMDB-CPP)

set (CMAKE_CXX_STANDARD 17)

set (CMAKE_EXPORT_COMPILE_COMMANDS ON)

include_directories(.)

file(GLOB SOURCES "*.cpp")

add_executable(Test ${SOURCES})