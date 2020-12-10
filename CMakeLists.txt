cmake_minimum_required(VERSION 3.10)

project(LMDB-CPP)

set (CMAKE_CXX_STANDARD 11)

include_directories(.)

file(GLOB SOURCES "*.cpp")

add_executable(Test ${SOURCES})