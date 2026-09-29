cmake_minimum_required(VERSION 3.24)
project(cppcoroservicelib_grpc_consumer LANGUAGES CXX)

find_package(cppcoroservicelib REQUIRED)

add_executable(cppcoroservicelib_grpc_consumer main.cpp)
target_link_libraries(cppcoroservicelib_grpc_consumer PRIVATE
    servicelib::grpc)
