cmake_minimum_required(VERSION 3.24)

project(cppcoroservicelib_consumer LANGUAGES CXX)

find_package(cppcoroservicelib REQUIRED)

add_executable(cppcoroservicelib_consumer main.cpp)
target_link_libraries(cppcoroservicelib_consumer PRIVATE
    servicelib::servicelib servicelib::cron)
