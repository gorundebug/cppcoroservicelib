cmake_minimum_required(VERSION 3.25)
project(coro_hiredis_eligibility LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
find_package(Boost 1.89 REQUIRED CONFIG)
find_package(hiredis 1.5.0 EXACT REQUIRED CONFIG)
find_package(hiredis_ssl REQUIRED CONFIG) # Official SSL package has no version file; built from the same pinned source.
find_package(Threads REQUIRED)
add_executable(coro_hiredis_probe probe.cpp)
target_link_libraries(coro_hiredis_probe PRIVATE Boost::headers hiredis::hiredis hiredis::hiredis_ssl Threads::Threads)
