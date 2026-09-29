# HTTP, gRPC and timers use the backend selected by CoroIO.cmake.
if(NOT TARGET c-ares::cares AND NOT TARGET cares)
  find_package(c-ares CONFIG REQUIRED)
endif()
if(TARGET c-ares::cares)
  set(_coro_cares c-ares::cares)
else()
  set(_coro_cares cares)
endif()
add_library(cppcoro_dns STATIC src/runtime/coro_resolver.cpp)
target_compile_features(cppcoro_dns PUBLIC cxx_std_20)
target_include_directories(cppcoro_dns PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include")
if(TARGET Boost::asio)
  set(_coro_dns_boost_target Boost::asio)
else()
  set(_coro_dns_boost_target Boost::headers)
endif()
target_link_libraries(cppcoro_dns PUBLIC
    $<BUILD_INTERFACE:${_coro_dns_boost_target}>
    Threads::Threads cppcoro_io
    PRIVATE ${_coro_cares} $<BUILD_INTERFACE:cppcoroservicelib_build_options>)
set_target_properties(cppcoro_dns PROPERTIES
    POSITION_INDEPENDENT_CODE ON EXPORT_NAME coro_dns)
target_link_libraries(cppcoroservicelib INTERFACE cppcoro_dns)
install(TARGETS cppcoro_dns EXPORT cppcoroservicelibTargets
    ARCHIVE DESTINATION lib LIBRARY DESTINATION lib)
if(CPPCOROSERVICELIB_BUILD_TESTS)
  set(_coro_backend_test "${CPP_CORO_IO_BACKEND}_backend_tests")
  add_executable(${_coro_backend_test} "tests/${_coro_backend_test}.cpp"
      tests/coro_worker_backend_tests.cpp)
  target_link_libraries(${_coro_backend_test} PRIVATE cppcoroservicelib GTest::gtest_main)
  add_test(NAME ${_coro_backend_test} COMMAND ${_coro_backend_test})
  set_tests_properties(${_coro_backend_test} PROPERTIES TIMEOUT 10)
  add_executable(coro_resolver_tests tests/coro_resolver_tests.cpp)
  target_link_libraries(coro_resolver_tests PRIVATE cppcoroservicelib GTest::gtest_main)
  add_test(NAME coro_resolver_tests COMMAND coro_resolver_tests)
  set_tests_properties(coro_resolver_tests PROPERTIES TIMEOUT 45)
endif()

if(CPPCOROSERVICELIB_ENABLE_GRPC)
  add_library(cppcoro_event_engine STATIC
      src/runtime/coro_event_engine.cpp src/runtime/coro_dns.cpp)
  add_library(servicelib::coro_event_engine ALIAS cppcoro_event_engine)
  target_compile_features(cppcoro_event_engine PUBLIC cxx_std_20)
  target_link_libraries(cppcoro_event_engine PUBLIC
      cppcoroservicelib ${CPPCOROSERVICELIB_GRPCPP_TARGET}
      PRIVATE ${_coro_cares})
  set_target_properties(cppcoro_event_engine PROPERTIES
      POSITION_INDEPENDENT_CODE ON EXPORT_NAME coro_event_engine)
  target_link_libraries(cppcoroservicelib_grpc INTERFACE cppcoro_event_engine)
  install(TARGETS cppcoro_event_engine EXPORT cppcoroservicelibTargets
      ARCHIVE DESTINATION lib LIBRARY DESTINATION lib)
  if(CPPCOROSERVICELIB_BUILD_TESTS)
    add_executable(coro_event_engine_tests tests/coro_event_engine_tests.cpp
        tests/coro_listener_handoff_tests.cpp)
    target_link_libraries(coro_event_engine_tests PRIVATE
        cppcoro_event_engine GTest::gtest_main)
    add_test(NAME coro_event_engine_tests COMMAND coro_event_engine_tests)
    set_tests_properties(coro_event_engine_tests PROPERTIES TIMEOUT 45)
    set(_coro_proto_dir "${CMAKE_CURRENT_BINARY_DIR}/coro-proto")
    file(MAKE_DIRECTORY "${_coro_proto_dir}")
    add_custom_command(
        OUTPUT "${_coro_proto_dir}/coro_transport.pb.cc"
               "${_coro_proto_dir}/coro_transport.pb.h"
               "${_coro_proto_dir}/coro_transport.grpc.pb.cc"
               "${_coro_proto_dir}/coro_transport.grpc.pb.h"
        COMMAND $<TARGET_FILE:protobuf::protoc>
            --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/tests/proto
            --cpp_out=${_coro_proto_dir} --grpc_out=${_coro_proto_dir}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/proto/coro_transport.proto
        DEPENDS tests/proto/coro_transport.proto
                protobuf::protoc gRPC::grpc_cpp_plugin VERBATIM)
    add_executable(coro_callback_transport_tests
        tests/coro_callback_transport_tests.cpp
        tests/coro_callback_endpoints_tests.cpp
        tests/coro_runtime_tests.cpp
        tests/coro_race_tests.cpp
        tests/coro_worker_transport_tests.cpp
        tests/worker_http_client_tests.cpp
        tests/coroutine_pool_test.cpp
        tests/worker_http_server_tests.cpp
        tests/worker_task_pool_tests.cpp
        tests/worker_rotating_map_tests.cpp
        tests/worker_join_storage_tests.cpp
        tests/worker_parallel_tests.cpp
        tests/worker_lifecycle_tests.cpp
        "${_coro_proto_dir}/coro_transport.pb.cc"
        "${_coro_proto_dir}/coro_transport.grpc.pb.cc")
    target_include_directories(coro_callback_transport_tests PRIVATE "${_coro_proto_dir}")
    target_link_libraries(coro_callback_transport_tests PRIVATE
        cppcoro_event_engine GTest::gtest_main protobuf::libprotobuf)
    add_test(NAME coro_callback_transport_tests COMMAND coro_callback_transport_tests)
    set_tests_properties(coro_callback_transport_tests PROPERTIES TIMEOUT 45)
  endif()
endif()
