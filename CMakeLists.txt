cmake_minimum_required(VERSION 3.24)

project(cppcoroservicelib VERSION 0.2.151 LANGUAGES CXX)

option(USE_CCACHE "Use ccache when it is available" ON)
if(USE_CCACHE AND NOT CMAKE_CXX_COMPILER_LAUNCHER)
  find_program(SERVICEGEN_CCACHE_PROGRAM ccache)
  if(SERVICEGEN_CCACHE_PROGRAM)
    set(CMAKE_CXX_COMPILER_LAUNCHER "${SERVICEGEN_CCACHE_PROGRAM}")
    message(STATUS "C++ compiler cache: ${SERVICEGEN_CCACHE_PROGRAM}")
  endif()
endif()

include(CTest)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(cmake/CompilerOptions.cmake)
include(cmake/CoroIO.cmake)

option(CPPCOROSERVICELIB_BUILD_TESTS "Build framework tests" ON)
include(cmake/Dependencies.cmake)

find_package(Threads REQUIRED GLOBAL)
find_package(OpenSSL REQUIRED GLOBAL)

# FetchContent dependency targets expose their headers as ordinary include
# directories. Keep warnings strict for ServiceLib and its consumers, but do
# not turn diagnostics in third-party headers into ServiceLib build failures.
function(_servicelib_mark_dependency_includes_system target_name)
  if(NOT TARGET "${target_name}")
    return()
  endif()
  get_target_property(real_target "${target_name}" ALIASED_TARGET)
  if(NOT real_target)
    set(real_target "${target_name}")
  endif()
  get_target_property(dependency_includes "${real_target}"
      INTERFACE_INCLUDE_DIRECTORIES)
  if(dependency_includes)
    set_property(TARGET "${real_target}" APPEND PROPERTY
        INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${dependency_includes}")
  endif()
endfunction()

foreach(dependency_target
    Boost::headers Boost::asio Boost::beast Boost::json
    gRPC::grpc++ gRPC::grpc protobuf::libprotobuf)
  _servicelib_mark_dependency_includes_system("${dependency_target}")
endforeach()

if(TARGET yaml-cpp::yaml-cpp)
  set(CPPCOROSERVICELIB_YAML_TARGET yaml-cpp::yaml-cpp)
else()
  set(CPPCOROSERVICELIB_YAML_TARGET yaml-cpp)
endif()

add_library(cppcoroservicelib INTERFACE)
add_library(servicelib::servicelib ALIAS cppcoroservicelib)
set_target_properties(cppcoroservicelib PROPERTIES EXPORT_NAME servicelib)

target_compile_features(cppcoroservicelib INTERFACE cxx_std_20)
target_include_directories(cppcoroservicelib INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
if(CPPCOROSERVICELIB_DEPENDENCY_MODE MATCHES "^(CONAN|SYSTEM)$")
  target_link_libraries(cppcoroservicelib INTERFACE
      Boost::headers Boost::json Threads::Threads
      ${CPPCOROSERVICELIB_YAML_TARGET})
else()
  # FETCH/LOCAL targets belong to their own projects and cannot be members of
  # our install export. Consume their build-tree usage requirements without
  # exporting those targets; the installed package uses find_dependency.
  target_include_directories(cppcoroservicelib SYSTEM INTERFACE
      $<BUILD_INTERFACE:${CPPCOROSERVICELIB_BOOST_BUILD_INCLUDE_DIR}>
      $<BUILD_INTERFACE:${CPPCOROSERVICELIB_YAML_BUILD_INCLUDE_DIR}>)
  target_link_libraries(cppcoroservicelib INTERFACE
      Threads::Threads
      $<BUILD_INTERFACE:Boost::headers>
      $<BUILD_INTERFACE:Boost::asio>
      $<BUILD_INTERFACE:Boost::beast>
      $<BUILD_INTERFACE:Boost::json>
      $<BUILD_INTERFACE:${CPPCOROSERVICELIB_YAML_TARGET}>)
endif()
target_link_libraries(cppcoroservicelib INTERFACE
    $<BUILD_INTERFACE:cppcoroservicelib_build_options>
    cppcoro_io
    OpenSSL::Crypto)

if(CPPCOROSERVICELIB_ENABLE_CRON)
  set(CPPCOROSERVICELIB_CRON_REQUIRES_LIBCRON OFF)
  if(CPPCOROSERVICELIB_DEPENDENCY_MODE STREQUAL "CONAN")
    set(CPPCOROSERVICELIB_CRON_REQUIRES_LIBCRON ON)
    add_library(cppcoroservicelib_cron STATIC
        src/datasource/cron/libcron.cpp)
  else()
    # libcron v1.3.3 predates the C++20 calendar names and intentionally
    # builds its own sources as C++17 in the legacy provider.
    add_library(cppcoroservicelib_libcron_objects OBJECT
        ${libcron_SOURCE_DIR}/libcron/src/CronClock.cpp
        ${libcron_SOURCE_DIR}/libcron/src/CronData.cpp
        ${libcron_SOURCE_DIR}/libcron/src/CronRandomization.cpp
        ${libcron_SOURCE_DIR}/libcron/src/CronSchedule.cpp
        ${libcron_SOURCE_DIR}/libcron/src/Task.cpp)
    set_target_properties(cppcoroservicelib_libcron_objects PROPERTIES
        CXX_STANDARD 17 CXX_STANDARD_REQUIRED YES CXX_EXTENSIONS NO)
    target_compile_definitions(cppcoroservicelib_libcron_objects PRIVATE
        HAS_UNCAUGHT_EXCEPTIONS)
    target_include_directories(cppcoroservicelib_libcron_objects SYSTEM PRIVATE
        ${libcron_SOURCE_DIR}/libcron/include
        ${libcron_date_SOURCE_DIR}/include)
    add_library(cppcoroservicelib_cron STATIC
        src/datasource/cron/libcron.cpp
        $<TARGET_OBJECTS:cppcoroservicelib_libcron_objects>)
  endif()
  add_library(servicelib::cron ALIAS cppcoroservicelib_cron)
  set_target_properties(cppcoroservicelib_cron PROPERTIES EXPORT_NAME cron)
  target_compile_features(cppcoroservicelib_cron PUBLIC cxx_std_20)
  target_link_libraries(cppcoroservicelib_cron PUBLIC servicelib::servicelib)
  if(CPPCOROSERVICELIB_DEPENDENCY_MODE STREQUAL "CONAN")
    target_link_libraries(cppcoroservicelib_cron PRIVATE libcron::libcron)
  else()
    target_include_directories(cppcoroservicelib_cron SYSTEM PRIVATE
        ${libcron_SOURCE_DIR}/libcron/include
        ${libcron_date_SOURCE_DIR}/include)
  endif()
endif()

if(CPPCOROSERVICELIB_ENABLE_GRPC)
  # Transport dependencies stay in a separate component: HTTP-only consumers
  # do not inherit protobuf/gRPC, while gRPC services get the exact headers,
  # compile definitions and libraries required by GrpcRuntime and adapters.
  add_library(cppcoroservicelib_grpc INTERFACE)
  add_library(servicelib::grpc ALIAS cppcoroservicelib_grpc)
  set_target_properties(cppcoroservicelib_grpc PROPERTIES EXPORT_NAME grpc)
  if(NOT CPPCOROSERVICELIB_DEPENDENCY_MODE MATCHES "^(CONAN|SYSTEM)$")
    target_include_directories(cppcoroservicelib_grpc SYSTEM INTERFACE
        $<BUILD_INTERFACE:${asio-grpc_SOURCE_DIR}/src>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/abseil-cpp>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/re2>
        $<BUILD_INTERFACE:${grpc_BINARY_DIR}/third_party/cares/cares>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/cares/cares>
        $<BUILD_INTERFACE:${grpc_SOURCE_DIR}/third_party/cares/cares/include>
        $<BUILD_INTERFACE:${protobuf_SOURCE_DIR}/src>)
  endif()
  target_link_libraries(cppcoroservicelib_grpc INTERFACE
      $<BUILD_INTERFACE:servicelib::servicelib>
      $<BUILD_INTERFACE:asio-grpc::asio-grpc>
      $<BUILD_INTERFACE:${CPPCOROSERVICELIB_GRPCPP_TARGET}>
      $<INSTALL_INTERFACE:servicelib::servicelib>
      $<INSTALL_INTERFACE:asio-grpc::asio-grpc>
      $<INSTALL_INTERFACE:gRPC::grpc++>)
endif()

if(CPPCOROSERVICELIB_ENABLE_KAFKA)
  _servicelib_mark_dependency_includes_system(
      "${CPPCOROSERVICELIB_RDKAFKA_TARGET}")
  add_library(cppcoroservicelib_kafka INTERFACE)
  add_library(servicelib::kafka ALIAS cppcoroservicelib_kafka)
  set_target_properties(cppcoroservicelib_kafka PROPERTIES EXPORT_NAME kafka)
  if(NOT CPPCOROSERVICELIB_DEPENDENCY_MODE MATCHES "^(CONAN|SYSTEM)$")
    target_include_directories(cppcoroservicelib_kafka SYSTEM INTERFACE
        $<BUILD_INTERFACE:${RdKafka_SOURCE_DIR}/src>)
  endif()
  target_link_libraries(cppcoroservicelib_kafka INTERFACE
      $<BUILD_INTERFACE:servicelib::servicelib>
      $<BUILD_INTERFACE:${CPPCOROSERVICELIB_RDKAFKA_TARGET}>
      $<INSTALL_INTERFACE:servicelib::servicelib>
      $<INSTALL_INTERFACE:RdKafka::rdkafka>)
endif()

if(CPPCOROSERVICELIB_ENABLE_OTEL)
  foreach(dependency_target IN ITEMS
      opentelemetry_api opentelemetry_sdk
      opentelemetry-cpp::api opentelemetry-cpp::sdk
      ${CPPCOROSERVICELIB_OTEL_TRACE_TARGET}
      ${CPPCOROSERVICELIB_OTEL_LOGS_TARGET}
      ${CPPCOROSERVICELIB_OTEL_GRPC_TARGET}
      ${CPPCOROSERVICELIB_OTEL_GRPC_LOG_TARGET}
      ${CPPCOROSERVICELIB_OTEL_OSTREAM_TRACE_TARGET})
    _servicelib_mark_dependency_includes_system("${dependency_target}")
  endforeach()
  add_library(cppcoroservicelib_otel INTERFACE)
  add_library(servicelib::otel ALIAS cppcoroservicelib_otel)
  set_target_properties(cppcoroservicelib_otel PROPERTIES EXPORT_NAME otel)
  target_link_libraries(cppcoroservicelib_otel INTERFACE
      $<BUILD_INTERFACE:servicelib::servicelib>
      $<BUILD_INTERFACE:${CPPCOROSERVICELIB_OTEL_TRACE_TARGET}>
      $<BUILD_INTERFACE:${CPPCOROSERVICELIB_OTEL_LOGS_TARGET}>
      $<BUILD_INTERFACE:${CPPCOROSERVICELIB_OTEL_GRPC_TARGET}>
      $<BUILD_INTERFACE:${CPPCOROSERVICELIB_OTEL_GRPC_LOG_TARGET}>
      $<BUILD_INTERFACE:${CPPCOROSERVICELIB_OTEL_OSTREAM_TRACE_TARGET}>
      $<INSTALL_INTERFACE:servicelib::servicelib>
      $<INSTALL_INTERFACE:opentelemetry-cpp::trace>
      $<INSTALL_INTERFACE:opentelemetry-cpp::logs>
      $<INSTALL_INTERFACE:opentelemetry-cpp::otlp_grpc_exporter>
      $<INSTALL_INTERFACE:opentelemetry-cpp::otlp_grpc_log_record_exporter>)
  target_link_libraries(cppcoroservicelib_otel INTERFACE
      $<INSTALL_INTERFACE:opentelemetry-cpp::ostream_span_exporter>)
endif()

if(CPPCOROSERVICELIB_BUILD_TESTS)
  if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
    # Test fixtures intentionally use partial designated initialization for
    # config aggregates whose omitted fields have framework defaults.
    # Release normally defines NDEBUG, but several standalone tests deliberately
    # use the standard assert contract.  Keep assertions enabled for test
    # targets only so Release verification executes the same checks as Debug.
    # Keep our tests warning-clean without turning deprecation diagnostics from
    # FetchContent dependencies (notably gRPC public compatibility headers)
    # into third-party build failures.
    add_compile_options(
        -UNDEBUG
        -Werror
        -Wno-error=deprecated-declarations
        -Wno-missing-field-initializers)
  endif()

  add_test(NAME cppcoroservicelib_structure_test
      COMMAND ${CMAKE_COMMAND}
          -DCPPCOROSERVICELIB_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}
          -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/VerifyStructure.cmake)

  add_executable(cppcoroservicelib_config_loader_test
      tests/config_loader_test.cpp)
  target_link_libraries(cppcoroservicelib_config_loader_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppcoroservicelib_config_loader_test
      COMMAND cppcoroservicelib_config_loader_test)

  add_executable(cppcoroservicelib_schedule_test
      tests/schedule_test.cpp)
  target_link_libraries(cppcoroservicelib_schedule_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppcoroservicelib_schedule_test
      COMMAND cppcoroservicelib_schedule_test)
  if(CPPCOROSERVICELIB_ENABLE_CRON)
    add_executable(cppcoroservicelib_cron_test tests/cron_test.cpp)
    target_link_libraries(cppcoroservicelib_cron_test PRIVATE
        servicelib::cron GTest::gtest_main)
    add_test(NAME cppcoroservicelib_cron_test
        COMMAND cppcoroservicelib_cron_test)
  endif()

  add_executable(cppcoroservicelib_asio_runtime_test
      tests/asio_runtime_test.cpp)
  target_link_libraries(cppcoroservicelib_asio_runtime_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppcoroservicelib_asio_runtime_test
      COMMAND cppcoroservicelib_asio_runtime_test)

  if(CPPCOROSERVICELIB_COROUTINE_DIAGNOSTICS)
    add_executable(cppcoroservicelib_asio_handler_diagnostics_test
        tests/asio_handler_diagnostics_test.cpp)
    target_link_libraries(cppcoroservicelib_asio_handler_diagnostics_test
        PRIVATE servicelib::servicelib)
    add_test(NAME cppcoroservicelib_asio_handler_diagnostics_test
        COMMAND cppcoroservicelib_asio_handler_diagnostics_test)
  endif()

  add_executable(cppcoroservicelib_taskpool_test tests/taskpool_test.cpp)
  add_executable(cppcoroservicelib_coroutine_mutex_test
      tests/coroutine_mutex_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_mutex_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_mutex_test
      COMMAND cppcoroservicelib_coroutine_mutex_test)
  set_tests_properties(cppcoroservicelib_coroutine_mutex_test PROPERTIES TIMEOUT 30)

  target_link_libraries(cppcoroservicelib_taskpool_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppcoroservicelib_taskpool_test
      COMMAND cppcoroservicelib_taskpool_test)

  add_executable(cppcoroservicelib_other_pools_test
      tests/other_pools_test.cpp)
  target_link_libraries(cppcoroservicelib_other_pools_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_other_pools_test
      COMMAND cppcoroservicelib_other_pools_test)

  add_executable(cppcoroservicelib_operators_test
      tests/operators_compile_test.cpp)
  add_executable(cppcoroservicelib_direct_caller_queue_test
      tests/direct_caller_queue_test.cpp)
  target_link_libraries(cppcoroservicelib_direct_caller_queue_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppcoroservicelib_direct_caller_queue_test
      COMMAND cppcoroservicelib_direct_caller_queue_test)
  add_executable(cppcoroservicelib_substream_test tests/substream_test.cpp)
  add_executable(cppcoroservicelib_coroutine_execution_test
      tests/coroutine_execution_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_execution_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_execution_test
      COMMAND cppcoroservicelib_coroutine_execution_test)
  set_tests_properties(cppcoroservicelib_coroutine_execution_test PROPERTIES
      TIMEOUT 30)
  target_link_libraries(cppcoroservicelib_substream_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_substream_test
      COMMAND cppcoroservicelib_substream_test)
  target_link_libraries(cppcoroservicelib_operators_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_operators_test
      COMMAND cppcoroservicelib_operators_test)

  add_executable(cppcoroservicelib_operators_topology_test
      tests/operators_topology_test.cpp)
  target_link_libraries(cppcoroservicelib_operators_topology_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_operators_topology_test
      COMMAND cppcoroservicelib_operators_topology_test)

  add_executable(cppcoroservicelib_join_topology_test
      tests/join_topology_test.cpp)
  target_link_libraries(cppcoroservicelib_join_topology_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_join_topology_test
      COMMAND cppcoroservicelib_join_topology_test)

  add_executable(cppcoroservicelib_store_test tests/store_test.cpp)
  target_link_libraries(cppcoroservicelib_store_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_store_test
      COMMAND cppcoroservicelib_store_test)

  add_executable(cppcoroservicelib_serde_test tests/serde_test.cpp)
  target_link_libraries(cppcoroservicelib_serde_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppcoroservicelib_serde_test
      COMMAND cppcoroservicelib_serde_test)

  add_executable(cppcoroservicelib_serviceapp_test tests/serviceapp_test.cpp)
  target_link_libraries(cppcoroservicelib_serviceapp_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppcoroservicelib_serviceapp_test
      COMMAND cppcoroservicelib_serviceapp_test)

  add_executable(cppcoroservicelib_telemetry_test tests/telemetry_test.cpp)
  target_link_libraries(cppcoroservicelib_telemetry_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  target_compile_definitions(cppcoroservicelib_telemetry_test PRIVATE
      SERVICELIB_TEST_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
  add_test(NAME cppcoroservicelib_telemetry_test
      COMMAND cppcoroservicelib_telemetry_test)

  add_executable(cppcoroservicelib_tracing_test tests/tracing_test.cpp)
  target_link_libraries(cppcoroservicelib_tracing_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_tracing_test
      COMMAND cppcoroservicelib_tracing_test)

  add_executable(cppcoroservicelib_prometheus_test tests/prometheus_test.cpp)
  target_link_libraries(cppcoroservicelib_prometheus_test PRIVATE
      servicelib::servicelib)
  add_test(NAME cppcoroservicelib_prometheus_test
      COMMAND cppcoroservicelib_prometheus_test)

  if(CPPCOROSERVICELIB_ENABLE_OTEL)
    if(TARGET opentelemetry_exporter_in_memory)
      set(_servicelib_otel_memory_test_target
          opentelemetry_exporter_in_memory)
    else()
      set(_servicelib_otel_memory_test_target
          opentelemetry-cpp::in_memory_span_exporter)
    endif()
    if(TARGET opentelemetry_exporter_ostream_logs)
      set(_servicelib_otel_ostream_logs_test_target
          opentelemetry_exporter_ostream_logs)
    else()
      set(_servicelib_otel_ostream_logs_test_target
          opentelemetry-cpp::ostream_log_record_exporter)
    endif()
    _servicelib_mark_dependency_includes_system(
        "${_servicelib_otel_memory_test_target}")
    _servicelib_mark_dependency_includes_system(
        "${_servicelib_otel_ostream_logs_test_target}")
    add_executable(cppcoroservicelib_opentelemetry_test
        tests/opentelemetry_test.cpp)
    target_link_libraries(cppcoroservicelib_opentelemetry_test PRIVATE
        servicelib::otel GTest::gtest_main
        ${_servicelib_otel_memory_test_target}
        ${_servicelib_otel_ostream_logs_test_target})
    add_test(NAME cppcoroservicelib_opentelemetry_test
        COMMAND cppcoroservicelib_opentelemetry_test)
    unset(_servicelib_otel_memory_test_target)
    unset(_servicelib_otel_ostream_logs_test_target)
  endif()

  add_executable(cppcoroservicelib_status_test tests/status_test.cpp)
  target_link_libraries(cppcoroservicelib_status_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_status_test
      COMMAND cppcoroservicelib_status_test)

  add_executable(cppcoroservicelib_http_endpoints_test
      tests/http_endpoints_test.cpp)
  target_link_libraries(cppcoroservicelib_http_endpoints_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_http_endpoints_test
      COMMAND cppcoroservicelib_http_endpoints_test)

  add_executable(cppcoroservicelib_custom_endpoints_test
      tests/custom_endpoints_test.cpp)
  target_link_libraries(cppcoroservicelib_custom_endpoints_test PRIVATE
      servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_custom_endpoints_test
      COMMAND cppcoroservicelib_custom_endpoints_test)

  if(CPPCOROSERVICELIB_ENABLE_KAFKA)
    add_executable(cppcoroservicelib_kafka_endpoints_test
        tests/kafka_endpoints_test.cpp)
    target_link_libraries(cppcoroservicelib_kafka_endpoints_test PRIVATE
        servicelib::kafka GTest::gtest_main)
    add_test(NAME cppcoroservicelib_kafka_endpoints_test
        COMMAND cppcoroservicelib_kafka_endpoints_test)
  endif()

  if(CPPCOROSERVICELIB_ENABLE_GRPC)
    asio_grpc_protobuf_generate(
        GENERATE_GRPC
        OUT_VAR CPPCOROSERVICELIB_TEST_PROTO_SOURCES
        OUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated"
        IMPORT_DIRS "${CMAKE_CURRENT_SOURCE_DIR}/tests/proto"
        PROTOS "${CMAKE_CURRENT_SOURCE_DIR}/tests/proto/connector_test.proto")
    add_library(cppcoroservicelib_test_proto OBJECT
        ${CPPCOROSERVICELIB_TEST_PROTO_SOURCES})
    target_link_libraries(cppcoroservicelib_test_proto PUBLIC
        protobuf::libprotobuf ${CPPCOROSERVICELIB_GRPCPP_TARGET})
    target_include_directories(cppcoroservicelib_test_proto SYSTEM PUBLIC
        "${CMAKE_CURRENT_BINARY_DIR}/generated")

    add_executable(cppcoroservicelib_grpc_runtime_test
        tests/grpc_runtime_test.cpp)
    target_link_libraries(cppcoroservicelib_grpc_runtime_test PRIVATE
        servicelib::grpc)
    add_test(NAME cppcoroservicelib_grpc_runtime_test
        COMMAND cppcoroservicelib_grpc_runtime_test)

    add_executable(cppcoroservicelib_grpc_endpoints_test
        tests/grpc_endpoints_test.cpp
        tests/grpc_streaming_lifecycle_test.cpp)
    target_link_libraries(cppcoroservicelib_grpc_endpoints_test PRIVATE
        servicelib::grpc cppcoroservicelib_test_proto GTest::gtest_main)
    add_test(NAME cppcoroservicelib_grpc_endpoints_test
        COMMAND cppcoroservicelib_grpc_endpoints_test)

    add_executable(cppcoroservicelib_grpc_unary_test
        tests/grpc_unary_test.cpp)
    target_link_libraries(cppcoroservicelib_grpc_unary_test PRIVATE
        servicelib::grpc cppcoroservicelib_test_proto)
    add_test(NAME cppcoroservicelib_grpc_unary_test
        COMMAND cppcoroservicelib_grpc_unary_test)

    # Explicit diagnostic target, not a timing-dependent correctness test.
    add_executable(cppcoroservicelib_grpc_client_benchmark EXCLUDE_FROM_ALL
        tests/grpc_client_benchmark.cpp)
    target_link_libraries(cppcoroservicelib_grpc_client_benchmark PRIVATE
        servicelib::grpc cppcoroservicelib_test_proto)

    add_executable(cppcoroservicelib_http_endpoint_benchmark EXCLUDE_FROM_ALL
        tests/http_endpoint_benchmark.cpp)
    target_link_libraries(cppcoroservicelib_http_endpoint_benchmark PRIVATE
        servicelib::grpc cppcoroservicelib_test_proto)

    add_executable(cppcoroservicelib_grpc_streaming_test
        tests/grpc_streaming_test.cpp)
    target_link_libraries(cppcoroservicelib_grpc_streaming_test PRIVATE
        servicelib::grpc cppcoroservicelib_test_proto)
    add_test(NAME cppcoroservicelib_grpc_streaming_test
        COMMAND cppcoroservicelib_grpc_streaming_test)
  endif()
endif()

install(TARGETS cppcoroservicelib EXPORT cppcoroservicelibTargets)
if(CPPCOROSERVICELIB_ENABLE_CRON)
  install(TARGETS cppcoroservicelib_cron
      EXPORT cppcoroservicelibTargets
      ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT cppcoroservicelibTargets
    FILE cppcoroservicelibTargets.cmake
    NAMESPACE servicelib::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cppcoroservicelib)

configure_package_config_file(
    cmake/cppcoroservicelibConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/cppcoroservicelibConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cppcoroservicelib)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/cppcoroservicelibConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/cppcoroservicelibConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/cppcoroservicelibConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/cppcoroservicelib)

if(BUILD_TESTING AND CPPCOROSERVICELIB_BUILD_TESTS)
  add_executable(cppcoroservicelib_coroutine_join_storage_test
    tests/coroutine_join_storage_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_join_storage_test
    PRIVATE servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_join_storage_test
    COMMAND cppcoroservicelib_coroutine_join_storage_test)
  set_tests_properties(cppcoroservicelib_coroutine_join_storage_test
    PROPERTIES TIMEOUT 30)
endif()

if(BUILD_TESTING AND CPPCOROSERVICELIB_BUILD_TESTS)
  add_executable(cppcoroservicelib_coroutine_task_executor_test
    tests/coroutine_task_executor_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_task_executor_test
    PRIVATE servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_task_executor_test
    COMMAND cppcoroservicelib_coroutine_task_executor_test)
  set_tests_properties(cppcoroservicelib_coroutine_task_executor_test
    PROPERTIES TIMEOUT 30)
endif()

if(BUILD_TESTING AND CPPCOROSERVICELIB_BUILD_TESTS)
  foreach(coroutine_test IN ITEMS pool lifecycle)
    add_executable(cppcoroservicelib_coroutine_${coroutine_test}_test
      tests/coroutine_${coroutine_test}_test.cpp)
    target_link_libraries(cppcoroservicelib_coroutine_${coroutine_test}_test
      PRIVATE servicelib::servicelib GTest::gtest_main)
    add_test(NAME cppcoroservicelib_coroutine_${coroutine_test}_test
      COMMAND cppcoroservicelib_coroutine_${coroutine_test}_test)
    set_tests_properties(cppcoroservicelib_coroutine_${coroutine_test}_test
      PROPERTIES TIMEOUT 30)
  endforeach()
endif()

if(BUILD_TESTING AND CPPCOROSERVICELIB_BUILD_TESTS)
  add_executable(cppcoroservicelib_coroutine_http_test tests/coroutine_http_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_http_test
    PRIVATE servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_http_test
    COMMAND cppcoroservicelib_coroutine_http_test)
  set_tests_properties(cppcoroservicelib_coroutine_http_test PROPERTIES TIMEOUT 30)
endif()

if(BUILD_TESTING AND CPPCOROSERVICELIB_BUILD_TESTS AND TARGET servicelib::grpc)
  add_executable(cppcoroservicelib_coroutine_grpc_unary_test tests/coroutine_grpc_unary_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_grpc_unary_test PRIVATE servicelib::grpc GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_grpc_unary_test COMMAND cppcoroservicelib_coroutine_grpc_unary_test)
  set_tests_properties(cppcoroservicelib_coroutine_grpc_unary_test PROPERTIES TIMEOUT 30)
endif()

if(BUILD_TESTING AND CPPCOROSERVICELIB_BUILD_TESTS AND TARGET servicelib::grpc)
  add_executable(cppcoroservicelib_coroutine_grpc_streaming_test
    tests/coroutine_grpc_streaming_test.cpp
    tests/grpc_streaming_lifecycle_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_grpc_streaming_test PRIVATE
    servicelib::grpc GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_grpc_streaming_test
    COMMAND cppcoroservicelib_coroutine_grpc_streaming_test)
  set_tests_properties(cppcoroservicelib_coroutine_grpc_streaming_test PROPERTIES TIMEOUT 30)
endif()

if(BUILD_TESTING AND CPPCOROSERVICELIB_BUILD_TESTS)
  add_executable(cppcoroservicelib_coroutine_local_source_test tests/coroutine_local_source_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_local_source_test PRIVATE servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_local_source_test COMMAND cppcoroservicelib_coroutine_local_source_test)
  set_tests_properties(cppcoroservicelib_coroutine_local_source_test PROPERTIES TIMEOUT 30)
endif()

if(BUILD_TESTING AND CPPCOROSERVICELIB_BUILD_TESTS)
  add_executable(cppcoroservicelib_coroutine_blocking_test tests/coroutine_blocking_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_blocking_test PRIVATE servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_blocking_test COMMAND cppcoroservicelib_coroutine_blocking_test)
  set_tests_properties(cppcoroservicelib_coroutine_blocking_test PROPERTIES TIMEOUT 30)
  if(TARGET servicelib::cron)
    add_executable(cppcoroservicelib_coroutine_cron_test tests/coroutine_cron_test.cpp)
    target_link_libraries(cppcoroservicelib_coroutine_cron_test PRIVATE servicelib::cron GTest::gtest_main)
    add_test(NAME cppcoroservicelib_coroutine_cron_test COMMAND cppcoroservicelib_coroutine_cron_test)
    set_tests_properties(cppcoroservicelib_coroutine_cron_test PROPERTIES TIMEOUT 30)
  endif()
endif()

if(BUILD_TESTING AND CPPCOROSERVICELIB_BUILD_TESTS)
  add_executable(cppcoroservicelib_coroutine_initialization_test tests/coroutine_initialization_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_initialization_test PRIVATE servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_initialization_test COMMAND cppcoroservicelib_coroutine_initialization_test)
  set_tests_properties(cppcoroservicelib_coroutine_initialization_test PROPERTIES TIMEOUT 30)
endif()

if(BUILD_TESTING AND CPPCOROSERVICELIB_BUILD_TESTS)
  add_executable(cppcoroservicelib_coroutine_selectors_test tests/coroutine_selectors_test.cpp)
  target_link_libraries(cppcoroservicelib_coroutine_selectors_test PRIVATE servicelib::servicelib GTest::gtest_main)
  add_test(NAME cppcoroservicelib_coroutine_selectors_test COMMAND cppcoroservicelib_coroutine_selectors_test)
  set_tests_properties(cppcoroservicelib_coroutine_selectors_test PROPERTIES TIMEOUT 30)
endif()

include(cmake/CoroTransport.cmake)
