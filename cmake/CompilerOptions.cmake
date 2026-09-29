option(CPPCOROSERVICELIB_ASAN "Enable AddressSanitizer" OFF)
option(CPPCOROSERVICELIB_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
option(CPPCOROSERVICELIB_TSAN "Enable ThreadSanitizer" OFF)
option(CPPCOROSERVICELIB_COVERAGE "Enable source coverage" OFF)
option(CPPCOROSERVICELIB_PROFILING "Enable profiler-friendly code generation" OFF)
option(CPPCOROSERVICELIB_COROUTINE_DIAGNOSTICS
       "Enable profiling-only Asio handler state diagnostics" OFF)
set(CPPCOROSERVICELIB_ASIO_RECYCLING_CACHE_SIZE "2" CACHE STRING
    "Per-thread Boost.Asio recycled operation blocks retained per allocator tag")
if(NOT CPPCOROSERVICELIB_ASIO_RECYCLING_CACHE_SIZE MATCHES "^[1-9][0-9]*$")
  message(FATAL_ERROR
      "CPPCOROSERVICELIB_ASIO_RECYCLING_CACHE_SIZE must be a positive integer")
endif()

if(CPPCOROSERVICELIB_COROUTINE_DIAGNOSTICS AND
   NOT CPPCOROSERVICELIB_PROFILING)
  message(FATAL_ERROR
      "CPPCOROSERVICELIB_COROUTINE_DIAGNOSTICS requires CPPCOROSERVICELIB_PROFILING")
endif()

set(_servicelib_sanitizers 0)
foreach(_option CPPCOROSERVICELIB_ASAN CPPCOROSERVICELIB_TSAN)
  if(${_option})
    math(EXPR _servicelib_sanitizers "${_servicelib_sanitizers} + 1")
  endif()
endforeach()
if(_servicelib_sanitizers GREATER 1)
  message(FATAL_ERROR "ASan and TSan cannot be enabled together")
endif()

add_library(cppcoroservicelib_build_options INTERFACE)
target_compile_definitions(cppcoroservicelib_build_options INTERFACE
    BOOST_ASIO_RECYCLING_ALLOCATOR_CACHE_SIZE=${CPPCOROSERVICELIB_ASIO_RECYCLING_CACHE_SIZE})
if(CPPCOROSERVICELIB_COROUTINE_DIAGNOSTICS)
  target_compile_definitions(cppcoroservicelib_build_options INTERFACE
      CPPCOROSERVICELIB_COROUTINE_DIAGNOSTICS=1
      BOOST_ASIO_CUSTOM_HANDLER_TRACKING=<servicelib/runtime/detail/asio_handler_diagnostics.hpp>)
endif()
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
  target_compile_options(cppcoroservicelib_build_options INTERFACE
      -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion)
  if(CPPCOROSERVICELIB_ASAN OR CPPCOROSERVICELIB_UBSAN OR
     CPPCOROSERVICELIB_TSAN)
    # Sanitizer builds retain optimized Release code while keeping symbolic
    # stacks and frame pointers for actionable reports.
    add_compile_options(-g -fno-omit-frame-pointer)
  endif()
  if(CPPCOROSERVICELIB_ASAN)
    # Conan supplies matching sanitizer flags to the complete static
    # dependency graph; these options instrument this project and consumers.
    add_compile_options(
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=address>
        $<$<COMPILE_LANGUAGE:C,CXX>:-fno-omit-frame-pointer>)
    add_link_options(-fsanitize=address)
    # Propagate the flags through the public build-options target so sibling
    # consumer targets are instrumented and linked with the ASan runtime too.
    target_compile_options(cppcoroservicelib_build_options INTERFACE
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=address>
        $<$<COMPILE_LANGUAGE:C,CXX>:-fno-omit-frame-pointer>)
    target_link_options(cppcoroservicelib_build_options INTERFACE
        -fsanitize=address)
  endif()
  if(CPPCOROSERVICELIB_UBSAN)
    # The generated ASan profile also supplies UBSan to the complete static
    # dependency graph through Conan.
    target_compile_options(cppcoroservicelib_build_options INTERFACE
        -fsanitize=undefined -fno-omit-frame-pointer)
    target_link_options(cppcoroservicelib_build_options INTERFACE
        -fsanitize=undefined)
  endif()
  if(CPPCOROSERVICELIB_TSAN)
    # Conan supplies matching TSan flags to the complete static dependency
    # graph; these options instrument this project and consumers.
    add_compile_options(
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=thread>
        $<$<COMPILE_LANGUAGE:C,CXX>:-fno-omit-frame-pointer>)
    add_link_options(-fsanitize=thread)
    target_compile_options(cppcoroservicelib_build_options INTERFACE
        $<$<COMPILE_LANGUAGE:C,CXX>:-fsanitize=thread>
        $<$<COMPILE_LANGUAGE:C,CXX>:-fno-omit-frame-pointer>)
    target_link_options(cppcoroservicelib_build_options INTERFACE
        -fsanitize=thread)
  endif()
  if(CPPCOROSERVICELIB_COVERAGE)
    target_compile_options(cppcoroservicelib_build_options INTERFACE
        --coverage -O0 -g)
    target_link_options(cppcoroservicelib_build_options INTERFACE --coverage)
  endif()
  if(CPPCOROSERVICELIB_PROFILING)
    target_compile_options(cppcoroservicelib_build_options INTERFACE
        -g -fno-omit-frame-pointer)
  endif()
endif()
