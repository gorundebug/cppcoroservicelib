if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "The shared coroutine HTTP/gRPC transport requires Linux")
endif()

set(CPP_CORO_IO_BACKEND "epoll" CACHE STRING "Coro I/O backend: epoll or uring")
set_property(CACHE CPP_CORO_IO_BACKEND PROPERTY STRINGS epoll uring)
if(NOT CPP_CORO_IO_BACKEND MATCHES "^(epoll|uring)$")
  message(FATAL_ERROR "CPP_CORO_IO_BACKEND must be epoll or uring")
endif()

# Export the same backend definitions to every consumer of inline Asio code.
# Keeping these only on a private build-options target would mix ABIs after
# installing the library or linking a separately compiled gRPC adapter.
add_library(cppcoro_io INTERFACE)
add_library(servicelib::coro_io ALIAS cppcoro_io)
set_property(TARGET cppcoro_io PROPERTY
    INTERFACE_SERVICELIB_CORO_IO_BACKEND "${CPP_CORO_IO_BACKEND}")
set_property(TARGET cppcoro_io APPEND PROPERTY
    COMPATIBLE_INTERFACE_STRING SERVICELIB_CORO_IO_BACKEND)
set_target_properties(cppcoro_io PROPERTIES EXPORT_NAME coro_io)
if(CPP_CORO_IO_BACKEND STREQUAL "uring")
  include("${CMAKE_CURRENT_LIST_DIR}/CoroUring.cmake")
  target_compile_definitions(cppcoro_io INTERFACE
      SERVICELIB_CORO_IO_URING=1 BOOST_ASIO_HAS_IO_URING BOOST_ASIO_DISABLE_EPOLL)
  target_link_libraries(cppcoro_io INTERFACE servicelib::uring)
else()
  target_compile_definitions(cppcoro_io INTERFACE
      BOOST_ASIO_DISABLE_IO_URING BOOST_ASIO_HAS_EPOLL)
endif()
install(TARGETS cppcoro_io EXPORT cppcoroservicelibTargets)
install(FILES "${CMAKE_CURRENT_LIST_DIR}/CoroUring.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/cppcoroservicelib")
message(STATUS "Coro I/O backend: ${CPP_CORO_IO_BACKEND}")
