if(NOT TARGET servicelib::uring)
  find_path(CPP_CORO_URING_INCLUDE_DIR NAMES liburing.h REQUIRED)
  find_library(CPP_CORO_URING_LIBRARY NAMES uring REQUIRED)
  add_library(servicelib::uring UNKNOWN IMPORTED GLOBAL)
  set_target_properties(servicelib::uring PROPERTIES
      IMPORTED_LOCATION "${CPP_CORO_URING_LIBRARY}"
      INTERFACE_INCLUDE_DIRECTORIES "${CPP_CORO_URING_INCLUDE_DIR}")
endif()
