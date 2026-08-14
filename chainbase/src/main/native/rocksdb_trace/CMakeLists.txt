cmake_minimum_required(VERSION 3.16)
project(java_tron_rocksdb_trace LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "RocksDB trace bridge is supported only on Linux")
endif()

if(NOT DEFINED ROCKSDB_SOURCE_DIR)
  message(FATAL_ERROR "ROCKSDB_SOURCE_DIR must point to RocksDB v9.7.4 source")
endif()

if(NOT DEFINED ENV{JAVA_HOME})
  message(FATAL_ERROR "JAVA_HOME must point to a JDK containing include/jni.h")
endif()

set(JAVA_INCLUDE_DIR "$ENV{JAVA_HOME}/include")
set(JAVA_PLATFORM_INCLUDE_DIR "${JAVA_INCLUDE_DIR}/linux")

if(NOT EXISTS "${JAVA_INCLUDE_DIR}/jni.h")
  message(FATAL_ERROR "jni.h not found under JAVA_HOME=$ENV{JAVA_HOME}")
endif()

add_library(java_tron_rocksdb_trace SHARED rocksdb_trace_bridge.cc)
target_compile_features(java_tron_rocksdb_trace PRIVATE cxx_std_17)
target_compile_definitions(java_tron_rocksdb_trace PRIVATE _GLIBCXX_USE_CXX11_ABI=0)
target_include_directories(java_tron_rocksdb_trace PRIVATE
  ${JAVA_INCLUDE_DIR}
  ${JAVA_PLATFORM_INCLUDE_DIR}
  ${ROCKSDB_SOURCE_DIR}/include)
target_compile_options(java_tron_rocksdb_trace PRIVATE -Wall -Wextra -Werror)
target_link_libraries(java_tron_rocksdb_trace PRIVATE dl)
target_link_options(java_tron_rocksdb_trace PRIVATE -Wl,-Bsymbolic)
