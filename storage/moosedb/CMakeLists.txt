# MooseDB storage engine — build integrated with the MariaDB source tree.
#
# Copyright (c) 2026 Kevenny
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# The Rust core is built by cargo as a static library (libmoosedb.a) and
# linked into the ha_moosedb.so plugin.

FIND_PROGRAM(CARGO_EXECUTABLE cargo)
IF(NOT CARGO_EXECUTABLE)
  MESSAGE(STATUS "MooseDB: cargo not found, storage engine will not be built")
  RETURN()
ENDIF()

SET(MOOSEDB_RUST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/rust)
SET(MOOSEDB_CARGO_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}/cargo-target)
SET(MOOSEDB_RUST_LIB ${MOOSEDB_CARGO_TARGET_DIR}/release/libmoosedb.a)

FILE(GLOB_RECURSE MOOSEDB_RUST_SOURCES CONFIGURE_DEPENDS
  ${MOOSEDB_RUST_DIR}/moosedb-core/*.rs
  ${MOOSEDB_RUST_DIR}/moosedb-ffi/src/*.rs
  ${MOOSEDB_RUST_DIR}/moosedb-core/src/*/*.rs
  ${MOOSEDB_RUST_DIR}/*/Cargo.toml)

ADD_CUSTOM_COMMAND(
  OUTPUT ${MOOSEDB_RUST_LIB}
  COMMAND ${CMAKE_COMMAND} -E env CARGO_TARGET_DIR=${MOOSEDB_CARGO_TARGET_DIR}
          ${CARGO_EXECUTABLE} build --release --locked -p moosedb-ffi
          --manifest-path ${MOOSEDB_RUST_DIR}/Cargo.toml
  DEPENDS ${MOOSEDB_RUST_SOURCES}
          ${MOOSEDB_RUST_DIR}/Cargo.toml ${MOOSEDB_RUST_DIR}/Cargo.lock
  COMMENT "Building MooseDB Rust core (cargo)"
  VERBATIM)
ADD_CUSTOM_TARGET(moosedb_rust DEPENDS ${MOOSEDB_RUST_LIB})

# moosedb_ffi.h is committed so the C++ side builds without cbindgen at
# *compile* time, but the build must prove it matches the Rust FFI: a stale
# header is an ABI mismatch that only shows up at runtime. cbindgen --verify
# is therefore mandatory. MOOSEDB_SKIP_HEADER_CHECK=ON opts out explicitly.
OPTION(MOOSEDB_SKIP_HEADER_CHECK
  "Skip the cbindgen --verify check of moosedb_ffi.h (unsafe)" OFF)
IF(MOOSEDB_SKIP_HEADER_CHECK)
  MESSAGE(WARNING "MooseDB: MOOSEDB_SKIP_HEADER_CHECK=ON, moosedb_ffi.h is NOT "
                  "verified against the Rust FFI; a stale header means an ABI "
                  "mismatch at runtime")
ELSE()
  FIND_PROGRAM(CBINDGEN_EXECUTABLE cbindgen)
  IF(NOT CBINDGEN_EXECUTABLE)
    MESSAGE(FATAL_ERROR "MooseDB: cbindgen not found. It is required to verify "
      "that moosedb_ffi.h matches the Rust FFI (cargo install cbindgen). "
      "To build without the check at your own risk, pass "
      "-DMOOSEDB_SKIP_HEADER_CHECK=ON.")
  ENDIF()
  ADD_CUSTOM_TARGET(moosedb_ffi_header_check
    COMMAND ${CBINDGEN_EXECUTABLE} --verify
            --config ${MOOSEDB_RUST_DIR}/cbindgen.toml
            --output ${CMAKE_CURRENT_SOURCE_DIR}/moosedb_ffi.h
            ${MOOSEDB_RUST_DIR}/moosedb-ffi
    COMMENT "Checking that moosedb_ffi.h matches the Rust FFI"
    VERBATIM)
ENDIF()

MYSQL_ADD_PLUGIN(moosedb ha_moosedb.cc moosedb_gbh.cc
  STORAGE_ENGINE MODULE_ONLY
  LINK_LIBRARIES ${MOOSEDB_RUST_LIB} pthread dl m)

IF(TARGET moosedb)
  ADD_DEPENDENCIES(moosedb moosedb_rust)
  IF(TARGET moosedb_ffi_header_check)
    ADD_DEPENDENCIES(moosedb moosedb_ffi_header_check)
  ENDIF()
  SET_TARGET_PROPERTIES(moosedb PROPERTIES
    CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS OFF)
  # Keep the symbols of the static Rust library (std, zstd, ...) private to
  # the plugin, so they never interpose with other libraries in mysqld.
  # Only that library: the server patches the plugin-service pointers from
  # libmysqlservices.a (my_print_error_service, ...) through dlsym().
  IF(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    TARGET_LINK_OPTIONS(moosedb PRIVATE "LINKER:--exclude-libs,libmoosedb.a")
  ENDIF()
ENDIF()
