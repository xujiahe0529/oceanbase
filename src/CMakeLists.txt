add_library(ob_base INTERFACE)
target_include_directories(
  ob_base INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(ob_base INTERFACE cxx_std_11)
target_link_libraries(ob_base INTERFACE oblib_base objit_base
    -Wl,-znorelro -Wl,-T,${CMAKE_SOURCE_DIR}/rpm/ld.lds)
target_compile_definitions(ob_base INTERFACE
  PACKAGE_NAME="${PROJECT_NAME}"
  PACKAGE_VERSION="${PROJECT_VERSION}"
  PACKAGE_STRING="${PROJECT_NAME} ${PROJECT_VERSION}"
  # change to real release ID.
  RELEASEID="${BUILD_NUMBER}"
  )
target_compile_options(ob_base INTERFACE
  $<$<COMPILE_LANGUAGE:CXX>:-Wno-invalid-offsetof>)

add_library(python_lib INTERFACE)
target_include_directories(
  python_lib INTERFACE
  "/usr/local/include/python3.11m"
  "/usr/local/python311/lib/python3.11/site-packages/numpy/core/include/")
target_link_libraries(python_lib INTERFACE
  -L/usr/local/python311/lib -lpython3.11m -lpthread -ldl  -lutil -lm  -Xlinker -export-dynamic)

function(ob_server_add_target target)
  message(STATUS "observer_add_target ${target}")
  ob_add_object_target(${target})
  target_link_libraries(${target} PUBLIC ob_base)
  list(APPEND ob_objects ${target})
  set(ob_objects "${ob_objects}" CACHE INTERNAL "observer object library list")
endfunction()

add_subdirectory(share)
add_subdirectory(sql)
add_subdirectory(pl)
add_subdirectory(rootserver)
add_subdirectory(logservice)
add_subdirectory(storage)
add_subdirectory(diagnose)
add_subdirectory(observer)
add_subdirectory(libtable)
