set (LIBRARY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../boost")

########################## filesystem ##########################

set (SRCS_FILESYSTEM
    "${LIBRARY_DIR}/libs/filesystem/src/codecvt_error_category.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/directory.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/exception.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/operations.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/path.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/path_traits.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/portability.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/unique_path.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/utf8_codecvt_facet.cpp"
    "${LIBRARY_DIR}/libs/filesystem/src/windows_file_codecvt.cpp"
)

add_library (_boost_filesystem ${SRCS_FILESYSTEM})
add_library (boost::filesystem ALIAS _boost_filesystem)
target_include_directories (_boost_filesystem SYSTEM BEFORE PUBLIC ${LIBRARY_DIR})

########################## includes-only ##########################

add_library (_boost_includes_only INTERFACE)
add_library (boost::includes_only ALIAS _boost_includes_only)
target_include_directories (_boost_includes_only SYSTEM BEFORE INTERFACE ${LIBRARY_DIR})

########################## asio ##########################

target_compile_definitions (_boost_includes_only INTERFACE
  BOOST_ASIO_STANDALONE=1
  # Avoid using of deprecated in c++ > 17 std::result_of
  BOOST_ASIO_HAS_STD_INVOKE_RESULT=1
)

########################## iostreams ##########################

set (SRCS_IOSTREAMS
    "${LIBRARY_DIR}/libs/iostreams/src/file_descriptor.cpp"
    "${LIBRARY_DIR}/libs/iostreams/src/gzip.cpp"
    "${LIBRARY_DIR}/libs/iostreams/src/mapped_file.cpp"
    "${LIBRARY_DIR}/libs/iostreams/src/zlib.cpp"
)

add_library (_boost_iostreams ${SRCS_IOSTREAMS})
add_library (boost::iostreams ALIAS _boost_iostreams)
target_include_directories (_boost_iostreams PRIVATE ${LIBRARY_DIR})

########################## program_options ##########################

set (SRCS_PROGRAM_OPTIONS
    "${LIBRARY_DIR}/libs/program_options/src/cmdline.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/config_file.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/convert.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/options_description.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/parsers.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/positional_options.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/split.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/utf8_codecvt_facet.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/value_semantic.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/variables_map.cpp"
    "${LIBRARY_DIR}/libs/program_options/src/winmain.cpp"
)

add_library (_boost_program_options ${SRCS_PROGRAM_OPTIONS})
add_library (boost::program_options ALIAS _boost_program_options)
target_include_directories (_boost_program_options SYSTEM BEFORE PUBLIC ${LIBRARY_DIR})

########################## regex ##########################

set (SRCS_REGEX
    "${LIBRARY_DIR}/libs/regex/src/posix_api.cpp"
    "${LIBRARY_DIR}/libs/regex/src/regex_debug.cpp"
    "${LIBRARY_DIR}/libs/regex/src/regex.cpp"
    "${LIBRARY_DIR}/libs/regex/src/static_mutex.cpp"
    "${LIBRARY_DIR}/libs/regex/src/wide_posix_api.cpp"
)

add_library (_boost_regex ${SRCS_REGEX})
add_library (boost::regex ALIAS _boost_regex)
target_include_directories (_boost_regex PRIVATE ${LIBRARY_DIR})

########################## system ##########################

set (SRCS_SYSTEM
    "${LIBRARY_DIR}/libs/system/src/error_code.cpp"
)

add_library (_boost_system ${SRCS_SYSTEM})
add_library (boost::system ALIAS _boost_system)
target_include_directories (_boost_system PRIVATE ${LIBRARY_DIR})

########################## graph ##########################

set (SRCS_GRAPH
    "${LIBRARY_DIR}/libs/graph/src/graphml.cpp"
    "${LIBRARY_DIR}/libs/graph/src/read_graphviz_new.cpp"
)

add_library (_boost_graph ${SRCS_GRAPH})
add_library (boost::graph ALIAS _boost_graph)
target_include_directories (_boost_graph PRIVATE ${LIBRARY_DIR})
target_link_libraries(_boost_graph PRIVATE _boost_regex)

########################## circular buffer ##########################

add_library(_boost_circular_buffer INTERFACE)
add_library(boost::circular_buffer ALIAS _boost_circular_buffer)
target_include_directories(_boost_circular_buffer SYSTEM BEFORE INTERFACE ${LIBRARY_DIR})

########################## heap ##########################

add_library(_boost_heap INTERFACE)
add_library(boost::heap ALIAS _boost_heap)
target_include_directories(_boost_heap SYSTEM BEFORE INTERFACE ${LIBRARY_DIR})
