cmake_minimum_required(VERSION 2.6)



#-------------------------------------------------------------------------------
# Search for dependencies
#-------------------------------------------------------------------------------
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package(XRootD REQUIRED)



#-------------------------------------------------------------------------------
# Compiler options
#-------------------------------------------------------------------------------

add_definitions(-Wall -Wextra -Werror -Wno-unused-parameter -std=c++11)

#-------------------------------------------------------------------------------
# Build source and tests
#-------------------------------------------------------------------------------

add_subdirectory(test)
add_subdirectory(src)
