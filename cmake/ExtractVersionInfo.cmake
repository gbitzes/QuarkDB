# Extract version info from Version.hh, so it's also available in cmake
macro(ExtractVariable VARIABLE_NAME)
  execute_process(
    COMMAND awk "/${VARIABLE_NAME}/ { printf \"%s\", $3 }" ${CMAKE_CURRENT_SOURCE_DIR}/src/Version.hh
    OUTPUT_VARIABLE TMP
  )
  set(${VARIABLE_NAME} ${TMP})
endmacro()

macro(ExtractVersionInfo)
  execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/genversion.py OUTPUT_QUIET)

  ExtractVariable("VERSION_MAJOR")
  ExtractVariable("VERSION_MINOR")
  ExtractVariable("VERSION_PATCH")
  ExtractVariable("VERSION_RELEASE")
  ExtractVariable("VERSION_FULL")
endmacro()
