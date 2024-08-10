#!/bin/bash

SCRIPT_DIR_NAME=$(dirname "$0")
TEST_TARGET_NAME=$1

echo "Test target name $TEST_TARGET_NAME"

function test_configuration # CONFIGURATION_CMAKE_FLAGS
{
    CONFIGURATION_CMAKE_FLAGS=$1
    echo "Test configuration $CONFIGURATION_CMAKE_FLAGS"

    if (mkdir -p $SCRIPT_DIR_NAME/build_test && rm -rf $SCRIPT_DIR_NAME/build_test/* && \
        cd $SCRIPT_DIR_NAME/build_test && cmake .. $CONFIGURATION_CMAKE_FLAGS && make -j8 $TEST_TARGET_NAME && tests/$TEST_TARGET_NAME)
    then
        echo "Tests passed for configuration $CONFIGURATION_CMAKE_FLAGS"
    else
        echo "Tests failed for configuration $CONFIGURATION_CMAKE_FLAGS"
        exit 1
    fi
}

test_configuration "-DCMAKE_BUILD_TYPE=Debug"
test_configuration "-DCMAKE_BUILD_TYPE=Debug -DSANITIZE=address"
test_configuration "-DCMAKE_BUILD_TYPE=RelWithDebInfo"
test_configuration "-DCMAKE_BUILD_TYPE=RelWithDebInfo -DSANITIZE=address"
