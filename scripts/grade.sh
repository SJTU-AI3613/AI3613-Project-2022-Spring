#!/bin/bash

WORKDIR=$(dirname $(dirname $(readlink -f $0)))

build() {
    make build > /dev/null
    if [[ $? != 0 ]]; then 
        echo "Error: Compiling failed."
        exit 1
    fi
}

grade_project1() {
    cd build
    local grade=0
    local failed_tests=$(ctest -R "executor" 2> /dev/null | grep -oE "[0-9]+ tests failed out of 10" | sed -E 's/([0-9]+) tests failed out of 10/\1/')
    grade=$((100 - ($failed_tests) * 10))
    echo "Project-1 Grade: $grade/100"
}

main() {
    if [[ $(pwd) != $WORKDIR ]]; then
        echo "Error: Please run this script in the project root directory."
        exit 1
    fi

    build
    grade_project1
}

main