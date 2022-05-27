#!/bin/bash

project=$1
WORKDIR=$(dirname $(dirname $(readlink -f $0)))

build() {
    make build > /dev/null
    if [[ $? != 0 ]]; then 
        echo "Error: Compiling failed."
        exit 1
    fi
}

grade_project1() {
    local grade=0
    local failed_tests=$(ctest -R "executor" 2> /dev/null | grep -oE "[0-9]+ tests failed out of 10" | sed -E 's/([0-9]+) tests failed out of 10/\1/')
    grade=$((100 - ($failed_tests) * 10))
    echo "Project-1 Grade: $grade/100"
}

grade_project2() {
    local grade=0
    local failed_tests=$(ctest -R "lock|transaction" 2> /dev/null | grep -oE "[0-9]+ tests failed out of 15" | sed -E 's/([0-9]+) tests failed out of 15/\1/')
    grade=$((150 - ($failed_tests) * 10))
    echo "Project-2 Grade: $grade/150"
}

main() {
    if [[ $(pwd) != $WORKDIR ]]; then
        echo "Error: Please run this script in the project root directory."
        exit 1
    fi

    build
    cd build
    if [[ $project = "project1" ]]; then
        grade_project1
    elif [[ $project = "project2" ]]; then
        grade_project2
    else
        echo "Error: Invalid argument."
    fi
}
main