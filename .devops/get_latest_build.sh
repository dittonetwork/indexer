#!/bin/bash
SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
. ${SCRIPT_DIR}/functions.sh
if [ -z "$1" ]
then
    echo "No branch input. Usage:  ${BASH_SOURCE[0]} branch"
    exit 1
fi
return_new_count_for_branch $1 $2