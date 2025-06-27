#!/bin/bash
SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
. ${SCRIPT_DIR}/functions.sh
if [ -z "$1" ]
then
    echo "No branch input. Usage:  ${BASH_SOURCE[0]} branch version"
    exit 2
fi
if [ -z "$2" ]
then
    echo "No version input. Usage:  ${BASH_SOURCE[0]} branch version"
    exit 3
fi

return_all_service_for_current_version $1 $2