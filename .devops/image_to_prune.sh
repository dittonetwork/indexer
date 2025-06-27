#!/bin/bash
SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
. ${SCRIPT_DIR}/functions.sh
return_all_orphaned_images $1