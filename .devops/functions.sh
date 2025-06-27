#!/bin/bash
SCRIPT_DIR="$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"

function return_new_count_for_branch() {
	local BRANCH="$1"
	local SERVICE="$2"
	local MAX_COUNT=0
	CURRENT_COUNT=0
	CURRENT_SERVICE=`aws ecr describe-images --filter tagStatus=TAGGED --repository-name epsilon/$SERVICE |jq -c '.imageDetails[] | select( .imageTags[] | contains("'${BRANCH}'_") )'|jq ".imageTags[]"`
	for i in ${CURRENT_SERVICE}
		do
			CURRENT_COUNT=$((CURRENT_COUNT + 1))
	done
		if [ $CURRENT_COUNT -gt $MAX_COUNT ]; then
	  		MAX_COUNT=$CURRENT_COUNT
		fi
	echo $MAX_COUNT
}

function return_all_deployable_branch() {
	local BRANCH=""	
	local LIST_SERVICE="$(get_list_of_services)"
	for CURRENT_SERVICE in $LIST_SERVICE
	do
		BRANCH+=`aws ecr describe-images --filter tagStatus=TAGGED --repository-name epsilon/$CURRENT_SERVICE |jq -c '.imageDetails[] | select( .imageTags[] | match("_[0-9]") )'|jq ".imageTags[]"|sed 's|\"||g'|cut -d'_' -f1|uniq`$'\n'
	done
	echo "$BRANCH"|grep -v ^$|sort|uniq
}

function return_all_service_for_current_version() {	
	local LIST_SERVICE="$(get_list_of_services)"
	local SERVICES=""
	for CURRENT_SERVICE in $LIST_SERVICE
	do
	    CHECK_SERVICE=`aws ecr describe-images --filter tagStatus=TAGGED --repository-name epsilon/$CURRENT_SERVICE |jq -c '.imageDetails[] | select( .imageTags[] == "'$1"_"$2'" )'`
	    if [ x"$CHECK_SERVICE" != x ]
		then
			SERVICES+="$CURRENT_SERVICE"" "
		fi		
	done
	echo "$SERVICES"
}

function return_all_orphaned_images() {	
	 local ALL_REMOTE_BRANCH=`git branch -r|awk '{print $1}'|sed 's|origin/||'`
	 local ALL_DEPLOYABLE_BRANCH=`return_all_deployable_branch`
	 local IMAGE_TO_PRUNE=``
	 for BRANCH in $ALL_DEPLOYABLE_BRANCH
	 do
		EXIST=`echo "$ALL_REMOTE_BRANCH"|grep -w $BRANCH`
		if [ x"$EXIST" == x ]
		then
			IMAGE_TO_PRUNE+="$BRANCH"" "
		fi
	 done
	 echo "$IMAGE_TO_PRUNE"
}