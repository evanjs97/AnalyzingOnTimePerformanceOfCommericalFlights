#!/usr/bin/env bash

HOME="$( cd "$( dirname "$0" )" && pwd )"
JOB=$1
USE_SHARED=$2

HHOME="/usr/local/hadoop/bin/hadoop"
SHARED_COMMAND="HADOOP_CONF_DIR=/s/bach/g/under/evanjs/Documents/Senior/CS555/client-config"
INPUT_MAIN="/data/main/"
INPUT_SUPPLEMENTARY="/data/supplementary"
OUTPUT="/transport/${JOB}/"

if [[ $USE_SHARED = "false" ]]
	then
	INPUT_MAIN="/transport${INPUT_MAIN}"
	INPUT_SUPPLEMENTARY="/transport${INPUT_SUPPLEMENTARY}"
elif [[ $USE_SHARED = "true" ]]
	then
	OUTPUT="/home${OUTPUT}"
	export $SHARED_COMMAND
fi

CARRIERS="${INPUT_SUPPLEMENTARY}/carriers.csv"
AIRPORTS="${INPUT_SUPPLEMENTARY}/airports.csv"

CLEAN_COMMAND="${HHOME} fs -rm -r ${OUTPUT}"
COMMAND="$HHOME jar ${HOME}/build/libs/hadoop-1.0.jar Entry ${JOB} ${INPUT_MAIN} ${AIRPORTS} ${CARRIERS} ${OUTPUT}"

FINAL_COMMAND="${CLEAN_COMMAND} || true && ${COMMAND}"
eval $FINAL_COMMAND