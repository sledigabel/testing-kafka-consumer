#!/bin/sh

OPTIONS=""

if [ -n "${BOOTSTRAP}" ]
then
  OPTIONS="$OPTIONS -bootstrap ${BOOTSTRAP}"
fi

if [ -n "${GROUP}" ]
then
  OPTIONS="$OPTIONS -name ${GROUP}"
fi

if [ -n "${DEBUG}" ]
then
  OPTIONS="$OPTIONS -debug"
fi

if [ -n "${DURATION}" ]
then
  OPTIONS="$OPTIONS -duration ${DURATION}"
fi

if [ -n "${TOPIC}" ]
then
  OPTIONS="$OPTIONS -topic ${TOPIC}"
fi


echo /kafka-consumer ${OPTIONS}
/kafka-consumer ${OPTIONS}
