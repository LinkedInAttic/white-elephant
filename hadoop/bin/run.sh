#!/usr/bin/env bash

# Environment Variables
#
#   HADOOP_CONF_DIR   Hadoop configuration
# 
#   HADOOP_LIB_DIR    Hadoop JARs directory
#
#   JAVA_OPTS         (Optional) Java runtime options

home_dir=$(dirname $0)

if [ -z "$HADOOP_CONF_DIR" ]; then
  echo "Must set HADOOP_CONF_DIR"
  exit 1
fi

if [ -z "$HADOOP_LIB_DIR" ]; then
  echo "Must set HADOOP_LIB_DIR"
  exit 1
fi

if [ ! -d "$HADOOP_CONF_DIR" ]; then
  echo "Directory $HADOOP_CONF_DIR not found"
  exit 1
fi

if [ ! -d "$HADOOP_LIB_DIR" ]; then
  echo "Directory $HADOOP_LIB_DIR not found"
  exit 1
fi

if [ $# -eq 1 ]; then
  JOB_FILE=$1
else
  echo "Usage: run.sh <job-file>"
  exit 1
fi

if [ ! -f $JOB_FILE ]; then
  echo "File $JOB_FILE not found"
  exit 1
fi

CLASSPATH=$HADOOP_CONF_DIR

for jar in $(find -H $HADOOP_LIB_DIR -name '*.jar');
do
  CLASSPATH=$CLASSPATH:$jar
done;

for jar in $(find $home_dir -name '*.jar');
do
  CLASSPATH=$CLASSPATH:$jar
done;

export CLASSPATH

java $JAVA_OPTS com.linkedin.whiteelephant.ProcessLogs $JOB_FILE