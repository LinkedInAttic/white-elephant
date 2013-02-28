#!/usr/bin/env bash

source environment.sh

export JRUBY_OPTS="--1.9 -J-Xms1G -J-Xmx4G"

CLASSPATH=lib/classes
export CLASSPATH

cp local_config.yml config.yml

trinidad --rackup config.ru --config config/trinidad.yml