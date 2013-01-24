#!/usr/bin/env bash

source environment.sh

export JRUBY_OPTS="--1.9 -J-Xms1G -J-Xmx4G"

CLASSPATH=lib/classes
export CLASSPATH

trinidad --rackup config.ru --config config/trinidad.yml