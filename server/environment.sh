#!/usr/bin/env bash

base_dir=`pwd`

RBENV_ROOT=$base_dir/.rbenv
export RBENV_ROOT

export PATH="$RBENV_ROOT/bin:$PATH"
eval "$(rbenv init -)"

rbenv rehash