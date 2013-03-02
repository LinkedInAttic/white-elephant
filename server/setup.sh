#!/usr/bin/env bash

# Copyright 2012 LinkedIn, Inc

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

txtgrn=$(tput setaf 2)    # Green
txtred=$(tput setaf 1)    # Red
txtyel=$(tput setaf 3)    # Yellow
txtrst=$(tput sgr0)       # Text reset
txtund=$(tput sgr 0 1)    # Underline
v="${txtgrn}[v]${txtrst}" # success mark
x="${txtred}[x]${txtrst}" # fail mark

base_dir=$(dirname $0)

JRUBY_VERSION=1.7.3
SETUP_SUCCESS=.setup-success

#
# Prints a string with a given color
# $1 message to print
# $2 font settings
#
function colorOut() {
  msg=$1
  clr=$2
  echo "${clr}$msg${txtrst}"
}

#
# Checks for errors in a previous command
# $1 message to print in case of an error
# $2 message to print in case of success
#
function checkErrors() {
  errCode=$?
  errMsg=$1
  successMsg=$2
  if [ "$errCode" -ne "0" ]; 
  then
    echo "$x $errMsg"
    echo "script will exit"
    exit $errCode
  else
    echo "$v $successMsg"
  fi
}

#
# Runs a given command, outputs command description and checks result
# $1 command to run
# $2 description
# $3 error message
#
function runCommand() {
  comm=$1
  descr=$2
  errMsg=$3

  colorOut "$descr" $txtyel
  $comm
  checkErrors "$errMsg" "done"
}

function cloneRbEnv() {
  tmp_dir=`mktemp -d tmp.XXXXXXXX`
  checkErrors "Failed to create temp directory" "Created temporary directory"
  command -v git >/dev/null 2>&1
  checkErrors "Failed to find git" "Found git"
  rbenv_dir=$base_dir/.rbenv
  git clone -q git://github.com/sstephenson/rbenv.git $tmp_dir && mv $tmp_dir $rbenv_dir
}

function checkRbEnv() {
  if [ ! -d "$base_dir/.rbenv" ];
  then
    colorOut "rbenv is not installed" $txtyel

    runCommand \
      "cloneRbEnv"\
      "Installing rbenv"\
      "Failed to install rbenv"

    colorOut "rbenv installed to $base_dir/.rbenv" $txtgrn

    source environment.sh
  else
    source environment.sh
    return 0
  fi
}

function installJRuby() {
  jruby_dir=$base_dir/.rbenv/versions/jruby-$JRUBY_VERSION
  tmp_dir=`mktemp -d tmp.XXXXXXXX`
  checkErrors "Failed to create temp directory" "Created temporary directory"
  mkdir -p $base_dir/.rbenv/versions  
  command -v wget >/dev/null 2>&1
  checkErrors "Failed to find wget" "Found wget"
  wget -nv -O jruby-bin-$JRUBY_VERSION.zip http://jruby.org.s3.amazonaws.com/downloads/$JRUBY_VERSION/jruby-bin-$JRUBY_VERSION.zip && unzip -q jruby-bin-$JRUBY_VERSION.zip -d $tmp_dir && rm jruby-bin-$JRUBY_VERSION.zip && mv $tmp_dir/jruby-$JRUBY_VERSION $jruby_dir
}

function checkJRuby() {
  if [ ! -d "$base_dir/.rbenv/versions/jruby-$JRUBY_VERSION" ];
  then
    colorOut "JRuby $JRUBY_VERSION is not installed" $txtyel

    runCommand \
      "installJRuby"\
      "Installing JRuby $JRUBY_VERSION"\
      "Failed to install JRuby $JRUBY_VERSION"

    rbenv rehash

    colorOut "Successfully installed JRuby $JRUBY_VERSION" $txtgrn
  fi
}

function useJRubyLocally() {
  rbenv local jruby-$JRUBY_VERSION
}

function checkForRvm() {
  if [ -d "$HOME/.rvm" ];
  then
    colorOut "Found $HOME/.rvm" $txtred
    return 1
  fi
}

function installBundlerGem() {
  gem install bundler
}

function checkBundler() {
  bundler=`gem list bundler | grep bundler`
  if [ "$bundler" != "" ];
  then
    colorOut "Found bundler" $txtgrn
  else
    colorOut "bundler not installed" $txtyel

    runCommand \
      "installBundlerGem"\
      "Installing bundler"\
      "Failed to install bundler"

    rbenv rehash

    colorOut "Successfully installed bundler" $txtgrn
  fi
}

function bundleInstall() {
  bundle install
}

function checkGems() {
  bundler=`bundle check | grep "dependencies are satisfied"`
  if [ "$bundler" != "" ];
  then
    colorOut "Gems are already installed" $txtgrn
  else
    runCommand \
      "bundleInstall"\
      "Installing missing gems"\
      "Failed to install missing gems"

    rbenv rehash

    colorOut "Successfully installed gems" $txtgrn
  fi
}

function antBuild() {
  ant build
}

function setupSuccess() {
  touch .setup-success
}

if [ -f "$SETUP_SUCCESS" ]; then
  colorOut "Environment already set up!" $txtgrn
  exit 0
fi

runCommand \
  "checkForRvm"\
  "Making sure RVM is not installed"\
  "rbenv is incompatible with RVM, if you wish to remove rvm then run 'rvm implode'"

runCommand \
  "checkRbEnv"\
  "Checking for rbenv"\
  "Failed to install rbenv"

runCommand \
  "checkJRuby"\
  "Checking for JRuby"\
  "Failed to install JRuby"

runCommand \
  "useJRubyLocally"\
  "Configuring local directory to use JRuby $JRUBY_VERSION"\
  "Failed to configure to use JRuby $JRUBY_VERSION"

runCommand \
  "checkBundler"\
  "Checking for bundler"\
  "Failed to install bundler"

runCommand \
  "checkGems"\
  "Checking if gems are installed"\
  "Failed to install gems"

runCommand \
  "setupSuccess"\
  "Recording success"\
  "Failed to record success"

colorOut "Finished!" $txtgrn