#!/bin/bash

basedir=`dirname $0`
redis-cli SCRIPT LOAD "`cat ${basedir}/../sketches/sketches/minhash_set_update.lua`"
