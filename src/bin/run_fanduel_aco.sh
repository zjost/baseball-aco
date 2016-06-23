#!/usr/bin/env bash

export BASEBALL_HOME=$HOME/git/baseball
export DATE_STR=$1

cd $BASEBALL_HOME
python $BASEBALL_HOME/src/workflows/fanduelTest.py $DATE_STR 
