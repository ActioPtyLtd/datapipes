#!/bin/sh

rm load_datalake.conf
./run.sh -c datalake_dsource.conf -DfileFolder=$1 -DfileName=$2
cat datalake_repo.conf >> load_datalake.conf
./run.sh -c load_datalake.conf -DfilePath=$3 -DextractPath=$4
