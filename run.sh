#!/bin/bash
JARFILE=$(find ./application/target/scala-2.11/ -name 'datapipes-assembly*.jar')

config_name="./application.conf"
pipe_name=""
vmargs=""
service=""

USAGE="Usage: $0 [-c configFile] [-p pipeName] -Dkey1=val1 -Dkey2=val2 ..."

while [ "$1" != "" ]; do
    case $1 in
        -c | --config )         shift
                                config_name="-c $1"
                                ;;
        -p | --pipe )           shift
                                pipe_name="-p $1"
                                ;;
        -s | --service )        shift
                                pipe_name="-s"
                                ;;
        -R | --Read )           shift
                                pipe_name="-R"
                                ;;
        -h | --help )           echo $USAGE
                                exit 1
                                ;;
        *)                      vmargs="$vmargs $1"
                                ;;
    esac
    shift
done

echo "Running Java with VM Arguments: $vmargs"

java $vmargs -cp $JARFILE actio.datapipes.application.AppConsole $config_name $pipe_name $service