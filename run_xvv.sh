#!/usr/bin/env sh
#test
#./mv_run.sh '/home/davidx/.actioswarm/tusd/nfsshare' 'http://localhost:3000/api'

#prod
#./mv_run.sh '/nfsshare' 'https://flowhub.actio.com.au/api'
#echo $#
workspace=$1
folder_tc=${workspace}/transfercomplete/
folder_p=${workspace}/processing/
apiurl=$2
pwd
process(){
    for file in `ls $folder_tc*.info`
    do
        if [ -f $file ]; then
            cmd="mv $file $folder_p"
            echo $cmd
            $cmd
            fullname=${file##*/}
            shortname="${fullname%.*}"
            file_info=$file
            file_bin=$folder_tc${shortname}.bin
            cmd='rm load_datalake.conf'
            echo $cmd
            $cmd
            cmd="sh run.sh -c datalake_dsource.conf -DfileFolder=$folder_p -DfileName=$fullname -Dflowhub_datasource.baseuri=$apiurl"
            echo $cmd
            $cmd
            cat datalake_repo.conf >> load_datalake.conf
            cmd="sh run.sh -c load_datalake.conf -DfilePath=$file_bin -DextractPath=/tmp"
            echo $cmd
            $cmd
        fi
    done
}

while [ 1 ];
    do
       iter=$(($iter+1))
       echo $iter
       process
       sleep 10
    done


