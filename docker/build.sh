#!/usr/bin/env sh

mkdir -p app
cp ../datalake_dsource.conf app/
cp ../datalake_repo.conf app/
cp ../run.sh app/
cp ../application/target/scala-2.11/datapipes-assembly-1.4.3.jar app/
cp ../run_xvv.sh app/
cp ./scire_datalake_preview.conf app/

docker build -t dockerregistry.actio.com.au/datapipes .

#docker push dockerregistry.actio.com.au/datapipes

rm -rf app




