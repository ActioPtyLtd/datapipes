FROM java:8-jre-alpine

#docker build -t dockerregistry.actio.com.au/fileprocessor .

RUN mkdir -p /app/application/target/scala-2.11/
ADD app/datapipes-assembly-1.4.3.jar /app/application/target/scala-2.11/


ADD app/run.sh /app
ADD app/datalake_dsource.conf /app
ADD app/datalake_repo.conf /app
ADD app/run_xvv.sh /app
ADD app/scire_datalake_preview.conf /app

WORKDIR /app

CMD ["sh", "run_xvv.sh", "/nfsshare", "http://flowhubbackend:9988/api"]