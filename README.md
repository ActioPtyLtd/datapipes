# Data Pipes by Actio&reg;

## What is Data Pipes?
Data Pipes is a lightweight cross-platform app specifically built to orchestrate the flow of data between systems on commodity hardware, which is becoming ever more vital in today’s modern data architecture. This includes the ability to interact with multiple data sources, such as collecting structured or unstructured data from systems that use a variety of data extraction procedures and protocols as well as loading data into disparate systems. Data Pipes captures additional activity metrics and metadata during execution to provide lineage and visibility over the flow of data. A common application of Data Pipes is to enable the flow of data from on-premise systems and/or cloud services to a data lake repository such as Actio’s FlowHUB. Data Pipes can then subsequently be used to cleanse, aggregate, transform and load the data into disparate systems to support the intended business use case.

## Note on versions
Data Pipes version 2 is not backwards compatible with Version 1.  This was due to the introduction of some significant concepts in Version 2. 

## How does Data Pipes work?
Data Pipes is written in Scala and runs on the JVM, allowing for it to be deployed on Windows, Linux and macOS systems. [PipeScript&reg;](https://github.com/ActioPtyLtd/datapipes-pipescript) is a human readable DSL (domain specific language) that captures how to orchestrate the flow of data between systems. Data Pipes interprets and executes [PipeScript&reg;](https://github.com/ActioPtyLtd/datapipes-pipescript) which can be read from your local file system or retrieved via an API call to get up-to-date instructions.

## Build Data Pipes
To build Data Pipes you will need sbt. Run the following command:

```shell
$ sbt assembly
```

This will generate a fat jar.

## Command Line Interface
To run Data Pipes, either build it yourself, or simply download [here](https://github.com/ActioPtyLtd/datapipes/releases) and execute it with the following command:

```
$ java [vmargs] -jar datapipes-assembly.jar -c <filename> [options]...
```

options:
* **-p, --pipe**
    This is used to specify which pipe to execute in the configuration file. The default is the startup pipe specified in the configuration file.
* **-s, --service**
    The parameter will run Data Pipes as a long running service, listening on a predefined port specified in the configuration file.

vmargs:
* **-Dkey=val**
    Command-line arguments used to substitue values in the configuration file.


## Hello World
This example is a simple Hello World introduction to Data Pipes. Ensure you have downloaded the latest release and example file (rest_to_csv.conf) included in this repo.

```
$ java -jar datapipes-assembly.jar -c ./examples/rest_to_csv.conf
```

This will generate the following output on the console:
```
[main] INFO  AppConsole - ./examples/rest_to_csv.conf
[main] INFO  AppConsole - run.id=ea8e2d50-ff88-4685-9d03-db652735316b
[main] INFO  AppConsole - Running pipe: rest_to_csv
[main] INFO  RESTJsonDataSource - Calling GET https://randomuser.me/api?results=100
[main] INFO  RESTJsonDataSource - Response size: 103032
[main] INFO  RESTJsonDataSource - Response body:
[main] INFO  RESTJsonDataSource - {"results":[{"gender":"male","name":{"title":"mr","first":"arno","last":"krah"},"location":{"street"...
[main] INFO  RESTJsonDataSource - Status code 200 returned.
[main] INFO  TaskExtract - Sending remaining buffered data...
[main] INFO  LocalFileSystemDataSource - Writing to file: ./examples/users-female.csv...
[main] INFO  LocalFileSystemDataSource - Completed writing to file: ./examples/users-female.csv.
[main] INFO  LocalFileSystemDataSource - Writing to file: ./examples/users-male.csv...
[main] INFO  LocalFileSystemDataSource - Completed writing to file: ./examples/users-male.csv.
[main] INFO  SimpleExecutor - === Operation get_each_user completed ===
[main] INFO  SimpleExecutor - === Operation save_to_csv completed ===
[main] INFO  AppConsole - Pipe rest_to_csv completed successfully.
```

Congratulations! You have run your first script using Data Pipes. 

As suggested by the logs, you should see two csv files generated. These files have been populated with sample data from a public Restful service. Some of the data has been transformed to demonstrate some of the expression capability. Feel free to make changes to the script and remember to have fun!
