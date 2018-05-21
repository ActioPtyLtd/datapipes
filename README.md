# Data Pipes by Actio&reg;

## What is Data Pipes?
Data Pipes is a lightweight cross-platform app specifically built to orchestrate the flow of data between systems on commodity hardware, which is becoming ever more vital in today’s modern data architecture. This includes the ability to interact with multiple data sources, such as collecting structured or unstructured data from systems that use a variety of data extraction procedures and protocols as well as loading data into disparate systems. Data Pipes captures additional activity metrics and metadata during execution to provide lineage and visibility over the flow of data. A common application of Data Pipes is to enable the flow of data from on-premise systems and/or cloud services to a data lake repository such as Actio’s FlowHUB. Data Pipes can then subsequently be used to cleanse, aggregate, transform and load the data into disparate systems to support the intended business use case.

## How does Data Pipes work?
Data Pipes is written in Scala and runs on the JVM, allowing for it to be deployed on Windows, Linux and macOS systems. [PipeScript&reg;](https://github.com/ActioPtyLtd/datapipes-pipescript) is a human readable DSL (domain specific language) that captures how to orchestrate the flow of data between systems. Data Pipes interprets and executes [PipeScript&reg;](https://github.com/ActioPtyLtd/datapipes-pipescript) which can be read from your local file system or retrieved via an API call to get up-to-date instructions.

## Command Line Interface
The command line options for Data Pipes are as follows:

```shell
$ datapipes -c <filename> [options]... [vmargs]...

```

options:
* **-p, --pipe**
    This is used to specify which pipe to execute in the configuration file. The default is the startup pipe specified in the configuration file.
* **-s, --service**
    The parameter will run Data Pipes as a long running service, listening on a predefined port specified in the configuration file.

vmargs:
* **-Dkey=val**
    Command-line arguments used to substitue values in the configuration file.
