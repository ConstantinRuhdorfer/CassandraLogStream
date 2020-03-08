# Welcome

This project aims to introduce the use of cassandra an display a cassandra use case.

## How set this project up

This project assumes Scala 2.11.X and Cassandra 3.11.
We currently do not support other versions even tough they might work.

### I have no experience with all of this

We recommend to install the newest version of Scala 2.11.X.
After that install IntelliJ IDEA - CE with the Scala plugin.
Lastly use IntelliJ to import this project via its github URL.

This project is a maven project.
Please important all requirements via its pom.xml.

You will also need to have Cassandra running locally.
For that install Cassandra 3.11 via its binaries/a package manager (e.g. `brew install cassandra` on mac).
Start Cassandra with `cassandra -f`. 
You can check for a working cassandra instance by connecting to cassandra using `clqsh`.

## How run this project

You will need to start two programs: LogProducer for producing fake logs and StreamingJob to process them.

## Source code structure

The most important source code files are listed in the following tables:

```
.
+-- scala/                         Entry point
|   +-- config/                    Global project configurations 
|   +-- domain/                    Some types for table
|   +-- domainTypes/               Various project types
|   +-- log/
|       +-- LogProducer.scala      Generates a range of fake logs and stores them
|   +-- query/
|       +-- QueryJob.scala         Executes some "statstical" queries
|   +-- streaming/
|       +-- StreamingJob.scala     Reads the file streams to query job/cassandra
|   +-- utils/                     Helpers for managing Spark/Cassandra
```

## Project architecture

The projects architecture/application overview is presented below:

![Project architecture](reamde_assets/LogStreamCassandraOverview.png)

## Cassandra

This project assumes a running cassandra instance under localhost and available under the port 9042.
If this is not the case please update the projects settings.
This project is going to setup all necessary tables itself.

### Cassandra table setup

The application creates its tables automatically.
The tables created are created for the use case presented below:

![Cassandra Query](reamde_assets/ApplicationFlow.png)

This results in the following table schema:

![Query tables](reamde_assets/QueryTables.png)

## Spark

Spark is run by this program per default on all available threads.