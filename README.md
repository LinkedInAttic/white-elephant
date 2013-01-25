# White Elephant

White Elephant is a Hadoop log aggregator and dashboard which enables 
visualization of Hadoop cluster utilization across users.

## Quick Start

To try out the server with some test data:

    cd server
    ant
    ./startup

Then visit [http://localhost:3000](http://localhost:3000).

## Server

The server is a JRuby web application.  In a production environment it can be deployed to tomcat
and reads aggregated usage data directly from Hadoop.  This data is stored in an in-memory database
provided by [HyperSQL](http://hsqldb.org/).  Charting is provided by 
[Rickshaw](http://code.shutterstock.com/rickshaw/).

### Getting started

To get started using the server, first set up the environment:

    cd server
    ant

The default target does several things, among them:

* Installs JRuby to a local directory under .rbenv
* Installs Ruby gems to the above directory
* Downloads JARs
* Creates test data under data/usage

At this point you should be able to start the server:

    ./startup

You can now visit [http://localhost:3000](http://localhost:3000).

This uses trinidad to run the JRuby web app in development mode.
Since it is in development mode the app assumes local data should be used,
which it looks for in the directory specified in `config.yml`.

### Configuration

The server configuration is contained in `config.yml`.  

When run in development mode using `./startup.sh` it uses configuration specified under `local`.  
The only configurable parameter here is `file_pattern`, which specifies where to load the usage
data from on local disk.

When packaged as a WAR it runs in production mode and uses configuration specified under `hadoop`, 
the assumption being that the aggregated usage data will be available there.  The following 
parameter must be specified:

* file_pattern: Glob pattern to load usage files from Hadoop
* libs: Directories containing Hadoop JARs (to be added to the classpath)
* conf_dir: Directory containing Hadoop configuration (to be added to the classpath)
* principal: User name used to access secure Hadoop
* keytab: Path to keytab file for user to access secure Hadoop

White elephant does not assume a specific version of Hadoop, so the JARs are not packaged in the WAR.
Therefore the path to the Hadoop JARs must be specified in the configuration.

### Deploying

To build a WAR which can be deployed to tomcat:

    ant war

The config.yml file will be packaged with the WAR.

## Hadoop Jobs

There are two Hadoop jobs, both managed by a job executor which keeps track of what
work needs to be done.

The first job is a Hadoop log parser.  It reads log files stored in Hadoop, parses out
relevant information, and writes the data out in an easier-to-work-with Avro format.

The second job reads the Avro-fied log data and aggregates it per-hour, writing the data
out in Avro format.  It essentially builds a data cube which can be easily loaded by the
web application and queries against.

### Configuration

The configuration files are located under `hadoop/config/jobs`:

* base.properties: Contains most of the configuration
* white-elephant-full-usage.job: Job file used when processing all logs.
* white-elephant-incremental-usage.job: Job file used when incrementally processing logs.

The `base.properties` file consists of configuration specific to White Elephant and configuration
specifically for Hadoop.  All Hadoop configuration parameter begin with `hadoop-conf`.

### Packaging

To create a zip package containing all files necessary to run the jobs simply run:

    ant zip

### Running

After unzipping the zip package you can run using `run.sh`.

To run the full job:

    ./run.sh white-elephant-full-usage.job

To run the incremental job:

    ./run.sh white-elephant-incremental-usage.job

The `run.sh` script requires a couple environment variables:

* HADOOP_CONF_DIR: Hadoop configuration directory
* HADOOP_LIB_DIR: Hadoop JARs directory

## License

Copyright 2012 LinkedIn, Inc

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.