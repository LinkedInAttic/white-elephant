# White Elephant

White Elephant is a Hadoop log aggregator and dashboard which enables 
visualization of Hadoop cluster utilization across users.

![Screenshot](https://s3.amazonaws.com/snaprojects/blog/whitel/whitel_ex2.png)

## Quick Start

To try out the server with some test data:

    cd server
    ant
    ./startup.sh

Then visit [http://localhost:3000](http://localhost:3000).  It may take a minute for the test data
to load.

## Hadoop Version Compatibility

White Elephant is compiled and tested against Hadoop 1.0.3 and should work with any 1.0.x version.
Hadoop 2.0 is not yet supported.

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

    ./startup.sh

You can now visit [http://localhost:3000](http://localhost:3000).  It may take a minute for the test data
to load.

This uses [trinidad](https://github.com/trinidad/trinidad) to run the JRuby web app in development mode.
Since it is in development mode the app assumes local data should be used,
which it looks for in the directory specified in `config.yml`.

### Configuration

The server configuration is contained in `config.yml`.  You can see a sample in `sample_config.yml`.

When run in development mode using `./startup.sh`, `sample_config.yml` is used and it follows the
settings specified under `local`.  The only configurable parameter here is `file_pattern`, which specifies 
where to load the usage data from on local disk.

When packaged as a WAR it runs in production mode and uses configuration specified under `hadoop`, 
the assumption being that the aggregated usage data will be available there.  The following 
parameter must be specified:

* **file_pattern**: Glob pattern to load usage files from Hadoop
* **libs**: Directories containing Hadoop JARs (to be added to the classpath)
* **conf_dir**: Directory containing Hadoop configuration (to be added to the classpath)
* **principal**: User name used to access secure Hadoop
* **keytab**: Path to keytab file for user to access secure Hadoop

White Elephant does not assume a specific version of Hadoop, so the JARs are not packaged in the WAR.
Therefore the path to the Hadoop JARs must be specified in the configuration.

### Deploying

To build a WAR which can be deployed to tomcat:

    ant war -Dconfig.path=<path-to-config>

The config file you specify will be packaged as `config.yml` within the WAR.  See `sample_config.yml`
as an example for how to write the config file.

## Hadoop Log Uploading

The script `hadoop/scripts/statsupload.pl` can be used to upload the Hadoop logs to HDFS
so they can be processed.  Check its documentation for details.

## Hadoop Jobs

There are three Hadoop jobs, all managed by a job executor which keeps track of what
work needs to be done.

The first two jobs parse and convert raw job configurations and logging into an easier-to-work-with Avro
format. Together, these two datasets can serve as the base data for a variety of usage analytics workflows.

The third and final job reads the Avro-fied log data and aggregates it per hour, writing the data
out in Avro format.  It essentially builds a data cube which can be easily loaded by the
web application into the DB and queried against.

### Configuration

Some sample configuration files can be found under `hadoop/config/jobs`:

* **base.properties**: Contains most of the configuration
* **white-elephant-full-usage.job**: Job file used when processing all logs.
* **white-elephant-incremental-usage.job**: Job file used when incrementally processing logs.

The `base.properties` file consists of configuration specific to White Elephant and configuration
specifically for Hadoop.  All Hadoop configuration parameter begin with `hadoop-conf`.  The two
job files just have a single settings `incremental` and only differ in the value they use for it.

### Hadoop Logs

Within `base.properties` is a parameter `logs.root`.  This is the root path where the Hadoop logs
are found which are to be parsed.  The parsing job assumes the logs are stored in Hadoop under daily
directories using the following directory structure:

    <logs.root>/<cluster-name>/daily/<yyyy>/<MMdd>

For example, logs on January 23rd, 2013 for the production cluster may be stored in a directory
such as:

    /data/hadoop/logs/prod/daily/2013/0123

### Packaging

To create a zip package containing all files necessary to run the jobs simply run:

    ant zip -Djob.config.dir=<path-to-job-config-dir>

The `job.config.dir` should be the directory containing the `.properties` and `.job` files you would
like to include in the package.

If you happen to be using [Azkaban](http://data.linkedin.com/opensource/azkaban) as your job scheduler
of choice then this zip file will work with it as long as you add the Azkaban specific configuration 
to `base.properties`.

### Running

After unzipping the zip package you can run using the `run.sh` script.  This requires a couple environment 
variables to be set:

* **HADOOP_CONF_DIR**: Hadoop configuration directory
* **HADOOP_LIB_DIR**: Hadoop JARs directory

To run the full job:

    ./run.sh white-elephant-full-usage.job

To run the incremental job:

    ./run.sh white-elephant-incremental-usage.job

The incremental job is more efficient as it only processes new data.  The full job reprocesses
*everything*.

## Contributing

White Elephant is open source and freely available under the Apache 2 license.  As always, we
welcome contributors, so send us your pull requests.

For help please see the [discussion group](http://groups.google.com/group/linkedin-white-elephant).

## Thanks

White Elephant is built using a suite of great open source projects.  Just to name a few:

* [JRuby](http://jruby.org/)
* [HyperSQL](http://hsqldb.org/)
* [Bootstrap](http://twitter.github.com/bootstrap/)
* [Rickshaw](http://code.shutterstock.com/rickshaw/)
* [Ember.js](http://emberjs.com/)
* [D3](http://d3js.org/)
* [Moment.js](http://momentjs.com/)
* [jQuery](http://jquery.com/)
* [jQuery UI](http://jqueryui.com/)

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
