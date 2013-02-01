# Hadoop Log Uploader

This script can be used to upload Hadoop logs to HDFS so they can be processed by the MapReduce
jobs.

## Dependencies

The script is written in Perl.  It has a dependency on `Date::Calc`.  There are at least 
a couple ways to install this.  On Redhat you could use `sudo yum install perl-date-calc`.  
Alternatively you could use the `cpan` tool.  After starting `cpan` enter `install Date::Calc` 
and agree to install all the dependencies.

## How it works

Log files are stored in HDFS by day under the `destination` path from the configuration file.
For example, given a `destination` such as `hdfs://mynamenode:9000/data/hadoop/history` and grid
name `mygrid`, it would store logs for the `default` queue for 1/31/2013 under:

    hdfs://mynamenode:9000/data/hadoop/history/mygrid/daily/default/2013/0131

The first thing the script does is check which log files already exist in HDFS.  Then it lists
files in the log directory and compares against what it found in HDFS.  It uploads any log files
which are missing.

## Running

The file `cfg.pm` contains a sample of the configuration.  Fill in the details based on your
environment.  To run it:

    ./statsupload.pl --config cfg.pm

