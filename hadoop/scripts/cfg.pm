# This is the config file for each grid.

package cfg;

our %CFG = (
    # Directory for the Hadoop binaries.  Expectation is that "bin/hadoop"
    # exists under this directory.
    "hadoop_home" => "/path/to/hadoop",
    
    # Directory for the Hadoop logs.  Expectation is that this
    # has a "history" subfolder.
    "hadoop_logs" => "/path/to/hadoop/logs",

    # How many days of log files to look for in HDFS.  This is used
    # to determine which logs must be uploaded.
    "days"        => 25,

    # Which queues to upload logs for.  The script checks the job conf xml
    # to determine which queue the job was run in.
    "queues"      => ['default'],

    # Which Hadoop grid is this?  This will be used to build the path name
    # in HDFS.  This is useful when you have multiple Hadoop grids.
    "grid"        => 'mygrid',

    # Base path where logs will be uploaded to in Hadoop.  Logs are uploaded
    # using this path format:
    #
    #    <destination>/<grid>/daily/<queue>/<year>/<month><day>/
    #
    "destination" => "hdfs://mynamenode:9000/data/hadoop/history",
);

return 1;
# PS: Don't forget your trailing commas after each line