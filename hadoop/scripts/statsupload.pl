#!/usr/bin/perl

# Hadoop log uploader
# authors: Allen Wittenauer, Adam Faris

use warnings;
use strict;
use File::Find     ();
use File::Basename ();
use Getopt::Long;
use Date::Calc ( "Add_Delta_Days" );

# Essentially how this works is we do a LSR on the hdfs data dir to get a list of files.
# Next we use find to find files less then "X" days.
# We compare the two sources and remove the files found on hdfs from the list to push.
# We prepare a new logfile name to something sane as jobnames are free form and users could call it whatever.
# Finally shell out to hadoop with a dfs put to push the files from local disk to hdfs.

my ( $CONFIG, $GRID, $HADOOP_DEST, $QUEUES, $DAYS, $options, $HADOOP_HOME, $HADOOP, $HADOOP_LOG_DIR );

# Keep track of log files on local disk to upload.
my @NONXMLS;

# Only upload logs older than 24 hours.
my $OLDERTHAN = time() - ( 60 * 60 * 24 * 1 );

my $NEWERTHAN;

# Keeps track of which files are already uploaded to HDFS.
my %DIRSTRUCT = ();

sub usage {
    print "statsupload.pl --config /path/to/my/config/file.pm\n";
    exit 1;
}

sub prefilter {
    # prefilter does a shell escape to the hadoop command to get a listing of files on hdfs
    # it populates a global hash named %DIRSTRUCT, where the file name is the key and each
    # value is 1
    my $queue = shift;
    my $Day   = shift;
    my $Month = shift;
    my $Year  = shift;
    my @line;
    if ( $Month < 10 ) {
        $Month = "0" . $Month;
    }
    if ( $Day < 10 ) {
        $Day = "0" . $Day;
    }

    my $path = "$HADOOP_DEST/$GRID/daily/$queue/$Year/${Month}${Day}";

    my $cmd = "$HADOOP dfs -lsr $path";

    print "$cmd\n";

    open( FH, "$cmd 2>/dev/null|" );
    while ( <FH> ) {
        @line = split( /\s+/ );
        my $hdfsfile = $line[7];
        $DIRSTRUCT{ $hdfsfile } = 1;    # this should be hash of hdfs filenames
    }
    close( FH );

    return ( 1 );

}

sub pathbuilder {
    # pathbuilder will create the hdfs location where we will store files on hdfs.
    # as you can see, it uses the global hash, %DIRSTRUCT to see if a file exists
    # on hdfs.   if the file does not exist, it will shell out to the hadoop command
    # to create HDFS files and paths.
    my $typedir = shift;
    my $grid    = shift;
    my $year    = shift;
    my $month   = shift;
    my $day     = shift;
    my $queue   = shift;
    my $name    = shift;
    my $newname;

    if ( !exists( $DIRSTRUCT{"$HADOOP_DEST/$grid"} ) ) {
        system( "$HADOOP dfs -mkdir $HADOOP_DEST/$grid 2>/dev/null" );
        $DIRSTRUCT{"$HADOOP_DEST/$grid"} = 1;
    }

    if ( !exists( $DIRSTRUCT{"$HADOOP_DEST/$grid/$typedir"} ) ) {
        system( "$HADOOP dfs -mkdir $HADOOP_DEST/$grid/$typedir 2>/dev/null" );
        $DIRSTRUCT{"$HADOOP_DEST/$grid/$typedir"} = 1;
    }

    if ( !exists( $DIRSTRUCT{"$HADOOP_DEST/$grid/$typedir/$queue"} ) ) {
        system( "$HADOOP dfs -mkdir $HADOOP_DEST/$grid/$typedir/$queue 2>/dev/null" );
        $DIRSTRUCT{"$HADOOP_DEST/$grid/$typedir/$queue"} = 1;
    }
    $newname = sprintf( "$HADOOP_DEST/$grid/$typedir/$queue/%04d", $year );
    if ( !exists( $DIRSTRUCT{"$newname"} ) ) {
        system( "$HADOOP dfs -mkdir $newname 2>/dev/null" );
        $DIRSTRUCT{"$newname"} = 1;
    }

    $newname = sprintf( "$HADOOP_DEST/$grid/$typedir/$queue/%04d/%02d%02d", $year, $month, $day );
    if ( !exists( $DIRSTRUCT{"$newname"} ) ) {
        system( "$HADOOP dfs -mkdir $newname 2>/dev/null" );
        $DIRSTRUCT{"$newname"} = 1;
    }

    $name = File::Basename::basename( $name );
    $newname = sprintf( "$HADOOP_DEST/$grid/$typedir/$queue/%04d/%02d%02d/$name", $year, $month, $day, $name );
    $newname =~ s,//,/,g;
    $newname =~ s,hdfs:/,hdfs://,g;
    return ( $newname );
}

sub wanted {
    # looks at log files on local disk and puts entries in global array named @NONXMLS.
    my $j = $File::Find::name;
    my $ftime;
    my $base = File::Basename::basename( $j );

    if ( ( -f $j ) && ( $j !~ /xml$/ ) && ( $base !~ /^\./ ) && ( $j !~ /\.crc$/ ) ) {
        $ftime = ( stat( $j ) )[9];
        if ( $ftime < $OLDERTHAN && $ftime >= $NEWERTHAN) {
            push( @NONXMLS, $j );
        }
    }
    return 1;
}
sub findqueue {
    # open file and find queue name
    my $file = shift;
    my ( $line, $beg, $q );

    open( FH, "<$file" ) || do {
        print STDERR "$0: $! [$file]\n";
        return ( undef );
    };
    while ( <FH> ) {
        if ( /<property>/ ) {
            if ( /<name>/ ) {
                if ( />mapred.job.queue.name</ ) {
                    $line = $_;
                    $beg  = ( split( /value/ ) )[1];
                    $q    = ( split( /[><]/, $beg ) )[1];
                }
            }
        }
    }
    close( FH );

    if ( !$q ) {    # we didn't find any queue names
        $q = "unknown";
    }

    return ( $q );

}

# GO MAIN GO!

# we only care about one option and that's our config file
$options = GetOptions( "configuration|config|c=s" => \$CONFIG, );

if ( !$CONFIG ) { usage(); }
# load config file
if ( -r $CONFIG ) {
    # lazy way to suck in config file as we don't have fancy YAML libs available
    require $CONFIG;
    if ( exists( $cfg::CFG{'grid'} ) ) {
        $GRID = $cfg::CFG{'grid'};
    } else {
        die( "Make sure 'grid' is set in your config file" );
    }
    if ( exists( $cfg::CFG{'queues'} ) ) {
        $QUEUES = $cfg::CFG{'queues'};
    } else {
        die( "Make sure 'queues' is set in your config file" );
    }
    if ( exists( $cfg::CFG{'destination'} ) ) {
        $HADOOP_DEST = $cfg::CFG{'destination'};
    } else {
        die( "Make sure 'destination' is set in your config file" );
    }
    if ( exists( $cfg::CFG{'days'} ) ) {
        $DAYS = $cfg::CFG{'days'};

        # Ignore local log files older than specified
        # number of days.  Otherwise due to the comparison
        # with files found in HDFS we will upload files which
        # may already be in HDFS.
        $NEWERTHAN = time() - ( 60 * 60 * 24 * $DAYS )
    } else {
        die( "Make sure 'days' is set in your config file" );
    }
    if ( exists( $cfg::CFG{'hadoop_home'} ) ) {
        $HADOOP_HOME = $cfg::CFG{'hadoop_home'};
        $HADOOP      = "$HADOOP_HOME/bin/hadoop";
    } else {
        die( "Make sure 'hadoop_home' is set in your config file" );
    }
    if ( exists( $cfg::CFG{'hadoop_logs'} ) ) {
        $HADOOP_LOG_DIR = $cfg::CFG{'hadoop_logs'};
    } else {
        die( "Make sure 'hadoop_logs' is set in your config file" );
    }
} else {
    die( "Unable to read $CONFIG" );
}

do {
    my ( $filename, $beg, $line, $queue, $logfile, $jobconfxml, $hdfsname, $confname );
    my ( $year, $month, $day, $ftime );
    my ( $deltayear, $deltamonth, $deltaday );
    my @fileparts;

    if ( -f "/tmp/statsupload.lock" ) {
        print STDERR "$0: already running (found /tmp/statsupload.lock)\n";
        exit 1;
    }

    open( FH, ">/tmp/statsupload.lock" ) or die "$0:$! [/tmp/statsupload.lock]";
    print FH "hey";
    close( FH );

    # figure out which days we need ...
    my ( $d, $m, $y ) = ( localtime() )[3, 4, 5];
    $y += 1900;
    $m += 1;

    print "Checking the last $DAYS days in HDFS for existing data\n";

    # loop from 0 to $DAYS and while doing so, call Add_Delta_Days for each day.
    for ( my $daycount = 0; $daycount <= $DAYS; $daycount++ ) {
        # we multiply $daycount by -1 to get negative number allowing us to work our way
        # backwards from today.
        ( $deltayear, $deltamonth, $deltaday ) = Add_Delta_Days( $y, $m, $d, ( $daycount * -1 ) );
        for my $q ( @$QUEUES ) {
            prefilter( $q, $deltaday, $deltamonth, $deltayear );
        }

    }

    print "Found " . keys( %DIRSTRUCT ) . " existing files in HDFS\n";

    my $history_dir = "$HADOOP_LOG_DIR/history";

    print "\nSearching $history_dir for logs\n";

    File::Find::find( { wanted => \&wanted }, $history_dir );

    my $upload_count = 0;
    my $existing_count = 0;
    my $total = 0;
    my $skipped = 0;
    my $failed = 0;

    foreach $filename ( @NONXMLS ) {

        $total += 1;

        $queue = "unknown";

        #
        # find the queue
        #

        $logfile = $filename;

        @fileparts   = split( /_/, File::Basename::basename($logfile) );

        my $job_index = 0;

        ++$job_index until $fileparts[$job_index] eq "job" or $job_index >= $#fileparts;

        if ($job_index >= $#fileparts)
        {
            next;
        }

        my $job_name = join( '_', @fileparts[($job_index) .. ($job_index + 2)] );

        # Find the job conf xml.  There should only be one matching the job name.
        my $conf_pattern = File::Basename::dirname($logfile) . "/" . "*" . $job_name . "_conf.xml";
        $jobconfxml = undef;
        foreach (glob ($conf_pattern)) {
            $jobconfxml = $_;
            last;
        }

        if (!$jobconfxml) {
            print "\nFailed to locate job conf xml file for $job_name, skipping...\n";
            $skipped += 1;
            next;
        }

        # Get the job queue from the job conf xml.
        $queue = findqueue( $jobconfxml );
        if ( !$queue ) { next; }

        $ftime = ( stat( $logfile ) )[9];
        ( $day, $month, $year ) = ( localtime( $ftime ) )[3, 4, 5];
        $year  += 1900;
        $month += 1;
        $hdfsname = pathbuilder( "daily", $GRID, $year, $month, $day, $queue, $job_name ) . ".log";
        
        # as that is missing from keys in %DIRSTRUCT
        # need to strip 'hdfs://mynamenode.example.com:9000' off hdfsname
        $hdfsname =~ m!^hdfs://.*?(/.*$)!;
        if ( !exists( $DIRSTRUCT{$1} ) ) {
            print "\nUploading $logfile\n";
            print "-> $hdfsname\n";
            my $cmd = "$HADOOP dfs -put $logfile $hdfsname";
            print "command: $cmd\n";
            my $return_code = system( "$cmd" );
            if ($return_code == 0)
            {
                $upload_count += 1;
            }
            else
            {
                print "Upload failed\n";
                $failed += 1;
            }
        }
        else {
            $existing_count += 1;
        }

        $confname = pathbuilder( "daily", $GRID, $year, $month, $day, $queue, $job_name . "_conf.xml" );
        # need to strip 'hdfs://mynamenode.example.com:9000' off confname
        # as that is missing from keys in %DIRSTRUCT
        $confname =~ m!^hdfs://.*?(/.*$)!;
        if ( !exists( $DIRSTRUCT{$1} ) ) {
            print "\nUploading $jobconfxml\n";
            print "-> $confname\n";
            my $cmd = "$HADOOP dfs -put $jobconfxml $confname";
            print "command: $cmd\n";
            my $return_code = system( "$cmd" );
            if ($return_code == 0)
            {
                $upload_count += 1;
            }
            else
            {
                print "Upload failed\n";
                $failed += 1;
            }
        }
        else {
            $existing_count += 1;
        }
    }

    if ($total == 0)
    {
        print "\nFound no new logs to upload\n";
    }
    else
    {
        print "\nUploaded $upload_count files, found $existing_count existing, skipped $skipped, and $failed failed\n"
    }

    unlink( "/tmp/statsupload.lock" );
};