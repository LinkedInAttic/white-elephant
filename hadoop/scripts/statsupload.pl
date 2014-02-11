#!/usr/bin/perl

# Hadoop log uploader
# authors: Allen Wittenauer, Adam Faris

use warnings;
use strict;
use File::Find     ();
use File::Basename ();
use Getopt::Long;
use Date::Calc ( "Add_Delta_Days" );
use Parallel::ForkManager;
use DateTime;
use XML::Simple;
use FindBin;
use lib "$FindBin::Bin/lib/perl5";
use File::Pid;

# Essentially how this works is we do a LSR on the hdfs data dir to get a list of files.
# Next we use find to find files less then "X" days.
# We compare the two sources and remove the files found on hdfs from the list to push.
# We prepare a new logfile name to something sane as jobnames are free form and users could call it whatever.
# Finally shell out to hadoop with a dfs put to push the files from local disk to hdfs.

my ( $CONFIG, $GRID, $HADOOP_DEST, $QUEUES, $DAYS, $options, $HADOOP_HOME, $HADOOP, $HADOOP_LOG_DIR );
my ( $my_queues, $my_parallel, $my_days, $my_verbose );
my ( $my_logpath, $my_loghandle );

# Keep track of log files on local disk to upload.
my @NONXMLS;

# Only upload logs older than 24 hours.
my $OLDERTHAN = time() - ( 60 * 60 * 24 * 1 );
$my_logpath = "/var/tmp/hadoop/hadoop-statsupload-log-" . DateTime->now->strftime("%Y%m%d-%H%M");
my $xml_simple = new XML::Simple;

my $NEWERTHAN;

# Keeps track of which files are already uploaded to HDFS.
my %DIRSTRUCT = ();

if ($< == 0) {
  die "ERROR| Cannot execute as root! Safety precaution. \n";
}

sub usage {
    print "statsupload.pl \n";
    print "        --config   /path/to/my/config/file.pm\n";
    print "        --queues   list of queues to process \n";
    print "        --days     number of last days\n";
    print "        --parallel number of parallel uploads to hdfs max(5) and default(2)\n";
    print "        --verbose  be loud!\n";
    exit 1;
}

sub openlog {
  unless ( open($my_loghandle, '>', $my_logpath) ) {
    die "ERROR| Unable to open ${my_logpath} to write:$@\n";
  }
}

sub logmsg {
  my $my_msg = shift;
  my $my_ts = DateTime->now;
  print STDOUT "$my_ts | $my_msg" if ( defined $my_verbose );
  print $my_loghandle "$my_ts | $my_msg";
}

sub closelog {
  if ( defined $my_loghandle ) {
    close $my_loghandle || die "ERROR| Unable to close ${my_logpath} file:$@\n";
  }
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

    my $cmd = "$HADOOP fs -lsr $path";

    logmsg( "$cmd\n" );

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
        system( "$HADOOP fs -mkdir $HADOOP_DEST/$grid 2>/dev/null" );
        $DIRSTRUCT{"$HADOOP_DEST/$grid"} = 1;
    }

    if ( !exists( $DIRSTRUCT{"$HADOOP_DEST/$grid/$typedir"} ) ) {
        system( "$HADOOP fs -mkdir $HADOOP_DEST/$grid/$typedir 2>/dev/null" );
        $DIRSTRUCT{"$HADOOP_DEST/$grid/$typedir"} = 1;
    }

    if ( !exists( $DIRSTRUCT{"$HADOOP_DEST/$grid/$typedir/$queue"} ) ) {
        system( "$HADOOP fs -mkdir $HADOOP_DEST/$grid/$typedir/$queue 2>/dev/null" );
        $DIRSTRUCT{"$HADOOP_DEST/$grid/$typedir/$queue"} = 1;
    }
    $newname = sprintf( "$HADOOP_DEST/$grid/$typedir/$queue/%04d", $year );
    if ( !exists( $DIRSTRUCT{"$newname"} ) ) {
        system( "$HADOOP fs -mkdir $newname 2>/dev/null" );
        $DIRSTRUCT{"$newname"} = 1;
    }

    $newname = sprintf( "$HADOOP_DEST/$grid/$typedir/$queue/%04d/%02d%02d", $year, $month, $day );
    if ( !exists( $DIRSTRUCT{"$newname"} ) ) {
        system( "$HADOOP fs -mkdir $newname 2>/dev/null" );
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
    my ( $q );

    my $xml_data = $xml_simple->XMLin($file);

    if ( defined $xml_data ) {
      if ( defined $xml_data->{'property'}->{'mapred.job.queue.name'}->{'value'} ) {
        $q = $xml_data->{'property'}->{'mapred.job.queue.name'}->{'value'};
      } else {
        $q = "unknown";
      }
    }

    logmsg("INFO| queue for $file is $q \n");
    undef $xml_data;
    
    return ( $q );

}

# GO MAIN GO!

# we only care about one option and that's our config file
$options = GetOptions( "configuration|config|c=s" => \$CONFIG, 
                       "verbose|v" => \$my_verbose,
                       "parallel|p=i" => \$my_parallel,
                       "days|d=i" => \$my_days,
                       "queues|q=s@" => \$my_queues,);

if ( !$CONFIG ) { usage(); }
# load config file
if ( -r $CONFIG ) {
    # lazy way to suck in config file as we don't have fancy YAML libs available
    require $CONFIG;
    openlog();
    if ( exists( $cfg::CFG{'grid'} ) ) {
        $GRID = $cfg::CFG{'grid'};
    } else {
        die( "Make sure 'grid' is set in your config file" );
    }
    if ( defined $my_queues) {
      @$QUEUES = (split(/,/,join(',',@$my_queues)));
    } elsif ( exists( $cfg::CFG{'queues'} ) ) {
        $QUEUES = $cfg::CFG{'queues'};
    } else{
        die( "Make sure 'queues' is set in your config file" );
    }
    logmsg( "INFO| Processing for queues: \n" );
    logmsg( "INFO| Queue: $_\n" ) foreach (@$QUEUES);
    if ( defined $my_parallel ) {
      unless ( $my_parallel =~ /\d/ && ( $my_parallel > 1 && $my_parallel < 6 ) ) {
        $my_parallel = 2;
      }
    } else { 
      $my_parallel = 2;
    }
    logmsg ( "INFO| Parallel uploads will be $my_parallel \n" );
    if ( exists( $cfg::CFG{'destination'} ) ) {
        $HADOOP_DEST = $cfg::CFG{'destination'};
    } else {
        die( "Make sure 'destination' is set in your config file" );
    }
    if ( defined $my_days ) {
      if ( $my_days !~ /\d+/ ) {
        $DAYS = 3; # default
      } else {
        $DAYS = $my_days;
      }
      $NEWERTHAN = time() - ( 60 * 60 * 24 * $DAYS )
    } elsif ( exists( $cfg::CFG{'days'} ) ) {
        $DAYS = $cfg::CFG{'days'};

        # Ignore local log files older than specified
        # number of days.  Otherwise due to the comparison
        # with files found in HDFS we will upload files which
        # may already be in HDFS.
        $NEWERTHAN = time() - ( 60 * 60 * 24 * $DAYS )
    } else {
        die( "Make sure 'days' is set in your config file" );
    }
    logmsg( "INFO| Number of days for which logs are to be uploaded : $DAYS \n" );
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

    my $pid_file = File::Pid->new({file => "/tmp/statsupload.lock"});

    if ( my $my_pid = $pid_file->running ) {
      die "ERROR| $0: Already running: $my_pid \n";
    }

    $pid_file->write;

    # figure out which days we need ...
    my ( $d, $m, $y ) = ( localtime() )[3, 4, 5];
    $y += 1900;
    $m += 1;

    logmsg( "Checking the last $DAYS days in HDFS for existing data\n" );

    # loop from 0 to $DAYS and while doing so, call Add_Delta_Days for each day.
    for ( my $daycount = 0; $daycount <= $DAYS; $daycount++ ) {
        # we multiply $daycount by -1 to get negative number allowing us to work our way
        # backwards from today.
        ( $deltayear, $deltamonth, $deltaday ) = Add_Delta_Days( $y, $m, $d, ( $daycount * -1 ) );
        for my $q ( @$QUEUES ) {
            prefilter( $q, $deltaday, $deltamonth, $deltayear );
        }

    }

    logmsg( "Found " . keys( %DIRSTRUCT ) . " existing files in HDFS\n");

    my $history_dir = "$HADOOP_LOG_DIR/history";

    logmsg( "\nSearching $history_dir for logs\n" );

    File::Find::find( { wanted => \&wanted }, $history_dir );

    my $upload_count = 0;
    my $existing_count = 0;
    my $total = 0;
    my $skipped = 0;
    my $failed = 0;

    my $fork_manager = new Parallel::ForkManager($my_parallel); 

    $fork_manager->run_on_finish(
        sub { 
          my ($pid, $exit_code, $ident) = @_;
          print "***** $ident just got finished with PID $pid and exit code: $exit_code\n";
        }
        );

    $fork_manager->run_on_start(
        sub { 
          my ($pid,$ident)=@_;
          print "***** $ident just started, pid: $pid\n";
        }
        );

    foreach $filename ( @NONXMLS ) {

        $fork_manager->start($filename) and next; # fork here

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
            print STDERR "\nFailed to locate job conf xml file for $job_name, skipping...\n";
            logmsg( "Failed to locate job conf xml file for $job_name, skipping...\n" );
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
            logmsg( "\nUploading $logfile\n" );
            logmsg( " --> $hdfsname\n" );
            my $cmd = "$HADOOP fs -put $logfile $hdfsname";
            logmsg( "command: $cmd \n" );
            my $return_code = system( "$cmd" );
            if ($return_code == 0)
            {
                $upload_count += 1;
            }
            else
            {
                print STDERR "ERROR| Upload failed\n";
                logmsg( "Upload failed\n" );
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
            logmsg( "\nUploading $jobconfxml\n" );
            logmsg( "-> $confname\n" );
            my $cmd = "$HADOOP fs -put $jobconfxml $confname";
            logmsg( "command: $cmd\n" );
            my $return_code = system( "$cmd" );
            if ($return_code == 0)
            {
                $upload_count += 1;
            }
            else
            {
                print STDERR "ERROR| Upload failed\n";
                logmsg( "Upload failed\n" );
                $failed += 1;
            }
        }
        else {
            $existing_count += 1;
        }

        $fork_manager->finish;
    }

    $fork_manager->wait_all_children;

    if ($total == 0)
    {
        logmsg( "\nFound no new logs to upload\n");
    }
    else
    {
        logmsg( "\nUploaded $upload_count files, found $existing_count existing, skipped $skipped, and $failed failed\n" );
    }

    $pid_file->remove or warn "Could not unlink the pid file /tmp/statsupload.lock\n";
    closelog();
};
