package File::Pid;
# $Id: Pid.pm,v 1.1 2005/01/11 13:09:54 cwest Exp $
use strict;

=head1 NAME

File::Pid - Pid File Manipulation

=head1 SYNOPSIS

  use File::Pid;
  
  my $pidfile = File::Pid->new({
    file => '/some/file.pid',
  });
  
  $pidfile->write;
  
  if ( my $num = $pidfile->running ) {
      die "Already running: $num\n";
  }

  $pidfile->remove;

=cut

use vars qw[$VERSION];
$VERSION = sprintf "%d.%02d", split m/\./, (qw$Revision: 1.1 $)[1];

use File::Spec::Functions qw[tmpdir catfile];
use File::Basename qw[basename];
use base qw[Class::Accessor::Fast];

=head1 DESCRIPTION

This software manages a pid file for you. It will create a pid file,
query the process within to discover if it's still running, and remove
the pid file.

=head2 new

  my $pidfile = File::Pid->new;

  my $thisfile = File::Pid->new({
    file => '/var/run/daemon.pid',
  });

  my $thisfileandpid = File::Pid->new({
    file => '/var/run/daemon.pid',
    pid  => '145',
  });

This constructor takes two optional paramters.

C<file> - The name of the pid file to work on. If not specified, a pid
file located in C<< File::Spec->tmpdir() >> will be created that matches
C<< (File::Basename::basename($0))[0] . '.pid' >>. So, for example, if
C<$0> is F<~/bin/sig.pl>, the pid file will be F</tmp/sig.pl.pid>.

C<pid> - The pid to write to a new pidfile. If not specified, C<$$> is
used when the pid file doesn't exist. When the pid file does exist, the
pid inside it is used.

=head2 file

  my $pidfile = $pidfile->file;

Accessor/mutator for the filename used as the pid file.

=head2 pid

  my $pid = $pidfile->pid;

Accessor/mutator for the pid being saved to the pid file.

=cut

sub new {
    my $class = shift;
    my $self  = $class->SUPER::new(@_);
    $self->_get_pidfile;
    $self->_get_pid;
    return $self;
}
__PACKAGE__->mk_accessors(qw[file pid]);

=head2 write

  my $pid = $pidfile->write;

Writes the pid file to disk, inserting the pid inside the file.
On success, the pid written is returned. On failure, C<undef> is
returned.

=cut

sub write {
    my $self = shift;
    my $file = $self->_get_pidfile;
    my $pid  = $self->_get_pid;

    local *WRITEPID;
    open WRITEPID, "> $file" or return;
    print WRITEPID "$pid\n";
    close WRITEPID;
    return $pid;
}

=head2 running

  my $pid = $pidfile->running;
  die "Service already running: $pid\n" if $pid;

Checks to see if the pricess identified in the pid file is still
running. If the process is still running, the pid is returned. Otherwise
C<undef> is returned.

=cut

sub running {
    my $self = shift;
    my $pid  = $self->_get_pid_from_file;

    return   kill(0, $pid)
           ? $pid
           : undef;
}

=head2 remove

  $pidfile->remove or warn "Couldn't unlink pid file\n";

Removes the pid file from disk. Returns true on success, false on
failure.

=cut

sub remove { unlink shift->_get_pidfile }

=head2 program_name

This is a utility method that allows you to determine what
C<File::Pid> thinks the program name is. Internally this is used
when no pid file is specified.

=cut

sub program_name {
    my $self = shift;
    my ($name) = basename($0);
    return $name;
}

sub _get_pidfile {
    my $self = shift;
    return $self->file if $self->file;

    my $file = catfile tmpdir, $self->program_name . '.pid';
    $self->file($file);
    return $self->file;
}

sub _get_pid {
    my $self = shift;
    return $self->pid if $self->pid;
    $self->pid($self->_get_pid_from_file || $$);
    return $self->pid;
}

sub _get_pid_from_file {
    my $self = shift;
    my $file = $self->_get_pidfile;
    local *READPID;
    open READPID, "< $file" or return;
    chomp(my $pid = <READPID>);
    close READPID;
    return $pid;
}

1;

__END__

=head1 SEE ALSO

L<perl>.

=head1 AUTHOR

Casey West, <F<casey@geeknest.com>>.

=head1 COPYRIGHT

  Copyright (c) 2005 Casey West.  All rights reserved.
  This module is free software; you can redistribute it and/or modify it
  under the same terms as Perl itself.

=cut
