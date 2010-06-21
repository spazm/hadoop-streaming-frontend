package Hadoop::Streaming::Mapper;
use Moose::Role;
use IO::Handle;

with 'Hadoop::Streaming::Role::Emitter';
#requires qw(emit counter status); #from Hadoop::Streaming::Role::Emitter
requires qw(map);  # from consumer

# ABSTRACT: Simplify writing Hadoop Streaming Mapper jobs.  Write a map() function and let this role handle the Stream interface.

=head1 SYNOPSIS

  #!/usr/bin/env perl
  
  package Wordcount::Mapper;
  use Moose;
  with 'Hadoop::Streaming::Mapper';
  
  sub map
  {
    my ( $self, $line ) = @_;
    $self->emit( $_ => 1 ) for ( split /\s+/, $line );
  }
  
  package main;
  Wordcount::Mapper->run;

Your mapper class must implement map($key,$value) and your reducer must 
implement reduce($key,$value).  Your classes will have emit(), counter(),
status() and run() methods added via a role.

=cut

=method run

    Package->run();

This method starts the Hadoop::Streaming::Mapper instance.  

After creating a new object instance, it reads from STDIN and calls 
$object->map() on each line of input.  Subclasses need only implement map() 
to produce a complete Hadoop Streaming compatible mapper.

=cut

sub run
{
    my $class = shift;
    my $self  = $class->new;

    while ( my $line = STDIN->getline )
    {
        chomp $line;
        $self->map($line);
    }
}

1;
