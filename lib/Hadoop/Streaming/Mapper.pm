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

=head1 INTERFACE DETAILS


The default inputformat for streaming jobs is TextInputFormat, which returns lines without keys in the streaming context.  Because of this, map is not provided a key/value pair, instead it is given the value (the input line).

If you change your jar options to use a different JavaClassName as inputformat, you may need to deal with key and value. TBD.

quoting from:  http://hadoop.apache.org/common/docs/r0.20.2/streaming.html#Specifying+Other+Plugins+for+Jobs 
=over 4
Specifying Other Plugins for Jobs

Just as with a normal Map/Reduce job, you can specify other plugins for a streaming job:

   -inputformat JavaClassName
   -outputformat JavaClassName
   -partitioner JavaClassName
   -combiner JavaClassName

The class you supply for the input format should return key/value pairs of Text class. If you do not specify an input format class, the TextInputFormat is used as the default. Since the TextInputFormat returns keys of LongWritable class, which are actually not part of the input data, the keys will be discarded; only the values will be piped to the streaming mapper.

The class you supply for the output format is expected to take key/value pairs of Text class. If you do not specify an output format class, the TextOutputFormat is used as the default. 

=back


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
