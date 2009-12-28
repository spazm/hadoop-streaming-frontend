package Hadoop::Streaming::Mapper;
use Moose::Role;

use IO::Handle;
use Params::Validate qw/validate_pos/;

with 'Hadoop::Streaming::Role::Emitter';
requires qw/map/;

# ABSTRACT: Simplify writing Hadoop Streaming jobs, now just write a map and reduce function and you're done.

=head1 SYNOPSIS

    #!/usr/bin/perl

    package Analog::Mapper;
    use Moose;
    with 'Hadoop::Streaming::Mapper';

    sub map {
        my ($self, $line ) = @_;

        my @segments = split /\s+/, $line;
        $self->emit($segments[8] => 1);
    }

    package main;
    Analog::Mapper->run;

=cut

=method run

    Package->run();

This method starts the Hadoop::Streaming::Mapper instance.  

After creating a new object instance, it reads from STDIN and calls $object->map() on each line of input.
Subclasses need only implement map() to produce a complete Hadoop Streaming compatible mapper.

=cut

sub run {
    my $class = shift;
    my $self = $class->new;

    ## FIXME: 入力の形式に併せて処理を変更
    while (my $line = STDIN->getline) {
        chomp $line;

        ## SequenceFileAsTextInputFormat
        #my ($key, $value) = split /\t/, $line;

        $self->map(undef, $line);
    }
}

=method emit

    $object->emit( $key, $value )

This method emits a key,value pair in the format expected by Hadoop::Streaming.  It does this 
by calling $self->put().  Catches errors from put and turns them into warnings.

=cut

sub emit {
    my ($self, $key, $value) = @_;
    eval {
        $self->put($key, $value);
    };
    if ($@) {
        warn $@;
    }
}

=method put

    $object->put( $key, $value )

This method emits a key,value pair to STDOUT in the format expected by Hadoop::Streaming. (key\tvalue\n)

=cut 

sub put {
    my ($self, $key, $value) = validate_pos(@_, 1, 1, 1);
    printf "%s\t%s\n", $key, $value;
}

1;
