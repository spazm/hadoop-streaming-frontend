package Hadoop::Streaming::Combiner;
use Moose::Role;

use IO::Handle;
use Hadoop::Streaming::Reducer::Input;

with 'Hadoop::Streaming::Role::Emitter';
requires qw/combine/;

# ABSTRACT: Simplify writing Hadoop Streaming jobs.  Combiner follows the same interface as Reducer.  Requires a combine() function which will be called for each line of combiner data.  Combiners are run on the same machine as the mapper as a pre-reduce reduction step.

=head1 SYNOPSIS

    #!/usr/bin/env perl

    package WordCount::Combiner;
    use Moose;
    with qw/Hadoop::Streaming::Combiner/;

    sub combine {
        my ($self, $key, $values) = @_;

        my $count = 0;
        while ( $values->has_next ) {
            $count++;
            $values->next;
        }

        $self->emit( $key => $count );
    }

    package main;
    WordCount::Combiner->run;

This combiner is identical to our reduce example.  In this case, the reducer could be used as the combiner as it maps (k,v)->(k',v') with the same key and value format.

This method exists as a convenience factor as a wrapper around the Hadoop::Streaming::Reduce pieces.

Your mapper class must implement map($key,$value), your optional combiner must implement combine($key,$value), and your reducer must implement reduce($key,$value).    The combiner should generally output ($key,$value_combine) in the same format to allow the user to engage or disengage the optional combiner step as necessary to optimize the job run.

Your classes will have emit() and run() methods added via the role.

=cut

=method run

    Package->run();

This method starts the Hadoop::Streaming::Combiner instance.  

After creating a new object instance, it reads from STDIN and calls $object->combine( ) passing in the key and an iterator of values for that key.

Subclasses need only implement combine() to produce a complete Hadoop Streaming compatible reducer.

=cut

sub run {
    my $class = shift;
    my $self = $class->new;

    my $input = Hadoop::Streaming::Reducer::Input->new(handle => \*STDIN);
    my $iter = $input->iterator;

    while ($iter->has_next) {
        my ($key, $values_iter) = $iter->next or last;
        eval {
            $self->combine( $key => $values_iter );
        };
        if ($@) {
            warn $@;
        }
    }
}

1;
