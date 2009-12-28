package Hadoop::Streaming::Reducer;
use Moose::Role;

with 'Hadoop::Streaming::Role::Emitter';

use IO::Handle;
use Params::Validate qw/validate_pos/;
use Hadoop::Streaming::Reducer::Input;

with 'Hadoop::Streaming::Role::Emitter';
requires qw/reduce/;

# ABSTRACT: Simplify writing Hadoop Streaming jobs, now just write a map and reduce function and you're done.

=method run

    Package->run();

This method starts the Hadoop::Streaming::Reducer instance.  

After creating a new object instance, it reads from STDIN and calls $object->reduce( ) passing in the key and an iterator of values for that key.

Subclasses need only implement reduce() to produce a complete Hadoop Streaming compatible reducer.

=cut

sub run {
    my $class = shift;
    my $self = $class->new;

    my $input = Hadoop::Streaming::Reducer::Input->new(handle => \*STDIN);
    my $iter = $input->iterator;

    while ($iter->has_next) {
        my ($key, $values_iter) = $iter->next or last;
        eval {
            $self->reduce( $key => $values_iter );
        };
        if ($@) {
            warn $@;
        }
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

