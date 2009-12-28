package Hadoop::Streaming::Reducer;
use Moose::Role;

with 'Hadoop::Streaming::Role::Emitter';

use IO::Handle;
use Params::Validate qw/validate_pos/;
use Hadoop::Streaming::Reducer::Input;

with 'Hadoop::Streaming::Role::Emitter';
requires qw/reduce/;

# ABSTRACT: Simplify writing Hadoop Streaming jobs, now just write a map and reduce function and you're done.


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

sub emit {
    my ($self, $key, $value) = @_;
    eval {
        $self->put($key, $value);
    };
    if ($@) {
        warn $@;
    }
}

sub put {
    my ($self, $key, $value) = validate_pos(@_, 1, 1, 1);
    printf "%s\t%s\n", $key, $value;
}

1;

