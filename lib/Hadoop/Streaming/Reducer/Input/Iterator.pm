package Hadoop::Streaming::Reducer::Input::Iterator;
use Moose;
with 'Hadoop::Streaming::Role::Iterator';

use Hadoop::Streaming::Reducer::Input::ValuesIterator;

has input => (
    is       => 'ro',
    isa      => 'Hadoop::Streaming::Reducer::Input',
    required => 1,
);

has current_key => (
    is   => 'rw',
    does => 'Str'
);

=method has_next

    $Iterator->has_next();

Checks if the iterator has a next_key.  Returns 1 if there is another key in the input iterator.

=cut

sub has_next {
    my $self = shift;
    return if not defined $self->input->next_key;
    1;
}

=method next

    $Iterator->next();

Returns the key and value iterator for the next key.  Discards any remaining values from the current key.

Moves the iterator to the next key value, and returns the output of retval( $key, $value);

=cut

sub next {
    my $self = shift;

    if ( not defined $self->current_key ) {
        $self->current_key($self->input->next_key);
        return $self->retval( $self->current_key );
    }

    if ($self->current_key ne $self->input->next_key) {
        $self->current_key($self->input->next_key);
        return $self->retval( $self->current_key );
    }

    my ($key, $value);
    do {
        ($key, $value) = $self->input->each or return;
    } while ($self->current_key eq $key);
    $self->current_key( $key );

    return $self->retval($key, $value);
}

=method retval

    $Iterator->retval($key );
    $Iterator->retval($key, $value);

Returns an two element array containing the key and a Hadoop::Streaming::Reducer::Input::ValuesIterator initialized with the given value as the first element.

( $key, $ValueIterator)

=cut

sub retval {
    my ($self, $key, $value) = @_;
    return (
        $key,
        Hadoop::Streaming::Reducer::Input::ValuesIterator->new(
            input_iter => $self,
            first      => $value,
        ),
    );
}

__PACKAGE__->meta->make_immutable;

1;
