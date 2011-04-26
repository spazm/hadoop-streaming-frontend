package Hadoop::Streaming::Reducer::Input::ValuesIterator;
use Any::Moose;
with 'Hadoop::Streaming::Role::Iterator';

#ABSTRACT: Role providing access to values for a given key.

has input_iter => (
    is       => 'ro',
    does     => 'Hadoop::Streaming::Role::Iterator',
    required => 1,
);

has first => ( is => 'rw', );

=method has_next

    $ValuesIterator->has_next();

Checks if the ValueIterator has another value available for this key.

Returns 1 on success, 0 if the next value is from another key, and undef if there is no next key.

=cut

sub has_next
{
    my $self = shift;
    return 1 if $self->first;
    return unless defined $self->input_iter->input->next_key;
    return $self->input_iter->current_key eq
        $self->input_iter->input->next_key ? 1 : 0;
}

=method next

    $ValuesIterator->next();

Returns the next value available.  Reads from $ValuesIterator->input_iter->input

=cut

sub next
{
    my $self = shift;
    if ( my $first = $self->first )
    {
        $self->first(undef);
        return $first;
    }
    my ( $key, $value ) = $self->input_iter->input->each;
    $value;
}

__PACKAGE__->meta->make_immutable;

1;
