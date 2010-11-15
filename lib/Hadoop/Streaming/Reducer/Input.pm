package Hadoop::Streaming::Reducer::Input;
use Moose;
use Hadoop::Streaming::Reducer::Input::Iterator;

#ABSTRACT: Parse input stream for reducer

has handle => (
    is       => 'ro',
    does     => 'FileHandle',
    required => 1,
);

has buffer => (
    is   => 'rw',
);

=method next_key 

    $Input->next_key();

Parses the next line into key/value (splits on tab) and returns the key portion.

Returns undef if there is no next line.

=cut

sub next_key
{
    my $self = shift;
    my $line = $self->buffer ? $self->buffer : $self->next_line;
    return if not defined $line;
    my ( $key, $value ) = split /\t/, $line, 2;
    return $key;
}

=method next_line

    $Input->next_line();

Reads the next line into buffer and returns it.

Returns undef if there are no more lines (end of file).

=cut

sub next_line {
    my $self = shift;
    return if $self->handle->eof;
    $self->buffer( $self->handle->getline );
    $self->buffer;
}

=method getline

    $Input->getline();

Returns the next available line. Clears the internal line buffer if set.

=cut

sub getline {
    my $self = shift;
    if (defined $self->buffer) {
        my $buf = $self->buffer;
        $self->buffer(undef);
        return $buf;
    } else {
        return $self->next_line;
    }
}

=method iterator

    $Input->iterator();

Returns a new Hadoop::Streaming::Reducer::Input::Iterator for this object.

=cut

sub iterator {
    my $self = shift;
    Hadoop::Streaming::Reducer::Input::Iterator->new( input => $self );
}

=method each

    $Input->each();

Grabs the next line and splits on tabs.  Returns an array containing the output of the split.

=cut

sub each
{
    my $self = shift;
    my $line = $self->getline or return;
    chomp $line;
    split /\t/, $line, 2;
}

__PACKAGE__->meta->make_immutable;

1;
