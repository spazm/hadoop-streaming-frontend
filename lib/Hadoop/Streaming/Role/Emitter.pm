package Hadoop::Streaming::Role::Emitter;
use Any::Moose 'Role';
use Params::Validate qw/validate_pos/;

#provides qw(run emit counter status);

# ABSTRACT: Role to provide emit, counter, and status interaction with Hadoop::Streaming.

=method emit

    $object->emit( $key, $value )

This method emits a key,value pair in the format expected by Hadoop::Streaming.
It does this by calling $self->put().  This catches errors from put and turns 
them into warnings.

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

This method emits a key,value pair to STDOUT in the format expected by 
Hadoop::Streaming: ( key \t value \n )

=cut 

sub put 
{
    my ($self, $key, $value) = validate_pos(@_, 1, 1, 1);
    printf "%s\t%s\n", $key, $value;
}

=method counter

    $object->counter(
        group   => $group,
        counter => $countername,
        amount  => $count,
    );

This method emits a counter key to STDERR in the format expected by hadoop:
  reporter:counter:<group>,<counter>,<amount>

=cut 

sub counter
{
    my ( $self, %opts ) = @_;

    my $group   = $opts{group}   || 'group';
    my $counter = $opts{counter} || 'counter';
    my $amount  = $opts{amount}  || 'amount';

    my $msg
        = sprintf( "reporter:counter:%s,%s,%s\n", $group, $counter, $amount );
    print STDERR $msg;
}

=method status

    $object->status( $message )

This method emits a status message to STDERR in the format expected by Hadoop::Streaming: 
  reporter:status:$message\n

=cut 

sub status
{
    my ($self, $message ) = @_;

    my $msg = sprintf( "reporter:status:%s\n", $message);
    print STDERR $msg;
}

1;
