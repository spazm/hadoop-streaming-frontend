#!/usr/bin/env perl

package Wordcount::Mapper;
use Moose;
with 'Hadoop::Streaming::Mapper';

sub map {
    my ($self, $key, $value) = @_;

    for (split /\s+/, $value) {
        $self->emit( $_ => 1 );
    }
}

package main;
Wordcount::Mapper->run;
