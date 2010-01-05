#!/usr/bin/env perl

package Analog::Mapper;
use Moose;
with 'Hadoop::Streaming::Mapper';

sub map {
    my ($self, $key, $value) = @_;

    my @segments = split /\s+/, $value;
    $self->emit($segments[1] => 1); #referrer
}

package main;
Analog::Mapper->run;
