package Hadoop::Streaming::Role::Iterator;
use Any::Moose 'Role';

requires qw(has_next next);
#ABSTRACT: Role to require has_next and next

1;

