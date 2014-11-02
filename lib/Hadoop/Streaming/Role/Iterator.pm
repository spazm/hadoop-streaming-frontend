package Hadoop::Streaming::Role::Iterator;

use Moo::Role;

requires qw(has_next next);
#ABSTRACT: Role to require has_next and next

1;

