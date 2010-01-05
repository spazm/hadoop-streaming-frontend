#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests=>4;
use Test::Command;

use FindBin;
my $path="$FindBin::Bin/wordcount/";

my $map             = $path . 'map.pl';
my $reduce          = $path . 'reduce.pl';
my $input           = $path . 'terms.txt';
my $expected_map    = $path . 'expected-map.out';
my $expected_reduce = $path . 'expected-reduce.out';

TEST_MAP:
{
    my $map_cmd = Test::Command->new( cmd => "perl $map < $input" );
    $map_cmd->exit_is_num( 0, 'map exit value is 0' );
    $map_cmd->stdout_is_file( $expected_map,
        "map output matches expected [$expected_map]" );
}

TEST_REDUCE:
{
    my $reduce_cmd = Test::Command->new( cmd => "sort $expected_map | perl $reduce" );
    $reduce_cmd->exit_is_num( 0, 'reducer exit value is 0' );
    $reduce_cmd->stdout_is_file( $expected_reduce,
        "reduce output matches expected [$expected_reduce]" );
}
