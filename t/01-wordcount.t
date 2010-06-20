#!/usr/bin/env perl 

use strict;
use warnings;

use Test::More tests=>7;
use Test::Command;

use FindBin;
ok( $FindBin::Bin , "FindBin::Bin set" );

my $path="$FindBin::Bin/wordcount/";

my $perl            = '/usr/bin/env perl';
my $sort            = $FindBin::Bin . '/sort.pl';

my $map             = $path . 'map.pl';
my $reduce          = $path . 'reduce.pl';
my $input           = $path . 'terms.txt';
my $expected_map    = $path . 'expected-map.out';
my $expected_reduce = $path . 'expected-reduce.out';

TEST_MAP:
{
    my $map_cmd = Test::Command->new( cmd => "$perl $map < $input" );
    $map_cmd->exit_is_num( 0, 'map exit value is 0' );
    $map_cmd->stdout_is_file( $expected_map,
        "map output matches expected [$expected_map]" );
    $map_cmd->stderr_is_eq( '', "stderr is blank");
}

TEST_REDUCE:
{
    my $reduce_cmd = Test::Command->new( cmd => "$perl $sort $expected_map | $perl $reduce" );
    $reduce_cmd->exit_is_num( 0, 'reducer exit value is 0' );
    $reduce_cmd->stdout_is_file( $expected_reduce,
        "reduce output matches expected [$expected_reduce]" );
    $reduce_cmd->stderr_is_eq( '', "stderr is blank");
}
