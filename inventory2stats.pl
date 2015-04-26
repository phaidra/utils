#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use DBD::mysql;
use Data::Dumper;
use MongoDB;
use Mojo::Util qw(slurp);
use Mojo::JSON qw(decode_json encode_json);

my $bytes = slurp "inventory2stats_config.json";
my $config = decode_json($bytes);

my $dbh = DBI->connect(
  $config->{statdb}->{data_source},
  $config->{statdb}->{username},
  $config->{statdb}->{password},
  {
    AutoCommit => 1,
    mysql_enable_utf8 => 1
  }
);

my $mongo = MongoDB::MongoClient->new(
  host => $config->{inventorydb}->{host},
  username => $config->{inventorydb}->{username},
  password => $config->{inventorydb}->{password},
  db_name => $config->{inventorydb}->{database}
);

my $invdb = $mongo->get_database( $config->{inventorydb}->{database} );
my $foxmldata = $invdb->get_collection('foxml.data');
my $doccount = $foxmldata->count();
my $docs = $foxmldata->find;

my $ss = "INSERT INTO inventory (oid, cmodel, owner, state, acccode, redcode, created, modified) VALUES (?,?,?,?,?,?,?,?)";
my $sth = $dbh->prepare($ss) or die "ERR: can't prepare: ".$DBI::errstr;
my $i = 0;
while (my $d = $docs->next) {
  #print Dumper($d);
  $i++;
  print "inserting $i/$doccount\n";
  $sth->execute(
    $d->{pid},
    $d->{model},
    $d->{ownerId},
    $d->{state},
    $d->{acc_code},
    $d->{red_code},
    $d->{createdDate},
    $d->{lastModifiedDate},
  ) or print "ERR: can't execute: ".$DBI::errstr."\n";

}
$sth->finish;

1
__END__
