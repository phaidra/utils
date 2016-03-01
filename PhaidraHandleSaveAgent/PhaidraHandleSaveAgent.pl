#!/usr/bin/env perl

=pod

=head1 PhaidraHandleSaveAgent.pl (-since sinceepoch)

  -since sinceepoch - check irma database for handle records since this date

=cut

use strict;
use warnings;
use Data::Dumper;
use POSIX qw(strftime);
use Mango::BSON ':bson';
use Mango::BSON::ObjectID;
use FindBin;
use lib $FindBin::Bin;

my $configpath = 'PhaidraHandleSaveAgent.json';

unless(-f $configpath){
	print scalar localtime, " Error: config path $configpath is not a file (or file does not exist)\n";
	system ("perldoc '$0'"); exit (0);
}

unless(-r $configpath){
	print scalar localtime, " Error: cannot access config: $configpath\n";
	system ("perldoc '$0'"); exit (0);
}

my $bytes = slurp $configpath;	
my $config = decode_json($bytes);

my $since;
my $irma_map_id;
my $ingest_instance;

while (defined (my $arg= shift (@ARGV)))
{
  if ($arg =~ /^-/)
  {
  	   if ($arg eq '-since') { $since = shift (@ARGV); }
  	elsif ($arg eq '-irma-map-id') { $irma_map_id = shift (@ARGV); }
    else { system ("perldoc '$0'"); exit (0); }
  }
}

my $irma_mongo = MongoDB::MongoClient->new(host => $config->{irma_mongodb}->{host}, username => $config->{irma_mongodb}->{username}, password => $config->{irma_mongodb}->{password}, db_name => $config->{irma_mongodb}->{database});
my $paf_mongo = MongoDB::MongoClient->new(host => $config->{paf_mongodb}->{host}, username => $config->{paf_mongodb}->{username}, password => $config->{paf_mongodb}->{password}, db_name => $config->{paf_mongodb}->{database});

my @records;
if(defined($irma_map_id)){
	print scalar localtime, " processing IRMA record id $irma_map_id\n"; 
	my $rec = $irma_mongo->get_collection('irma.map')->find_one({'_id' => MongoDB::OID->new(value => $irma_map_id)});
	push @records, $rec if defined $rec;
}elsif(defined($since)){
	print scalar localtime, " processing IRMA records since $since [".strftime("%m/%d/%Y %H:%M:%S",localtime($since))."]"; 
	my $recs = $irma_mongo->get_collection('irma.map')->find({'_created' => {'$gt' => $since } } );
	while (my $rec = $recs->next) {
		push @records, $rec if defined $rec;
	}	
}

my $rec_cnt = scalar @records;
print scalar localtime, " found $rec_cnt records\n"; 
my $i = 0;
for my $r (@records){
	$i++;

	unless ($r->{url} =~ /^http(s)?:\/\/([\w\.]+)\/(o:\d+)$/g){
		print scalar localtime, " processing [$i/$rec_cnt] ur[".$r->{url}."] not a phaidra url, skipping\n"; 
		next;
	}

	my $instance = $2;
	my $pid = $3;

	print scalar localtime, " processing [$i/$rec_cnt] pid[$3] instance[$2]\n"; 

	#emit_event($paf_mongo, $pid);
}

exit(1);

sub emit_event {
	my $paf_mongo = shift;
	my $pid = shift;

	$paf_mongo->get_collection('events')->insert({
		ts_iso => ts_iso(), 
		event => 'handle_save_finished',
		e => time, 
		pid => $pid
	});
}

sub ts_iso {
	my @ts = localtime (time());
	sprintf ("%04d%02d%02dT%02d%02d%02d", $ts[5]+1900, $ts[4]+1, $ts[3], $ts[2], $ts[1], $ts[0]);
}