#!/usr/bin/env perl

=pod

=head1 PhaidraHandleSaveAgent.pl (-since sinceepoch)

  -since sinceepoch - check irma database for handle records since this date

=cut

use strict;
use warnings;
use Data::Dumper;
use POSIX qw(strftime);
use MongoDB;
use Mojo::Util qw(slurp);
use Mojo::JSON qw(encode_json decode_json);
use Mojo::UserAgent;
use Mojo::URL;

my $configpath = 'PhaidraHandleSaveAgent.json';

unless(-f $configpath){
	print "[".scalar localtime."] ", "Error: config path $configpath is not a file (or file does not exist). Usually $configpath would be a link to /etc/phaidra/...\n";
	system ("perldoc '$0'"); exit (0);
}

unless(-r $configpath){
	print "[".scalar localtime."] ", "Error: cannot access config: $configpath\n";
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

unless(defined($since) || defined($irma_map_id)){
	print "[".scalar localtime."] ", "Error: Missing parameters.\n";
	system ("perldoc '$0'"); exit (0);
}


my $irma_mongo = MongoDB::MongoClient->new(host => $config->{irma_mongodb}->{host}, username => $config->{irma_mongodb}->{username}, password => $config->{irma_mongodb}->{password}, db_name => $config->{irma_mongodb}->{database})->get_database($config->{irma_mongodb}->{database});

my %paf_dbs;
while( my( $instance, $value ) = each %{$config->{phaidra_instances}} ){	
	if(defined($value->{paf_mongodb})){
		$paf_dbs{$instance} = MongoDB::MongoClient->new(host => $value->{paf_mongodb}->{host}, username => $value->{paf_mongodb}->{username}, password => $value->{paf_mongodb}->{password}, db_name => $value->{paf_mongodb}->{database})->get_database($value->{paf_mongodb}->{database}); 
	}
}

my @records;
if(defined($irma_map_id)){
	print "[".scalar localtime."] ", "processing IRMA record id $irma_map_id\n"; 
	my $rec = $irma_mongo->get_collection('irma.map')->find_one({'_id' => MongoDB::OID->new(value => $irma_map_id)});
	push @records, $rec if defined $rec;
}elsif(defined($since)){
	print "[".scalar localtime."] ", "processing IRMA records since $since [".strftime("%m/%d/%Y %H:%M:%S",localtime($since))."]"; 
	my $recs = $irma_mongo->get_collection('irma.map')->find({'_created' => {'$gt' => $since } } );
	while (my $rec = $recs->next) {
		push @records, $rec if defined $rec;
	}	
}

my $rec_cnt = scalar @records;
print "[".scalar localtime."] ", "found $rec_cnt records\n"; 
my $i = 0;
for my $r (@records){
	$i++;

	unless ($r->{url} =~ /^http(s)?:\/\/([\w\.\-]+)\/(o:\d+)$/g){
		print "[".scalar localtime."] ", "processing [$i/$rec_cnt] url[".$r->{url}."] not a phaidra url, skipping\n"; 
		next;
	}

	my $instance = $2;
	my $pid = $3;

	print "[".scalar localtime."] ", "processing [$i/$rec_cnt] pid[$pid] hdl[".$r->{hdl}."] instance[$instance]\n"; 

	my $has_handle = has_handle($config, $instance, $pid, $r->{hdl});
	if($has_handle > -1){
		if($has_handle){
			print "[".scalar localtime."] ", "hdl already saved, skipping\n"; 
		}else{
			if(save_handle($config, $instance, $pid, $r->{hdl})){
				emit_event($paf_dbs{$instance}, $pid, $r->{hdl}, $r->{url});	
			}
		}
	}else{
		print "[".scalar localtime."] ", "error getting object ids, skipping\n"; 
	}
	
}

exit(1);

sub has_handle {
	my $config = shift;
	my $instance = shift;
	my $pid = shift;
	my $hdl = shift;

	my $action = "/object/$pid/id";
	my $url = Mojo::URL->new;
	$url->scheme('https');
	my @base = split('/',$config->{phaidra_instances}->{$instance}->{apibaseurl});
	$url->host($base[0]);	
	if(exists($base[1])){
		$url->path($base[1].$action);
	}else{
		$url->path($action);
	}

	my $ua = Mojo::UserAgent->new;
	my $tx = $ua->get($url);
  	if (my $res = $tx->success) {
  		for my $id (@{$res->{ids}}){
  			if("hdl:$hdl" eq $id){  				
  				return 1;
  			}
  		}
    	return 0;
	}else{
		print "[".scalar localtime."] ", "ERROR searching for ids pid[$pid]:\n"; 
		if($tx->res->json){
			if($tx->res->json->{alerts}){
				print Dumper($tx->res->json->{alerts})."\n";
			}
		}
		print Dumper($tx->error)."\n"; 		
		return -1;
	}
}

sub save_handle {
	my $config = shift;
	my $instance = shift;
	my $pid = shift;
	my $hdl = shift;

	my $action = "/object/$pid/id/hdl/add?id=$hdl";
	my $url = Mojo::URL->new;
	$url->scheme('https');
	$url->userinfo($config->{phaidra_instances}->{$instance}->{intcallusername}.":".$config->{phaidra_instances}->{$instance}->{intcallpassword});
	my @base = split('/',$config->{phaidra_instances}->{$instance}->{apibaseurl});
	$url->host($base[0]);	
	if(exists($base[1])){
		$url->path($base[1].$action);
	}else{
		$url->path($action);
	}

	my $ua = Mojo::UserAgent->new;
	my $tx = $ua->post($url);
  	if (my $res = $tx->success) {
  		print "[".scalar localtime."] ", "success\n"; 
    	return 1;
	}else{
		print "[".scalar localtime."] ", "ERROR adding handle hdl[$hdl] pid[$pid]:\n"; 
		if($tx->res->json){
			if($tx->res->json->{alerts}){
				print Dumper($tx->res->json->{alerts})."\n";
			}
		}
		print Dumper($tx->error)."\n"; 		
	}
}

sub emit_event {
	my $paf_mongo = shift;
	my $pid = shift;
	my $hdl = shift;
	my $url = shift;

	$paf_mongo->get_collection('events')->insert({
		ts_iso => ts_iso(), 
		event => 'handle_save_finished',
		agent => 'phaidra_handle_save',
		e => time, 
		pid => $pid,
		hdl => $hdl,
		url => $url
	});
}

sub ts_iso {
	my @ts = localtime (time());
	sprintf ("%04d%02d%02dT%02d%02d%02d", $ts[5]+1900, $ts[4]+1, $ts[3], $ts[2], $ts[1], $ts[0]);
}