#!/usr/bin/env perl

=pod

=head1 PhaidraGenerateHandleAgent.pl (-since sinceepoch)

  -since sinceepoch - check events database for records since this date

=cut

use strict;
use warnings;
use Data::Dumper;
$Data::Dumper::Indent= 1;
use POSIX qw(strftime);
use MongoDB;
use Mojo::JSON qw(encode_json decode_json);
use Mojo::File;
use Mojo::UserAgent;
use Mojo::URL;

$ENV{MOJO_INACTIVITY_TIMEOUT} = 36000;

my $configpath = 'PhaidraGenerateHandleAgent.json';
unless(-f $configpath){
	print "[".scalar localtime."] ", "Error: config path $configpath is not a file (or file does not exist). Usually $configpath would be a link to /etc/phaidra/...\n";
	system ("perldoc '$0'"); exit (0);
}
unless(-r $configpath){
	print "[".scalar localtime."] ", "Error: cannot access config: $configpath\n";
	system ("perldoc '$0'"); exit (0);
}

my $configfile = Mojo::File->new($configpath);

my $bytes = $configfile->slurp;
my $config = decode_json($bytes);

my $since;
my @irma_map_ids;
my $job_col;

while (defined (my $arg= shift (@ARGV)))
{
  if ($arg =~ /^-/)
  {
  	   if ($arg eq '-since') { $since = shift (@ARGV); }
    else { system ("perldoc '$0'"); exit (0); }
  }
}

unless(defined($since)){
	print "[".scalar localtime."] ", "Error: Missing parameters.\n";
	system ("perldoc '$0'"); exit (0);
}


my $irma_mongo = MongoDB::MongoClient->new(host => $config->{irma_mongodb}->{host}, username => $config->{irma_mongodb}->{username}, password => $config->{irma_mongodb}->{password}, db_name => $config->{irma_mongodb}->{database})->get_database($config->{irma_mongodb}->{database});
my $paf_mongo = MongoDB::MongoClient->new(host => $config->{paf_mongodb}->{host}, username => $config->{paf_mongodb}->{username}, password => $config->{paf_mongodb}->{password}, db_name => $config->{paf_mongodb}->{database})->get_database($config->{paf_mongodb}->{database});

# search for events of object state changed to A
# example: 
# { "_id" : ObjectId("5758244c472bdd34895dc02e"), "e" : 1465394252, "procid" : 13449, "pid" : "o:62571", "agent" : "logscan", "event" : "PID_state", "state" : "A" }
my @records;


if ($since eq 'yesterday') { $since= time()-86400; }
elsif ($since =~ m#(\d+) days#) { my $days= $1; $since= time()-86400*$days; }

my $find= {};
if ($since > 0) { $find->{'e'}= {'$gt' => $since }; }
$find->{'event'}= 'PID_state';
$find->{'state'}= 'A';

print "[".scalar localtime."] ", "processing PID_state => A events since $since [".strftime("%m/%d/%Y %H:%M:%S",localtime($since))."]\n";#.Dumper($find); 
my $recs = $paf_mongo->get_collection('events')->find($find);
while (my $rec = $recs->next) {	
 push @records, $rec if defined $rec;
}	

# process the records
my $rec_cnt = scalar @records;
print "[".scalar localtime."] ", "found $rec_cnt records\n"; 

my $i = 0;
for my $r (@records){
  $i++;

  print "[".scalar localtime."] ", "processing [$i/$rec_cnt]";

  unless($r->{pid}){
    print "[".scalar localtime."] ", "skipping, empty pid\n"; 
  }

  my $hdl = $config->{hdl_prefix}."/".$config->{instance_hdl_prefix}.".".$r->{pid};
  my $url = $config->{instance_url_prefix}.$r->{pid};

  my $found = $irma_mongo->get_collection('irma.map')->find_one({hdl => $hdl, url => $url});
  if(defined($found) && exists($found->{hdl})){
      print "[".scalar localtime."] ", "skipping, ".$found->{hdl}." already in irma.map\n"; 
  }else{      
    print "[".scalar localtime."] ", "inserting url=[$url] hdl=[$hdl]\n"; 
    $irma_mongo->get_collection('irma.map')->insert(
      {
        ts_iso => ts_iso(), 
        _created => time, 
        hdl => $hdl,
        url => $url
      }
    ); 	
  }		
}


exit(1);

sub ts_iso {
	my @ts = localtime (time());
	sprintf ("%04d%02d%02dT%02d%02d%02d", $ts[5]+1900, $ts[4]+1, $ts[3], $ts[2], $ts[1], $ts[0]);
}
