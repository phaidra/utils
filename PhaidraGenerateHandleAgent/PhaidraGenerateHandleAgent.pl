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
#use Mojo::URL;
use Log::Log4perl qw(:easy);

$ENV{MOJO_INACTIVITY_TIMEOUT} = 36000;

Log::Log4perl->easy_init( { level => $DEBUG, layout => "[%d{dd-MM-yyyy HH:mm:ss}] [%p] %m%n", file => ">>/var/log/phaidra/PhaidraGenerateHandleAgent.log" } );

my $configpath = 'PhaidraGenerateHandleAgent.json';
unless(-f $configpath){
	ERROR("Error: config path $configpath is not a file (or file does not exist). Usually $configpath would be a link to /etc/phaidra/...");
	exit (0);
}
unless(-r $configpath){
	ERROR("Error: cannot access config: $configpath");
	exit (0);
}

my $configfile = Mojo::File->new($configpath);

my $bytes = $configfile->slurp;
my $config = decode_json($bytes);

my $ua = Mojo::UserAgent->new;
my $apiurl = "https://".$config->{phaidraapi_adminusername}.":".$config->{phaidraapi_adminpassword}."\@".$config->{phaidraapi_baseurl};

my $since;
my @irma_map_ids;
my $job_col;

while (defined (my $arg= shift (@ARGV)))
{
  if ($arg =~ /^-/)
  {
  	   if ($arg eq '-since') { $since = shift (@ARGV); }
    else { exit (0); }
  }
}

unless(defined($since)){
	ERROR("Error: Missing parameters.");
	exit (0);
}

my $mongodbConnectTimeoutMs = 300000;
my $mongodbSocketTimeoutMs  = 300000;

my $irma_mongo = MongoDB::MongoClient->new(host => $config->{irma_mongodb}->{host}, username => $config->{irma_mongodb}->{username}, password => $config->{irma_mongodb}->{password}, db_name => $config->{irma_mongodb}->{database}, connect_timeout_ms => $mongodbConnectTimeoutMs, socket_timeout_ms => $mongodbSocketTimeoutMs)->get_database($config->{irma_mongodb}->{database});
my $paf_mongo = MongoDB::MongoClient->new(host => $config->{paf_mongodb}->{host}, username => $config->{paf_mongodb}->{username}, password => $config->{paf_mongodb}->{password}, db_name => $config->{paf_mongodb}->{database}, connect_timeout_ms => $mongodbConnectTimeoutMs, socket_timeout_ms => $mongodbSocketTimeoutMs)->get_database($config->{paf_mongodb}->{database});

# search for events of object state changed to A
# example: 
# { "_id" : ObjectId("5758244c472bdd34895dc02e"), "e" : 1465394252, "procid" : 13449, "pid" : "o:62571", "agent" : "logscan", "event" : "PID_state", "state" : "A" }
my @records;


if ($since eq 'yesterday') { $since= time()-86400; }
elsif ($since =~ m#(\d+) days#) { my $days= $1; $since= time()-86400*$days; }
elsif ($since =~ m#(\d+) hours#) { my $hours= $1; $since= time()-3600*$hours; }

my $find= {};
if ($since > 0) { $find->{'e'}= {'$gte' => $since }; }
$find->{'event'}= 'PID_state';
$find->{'state'}= 'A';

INFO("processing PID_state => A events since $since [".strftime("%m/%d/%Y %H:%M:%S",localtime($since))."]"); 
my $recs = $paf_mongo->get_collection('events')->find($find);
while (my $rec = $recs->next) {	
 push @records, $rec if defined $rec;
}	

# process the records
my $rec_cnt = scalar @records;
INFO("found $rec_cnt records"); 

my $i = 0;
for my $r (@records){
  $i++;

  INFO("processing [$i/$rec_cnt]");

  unless($r->{pid}){
    WARN("skipping, empty pid");
    next;
  }

  my $pid = $r->{pid};

  my ($pid_stripped) = $pid =~ /o:(\d+)/; # strip pid prefix 'o:'
  my $hdl = $config->{hdl_prefix}."/".$config->{instance_hdl_prefix}.".".$pid_stripped;
#  my $hdl = $config->{hdl_prefix}."/".$config->{instance_hdl_prefix}.".".$r->{pid};
  my $url = $config->{instance_url_prefix}.$r->{pid};

  my $found = $irma_mongo->get_collection('irma.map')->find_one({hdl => $hdl, url => $url});
  if(defined($found) && exists($found->{hdl})){
    INFO("skipping, ".$found->{hdl}." already in irma.map"); 
  }else{ 
    # padova: skip object if it has cmodel Page
    if(exists($config->{skip_page_objects}) && $config->{skip_page_objects} eq 1){
      my $res = $ua->get("$apiurl/object/$pid/cmodel")->result;
      if ($res->code == 200) {
        if($res->json->{cmodel} && $res->json->{cmodel} eq 'Page'){
          DEBUG("skipping, pid=[$pid] has cmodel Page");
          next;
        }
      }else{
        ERROR("getting cmodel of pid[$pid] ".Dumper($res->error));
        next;
      }
    }
    INFO("inserting url=[$url] hdl=[$hdl]");
    $irma_mongo->get_collection('irma.map')->insert_one(
      {
        ts_iso => ts_iso(),
        _created => time,
        _updated => time,
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
