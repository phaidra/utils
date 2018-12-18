#!/usr/bin/env perl
package Ubmaps;

=head1 NAME
uploadmaps.pl
=head1 DESCRIPTION
given a list of AC it 
- queries Aleph
- maps metadata to MODS 
- uploads the map from share with the mapped metadata to Phaidra
=cut

use strict;
use warnings;
use diagnostics;
use utf8;
use lib ('.');
use Mojo::UserAgent;
use Mojo::File;
use Mojo::JSON qw(from_json encode_json decode_json);
use Mojo::ByteStream qw(b);
use Data::Dumper;
use Log::Log4perl;
use Digest::MD5;
use MongoDB;
use Storable qw(dclone);
use UBMapsMab2Mods qw(mab2mods);

binmode(STDOUT, ":utf8");

$ENV{MOJO_MAX_MESSAGE_SIZE} = 20737418240;
$ENV{MOJO_INACTIVITY_TIMEOUT} = 1209600;
$ENV{MOJO_HEARTBEAT_TIMEOUT} = 1209600;
$ENV{MOJO_MAX_REDIRECTS} = 5;

#Log::Log4perl->easy_init( { level => $DEBUG, file => '>>/var/log/phaidra/ubmaps_upload.log' } ); #stdout
my $logconf = q(
  log4perl.category.Ubmaps           = DEBUG, Logfile, Screen
 
  log4perl.appender.Logfile          = Log::Log4perl::Appender::File
  log4perl.appender.Logfile.filename = /var/log/phaidra/ubmaps_upload.log
  log4perl.appender.Logfile.layout   = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Logfile.layout.ConversionPattern=%d %m%n
 
  log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
  log4perl.appender.Screen.stderr  = 0
  log4perl.appender.Screen.layout  = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Screen.layout.ConversionPattern=%d %m%n
);
 
Log::Log4perl::init( \$logconf );
my $log = Log::Log4perl::get_logger("Ubmaps");

my $configfilepath = Mojo::File->new('ubmaps_upload.json');
my $config = from_json $configfilepath->slurp;

my @pids;
my @mapping_alerts;

while (defined (my $arg= shift (@ARGV)))
{
  push @pids, $arg;
}

$log->debug("pids:\n".Dumper(\@pids));

my $datadir = $config->{ubmaps_upload}->{datadir};

my $ua = Mojo::UserAgent->new;
my $res;
my $apiurl = "https://".$config->{ubmaps_upload}->{phaidraapi_uploadusername}.":".$config->{ubmaps_upload}->{phaidraapi_uploadpassword}."\@".$config->{ubmaps_upload}->{phaidraapi_apibaseurl};

my $mongouri = "mongodb://".$config->{ubmaps_upload}->{mongo_username}.":".$config->{ubmaps_upload}->{mongo_password}."@". $config->{ubmaps_upload}->{mongo_host}."/".$config->{ubmaps_upload}->{mongo_db};
my $client = MongoDB->connect($mongouri);
my $db = $client->get_database( $config->{ubmaps_upload}->{mongo_db} );
my $alma = $db->get_collection( $config->{ubmaps_upload}->{mongo_collection} );

$log->info("started");

sub main {

  my $pidscount = scalar @pids;
  my $i = 0;
  my $res;
  foreach my $pid (@pids){
    $i++;
    $log->info("processing pid[$pid] [$i/$pidscount]");

    $res = $ua->get("$apiurl/object/$pid/index")->result;
    if($res->is_error){
      $log->error("getting index for pid[$pid] failed:\n". $res->message);
      next;
    }

    my $acnumber;
    for my $id (@{$res->json->{index}->{dc_identifier}}){
      if($id =~ /^AC(\d)+$/g){
        $acnumber = $id;
        $log->info("found ac_number[$acnumber] for pid[$pid]");
        last;
      }
    }

    unless($acnumber){
	    $log->error("failed to find ac_number for pid[$pid]");
	    next;
	  }

	  $log->info("getting metadata for ac_number[$acnumber]");
	  my $md_stat = $alma->find({ac_number => $acnumber})->sort({fetched => -1})->fields({fetched => 1, xmlref2 => 1})->next;
    
	  unless($md_stat->{xmlref2}){
	    $log->error("mapping ac_number[$acnumber] failed, no marc metadata found");
	    next;
	  }

    my $mab = $md_stat->{xmlref2};
    my $fields = $mab->{record}[0]->{metadata}[0]->{oai_marc}[0]->{varfield};

    #$log->debug("XXXXXXXXXXXXXXXXXX catalog: ".Dumper($fields));

	  $log->info("mapping marc (fetched ".get_tsISO($md_stat->{fetched}).") to mods for ac_number[$acnumber]");

	  my ($mods, $geo) = mab2mods($log, $fields, $acnumber);

    $res = $ua->post("$apiurl/mods/json2xml" => form => { metadata => b(encode_json({ metadata => { mods => $mods }}))->decode('UTF-8') })->result;
    if($res->is_success){ 
      $log->info("mapping successful");
    } elsif($res->is_error){
      $log->error("mapping failed:\n". $res->message);
      next;
    }

    my $xml = $res->json->{metadata}->{mods};
    # $log->debug("mods:\n".$xml);
    
    $res = $ua->post("$apiurl/object/$pid/metadata" => form => { metadata => b(encode_json({ metadata => { mods => $mods, geo => $geo }}))->decode('UTF-8')})->result;
    if($res->is_success){ 
      $log->info("update successful");
    } elsif($res->is_error){
      $log->error("upload failed:\n". $res->message);
    }

  }

}

sub get_tsISO {
  my $tsin = shift;
  my @ts = localtime ($tsin);
  return sprintf ("%02d.%02d.%04d %02d:%02d:%02d", $ts[3], $ts[4]+1, $ts[5]+1900, $ts[2], $ts[1], $ts[0]);
}

main();

1;