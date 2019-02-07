#!/usr/bin/env perl

=pod

=head1 pidgen.pl -mongohost -mongouser -mongopass -mongodb -vocabulary -preflabelen -exactmatch

  Genereates a new (unused) identifier in the XXXX-XXXX form

  -mongohost - mongo host
  -mongouser - mongo user
  -mongopass - mongo pass
  -mongodb - identifier db (identifies pid type: pid.phaidra.org, pid.univie.ac.at, pid.phaidra.at)
  -vocabulary - vocabulary this identifier should belong to (mongo collection)
  -preflabelen - preffered label in english
  -exactmatch - exact match if available
=cut

use strict;
use warnings;
use utf8;
use Data::Dumper;
use Log::Log4perl;
use Encode::Base32::Crockford;
use MongoDB;
use POSIX qw(strftime);

$ENV{MOJO_MAX_MESSAGE_SIZE} = 20737418240;
$ENV{MOJO_INACTIVITY_TIMEOUT} = 1209600;
$ENV{MOJO_HEARTBEAT_TIMEOUT} = 1209600;
$ENV{MOJO_MAX_REDIRECTS} = 5;

my $logconf = q(
  log4perl.category.Pidgen           = DEBUG, Logfile, Screen
 
  log4perl.appender.Logfile          = Log::Log4perl::Appender::File
  log4perl.appender.Logfile.filename = /var/log/phaidra/pidgen.log
  log4perl.appender.Logfile.layout   = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Logfile.layout.ConversionPattern=%d %m%n
 
  log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
  log4perl.appender.Screen.stderr  = 0
  log4perl.appender.Screen.layout  = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Screen.layout.ConversionPattern=%d %m%n
);

Log::Log4perl::init( \$logconf );
my $log = Log::Log4perl::get_logger("Pidgen");

my $mongohost;
my $mongouser;
my $mongopass;
my $mongodb;
my $vocabulary;
my $preflabel_en;
my $exactmatch;

while (defined (my $arg= shift (@ARGV)))
{
  if ($arg =~ /^-/)
  {
  	   if ($arg eq '-mongohost') { $mongohost = shift (@ARGV); }
  	elsif ($arg eq '-mongouser') { $mongouser = shift (@ARGV); }
    elsif ($arg eq '-mongopass') { $mongopass = shift (@ARGV); }
    elsif ($arg eq '-mongodb') { $mongodb = shift (@ARGV); }
    elsif ($arg eq '-vocabulary') { $vocabulary = shift (@ARGV); }
    elsif ($arg eq '-preflabelen') { $preflabel_en = shift (@ARGV); }
    elsif ($arg eq '-exactmatch') { $exactmatch = shift (@ARGV); }
    else { system ("perldoc '$0'"); exit (0); }
  }
}

unless(
  defined ($mongohost) || 
  defined ($mongouser) || 
  defined ($mongopass) || 
  defined ($mongodb) || 
  defined ($vocabulary) || 
  defined ($preflabel_en)){
	print __LINE__, " [".scalar localtime."] ", "Error: Missing parameters.\n";
	system ("perldoc '$0'"); exit (0);
}

my $mongouri = "mongodb://".$mongouser.":".$mongopass."@". $mongohost."/".$mongodb;
my $client = MongoDB->connect($mongouri);
my $db = $client->get_database($mongodb);
my $voccollection = $db->get_collection($vocabulary);

sub ts_iso {
	my @ts = localtime (time());
	sprintf ("%04d%02d%02dT%02d%02d%02d", $ts[5]+1900, $ts[4]+1, $ts[3], $ts[2], $ts[1], $ts[0]);
}

sub getpid
{
  my $tries = 10;
  my $str;
  my $pid;
  do
  {
    my $num = rand(33285996544) + 1073741824;
    # my $num = rand(33286544) + 107824;
    
    $str = Encode::Base32::Crockford::base32_encode_with_checksum($num);
    #printf ("%12d %s\n", $num, $str);

    if ($str =~ m#^([A-Z0-9]{4})([A-Z0-9]{4})$#) {
      $pid = join ('-', $1, $2);

      my $samepid = $voccollection->find({pid => $pid})->next;
      if ($samepid) {
        $log->error("pid already exists: ".Dumper($samepid));
      } else {
        
        my $samelabel = $voccollection->find({prefLabel_en => $preflabel_en})->next;
        if ($samelabel) {
          $log->error("term with the same EN prefLabel already exists: label found: \n".Dumper($samelabel));
          exit 1;
        }
        if ($exactmatch) {
          my $sameexactmatch = $voccollection->find({exactMatch => $exactmatch})->next;
          if ($sameexactmatch) {
            $log->error("term with the same exactMatch already exists: exactMatch found: \n".Dumper($sameexactmatch));
            exit 1;
          }
        }
        
        $log->info("generated new pid $pid");
        my $ns;
        
        if($mongodb eq 'pid-phaidra-org'){
          $ns = "https://pid.phaidra.org/vocabulary/$vocabulary";
        } else {
          die ('not implemented');
        }

        $voccollection->insert_one({
          ts_iso => ts_iso(), 
          e => time,
          pid => $pid,
          exactMatch => $exactmatch,
          prefLabel_en => $preflabel_en
        });

        if ($exactmatch) {
          $log->debug("\n".'{ \'@id\': \''.$ns.'/'.$pid.'\', \'skos:exactMatch\': \''.$exactmatch.'\', \'skos:prefLabel\': { \'eng\': \''.$preflabel_en.'\' } },');
        } else {
          $log->debug("\n".'{ \'@id\': \''.$ns.'/'.$pid.'\', \'skos:prefLabel\': { \'eng\': \''.$preflabel_en.'\' } },');
        }

        exit 0;
      }
    }

  } while ($tries-- > 0);

  die('failed to generate pid');
  
}

print getpid();

1;