#!/usr/bin/env perl

use warnings; 
use strict;
use Data::Dumper;
$Data::Dumper::Indent= 1;

use Config::JSON;
use DBI;
use MongoDB;
use LWP::UserAgent;
use POSIX;
use POSIX qw( strftime );
use JSON;

use Benchmark;
use Time::HiRes qw/tv_interval gettimeofday/; 

=pod

=head1 Generate thumbnails 

usage:
inventory.pl path_to_config instance_number
e.g. perl inventory.pl path/to/config/config.json 4

=cut

my @pidWithTitleProblem;

# get config data
my $pathToFile = $ARGV[0];
my $instanceNumber = $ARGV[1];

if(not defined $pathToFile) {
     print "Please enter path to config as first parameter. e.g: perl inventory.pl my/path/to/config/config.json 4";
     system ("perldoc '$0'"); exit (0); 
}
if(not defined $instanceNumber) {
     print "Please enter path to config as second parameter. e.g: perl inventory.pl my/path/to/config/config.json 4";
     system ("perldoc '$0'"); exit (0); 
}

my $config = Config::JSON->new(pathToFile => $pathToFile);
$config = $config->{config};

my @phaidraInstances = @{$config->{phaidra_instances}};

my $curentPhaidraInstance;
foreach (@phaidraInstances){
      if($_->{phaidra_instance}->{instance_number} eq $instanceNumber){
               $curentPhaidraInstance = $_->{phaidra_instance};
      }
}

my $servicesTriples =  $curentPhaidraInstance->{services_triples};


#connect to frontend Statistics database (Hose)
my $hostFrontendStats     = $config->{frontendStatsMysql}->{host};
my $dbNameFrontendStats   = $config->{frontendStatsMysql}->{dbName};
my $userFrontendStats     = $config->{frontendStatsMysql}->{user};
my $passFrontendStats     = $config->{frontendStatsMysql}->{pass};
my $dbhFrontendStats = DBI->connect(          
                                  "dbi:mysql:dbname=$dbNameFrontendStats;host=$hostFrontendStats", 
                                  $userFrontendStats,
                                  $passFrontendStats,
                                  { RaiseError => 1}
                                ) or die $DBI::errstr;
                                
#connect to mongoDb
my $connestionString = 'mongodb://'.$curentPhaidraInstance->{mongoDb}->{user}.':'.
                                    $curentPhaidraInstance->{mongoDb}->{pass}.'@'.
                                    $curentPhaidraInstance->{mongoDb}->{host}.'/'.
                                    $curentPhaidraInstance->{mongoDb}->{dbName};
my $client     = MongoDB->connect($connestionString);
my $collection = $client->ns('ph001.foxml.ds');




=head1

  Get title from triplestore

=cut

sub getTitle($){

     my $pid = shift;
     my $ua = LWP::UserAgent->new(ssl_opts => { verify_hostname => 1 });
     
     $ua->agent("$0/0.1 " . $ua->agent);
 
     my $req = HTTP::Request->new(
           GET => "$servicesTriples?q=<info:fedora/".$pid."> <http://purl.org/dc/elements/1.1/title> *");
     $req->header('Accept' => 'application/json');

     my $res = $ua->request($req);
 
     my $json;
     if ($res->is_success) {
        $json = $res->decoded_content;
        $json  = decode_json $json;
     }
     else {
        print "Error: " . $res->status_line . "\n";
        push @pidWithTitleProblem, $pid;
     }


     my $title;
        for my $triple (@{$json->{result}}){
             $title = @$triple[2];
             $title =~ m/\"(.+)\"/;
             $title = $1;
             last;
      }
     $title = '' if not defined $title;

     return $title;
}

#####################################
#######  Main  ######################
#####################################


#get record with latest time from Frontend Statistics database
my $latestTimeFrontendStats = 0;
my $sthFrontendStats = $dbhFrontendStats->prepare( "SELECT UNIX_TIMESTAMP(ts) FROM  inventory ORDER BY ts DESC LIMIT 1;" );
$sthFrontendStats->execute();
while (my @frontendStatsDbrow = $sthFrontendStats->fetchrow_array){
    $latestTimeFrontendStats =  $frontendStatsDbrow[0];
}
# because mongodb connector requires explicitly int type !
print "time:", $latestTimeFrontendStats, "\n";
$latestTimeFrontendStats = $latestTimeFrontendStats + 1 - 1;


$dbhFrontendStats->{AutoCommit} = 0;  # enable transactions
$dbhFrontendStats->{RaiseError} = 1;
eval {
         my $frontendStats_upsert_query = "INSERT INTO `inventory`     (
                                                                 `idsite`, 
                                                                 `oid`, 
                                                                 `cmodel`, 
                                                                 `mimetype`, 
                                                                 `owner`, 
                                                                 `state`, 
                                                                 `filesize`, 
                                                                 `redcode`, 
                                                                 `acccode`, 
                                                                 `ts`, 
                                                                 `created`, 
                                                                 `modified`, 
                                                                 `title`
                                                              )
                                                     values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                                     on duplicate key update
                                                                        idsite = values(idsite),
                                                                        oid = values(oid),
                                                                        cmodel = values(cmodel),
                                                                        mimetype = values(mimetype),
                                                                        owner = values(owner),
                                                                        state = values(state),
                                                                        filesize = values(filesize),
                                                                        redcode = values(redcode),
                                                                        acccode = values(acccode),
                                                                        ts = values(ts),
                                                                        created = values(created),
                                                                        modified = values(modified),
                                                                        title = values(title)
                                                      ";
          my $sthFrontendStats_upsert = $dbhFrontendStats->prepare($frontendStats_upsert_query);


          #read data from mongoDb, only newer then $latestTimeFrontendStats
          #my $dataset    = $collection->query({ updated_at => { '$gte' => $latestTimeFrontendStats } })->sort( { updated_at => 1 } );
          my $dataset    = $collection->query({ updated_at => { '$gte' => $latestTimeFrontendStats } });
          
          my $counterUpsert = 0;

          while (my $doc = $dataset->next){
             if(
                $doc->{'model'} eq 'Picture' or
                $doc->{'model'} eq 'PDFDocument' or
                $doc->{'model'} eq 'Container' or 
                $doc->{'model'} eq 'Resource' or 
                $doc->{'model'} eq 'Collection' or 
                $doc->{'model'} eq 'Asset' or 
                $doc->{'model'} eq 'Video' or 
                $doc->{'model'} eq 'Audio' or 
                $doc->{'model'} eq 'LaTeXDocument' or 
                $doc->{'model'} eq 'Page' or 
                $doc->{'model'} eq 'Book' or 
                $doc->{'model'} eq 'Paper'
              ){
                  my $updated_at = 0;
                  $updated_at = strftime("%Y-%m-%d %H:%M:%S", localtime($doc->{'updated_at'})) if defined $doc->{'updated_at'};
          
                  print "Upserting $doc->{'pid'}... Record's 'updated_at' :",$updated_at,"\n";
           
                  my $title = "";
                  $title = getTitle($doc->{'pid'}) if defined $doc->{'pid'};
       
                  $sthFrontendStats_upsert->execute(
                                            $instanceNumber,
                                            $doc->{'pid'},
                                            $doc->{'model'},
                                            $doc->{'mimetype'},
                                            $doc->{'ownerId'},
                                            $doc->{'state'},
                                            $doc->{'fs_size'},
                                            $doc->{'red_code'},
                                            $doc->{'acc_code'},
                                            $updated_at,                #time when mongoDB record is updated ts
                                            $doc->{'createdDate'},      #time when fedora object is created taken from foxml
                                            $doc->{'lastModifiedDate'}, #time when fedora object is modified taken from foxml
                                            $title
                                           );
                      print "Error upserting record with PID $doc->{'pid'} :", $sthFrontendStats_upsert->errstr, "\n" if $sthFrontendStats_upsert->errstr;
                   $counterUpsert++;
               }else{
                      print "Object $doc->{'pid'} not upserted. Wrong model: $doc->{'model'} !";
               }
      }
       
      $dbhFrontendStats->commit;
      print "inventory upserted:",$counterUpsert,"\n";
};
if ($@) {
      print "Transaction aborted because $@";
      eval { $dbhFrontendStats->rollback };
          if ($@) {
               print "Rollback aborted because $@";
          }
}

$dbhFrontendStats->disconnect();

1;