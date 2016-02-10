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

=pod

=head1 Generate thumbnails 

perl inventory.pl path/to/config/config.json

=cut


# get config data
my $pathToFile = $ARGV[0];
if(not defined $pathToFile) {
     print "Please enter path to config as a parameter. e.g: perl inventory.pl my/path/to/config/config.json";
     system ("perldoc '$0'"); exit (0); 
}

my $config = Config::JSON->new(pathToFile => $pathToFile);
$config = $config->{config};

my $instaceNumber =  $config->{phaidra_instances}->{instance_number};
my $servicesTriples =  $config->{phaidra_instances}->{services_triples};


#connect to frontend Statistics database (Hose)
my $hostFrontendStats     = $config->{phaidra_instances}->{frontendStatsMysql}->{host};
my $dbNameFrontendStats   = $config->{phaidra_instances}->{frontendStatsMysql}->{dbName};
my $userFrontendStats     = $config->{phaidra_instances}->{frontendStatsMysql}->{user};
my $passFrontendStats     = $config->{phaidra_instances}->{frontendStatsMysql}->{pass};
my $dbhFrontendStats = DBI->connect(          
                                  "dbi:mysql:dbname=$dbNameFrontendStats;host=$hostFrontendStats", 
                                  $userFrontendStats,
                                  $passFrontendStats,
                                  { RaiseError => 1}
                                ) or die $DBI::errstr;
                                
#connect to mongoDb
my $connestionString = 'mongodb://'.$config->{phaidra_instances}->{mongoDb}->{user}.':'.
                                    $config->{phaidra_instances}->{mongoDb}->{pass}.'@'.
                                    $config->{phaidra_instances}->{mongoDb}->{host}.'/'.
                                    $config->{phaidra_instances}->{mongoDb}->{dbName};
  
my $client     = MongoDB->connect($connestionString);
my $collection = $client->ns('ph001.foxml.ds');



#read data from mongoDb  
my $dataset    = $collection->find();
my $mongoDbData;
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
           my $mtime = 0;
           $mtime = strftime("%Y-%m-%d %H:%M:%S", localtime($doc->{'mtime'})) if defined $doc->{'mtime'};
     
           my $element;
           $element->{pid} = $doc->{'pid'};
           $element->{mtime} = $mtime;
           $element->{updated_at} = $updated_at;
           $element->{fs_size} = $doc->{'fs_size'};
           $element->{red_code} = $doc->{'red_code'};
           $element->{mimetype} = $doc->{'mimetype'};
           $element->{acc_code} = $doc->{'acc_code'};
           $element->{ownerId} = $doc->{'ownerId'};
           $element->{model} = $doc->{'model'};
           $element->{'state'} = $doc->{'state'};
     
           $mongoDbData->{$doc->{'pid'}} = $element;
     }
}
my $numberOfMongoDBRecords = keys %{$mongoDbData};

#read from Frontend Statistics database
my $sthFrontendStats = $dbhFrontendStats->prepare( "SELECT oid, modified  FROM  inventory" );
$sthFrontendStats->execute();
my $frontendStats;
while (my @frontendStatsDbrow = $sthFrontendStats->fetchrow_array){
    $frontendStats->{$frontendStatsDbrow[0]} = $frontendStatsDbrow[1];
}

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


=head1

  Insert new record into Frontend statistics db

=cut

sub insertRecord($){
    
    my $pid = shift;
    my $title = getTitle($pid);
    print "Inserting pid:",$pid,"\n";
    
    my $frontendStats_insert_query = "INSERT INTO `inventory` (`idsite`, `oid`, `cmodel`, `mimetype`, `owner`, `state`, `filesize`, `redcode`, `acccode`, `created`, `modified`, `title`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
    my $sthFrontendStats_insert = $dbhFrontendStats->prepare($frontendStats_insert_query);
    $sthFrontendStats_insert->execute(
                                            $instaceNumber,
                                            $pid,
                                            $mongoDbData->{$pid}->{model},
                                            $mongoDbData->{$pid}->{mimetype},
                                            $mongoDbData->{$pid}->{ownerId},
                                            $mongoDbData->{$pid}->{'state'},
                                            $mongoDbData->{$pid}->{fs_size},
                                            $mongoDbData->{$pid}->{red_code},
                                            $mongoDbData->{$pid}->{acc_code},
                                            $mongoDbData->{$pid}->{mtime},
                                            $mongoDbData->{$pid}->{updated_at},
                                            $title
                                     );
    if ( $sthFrontendStats_insert->err ){
            print "DBI ERROR! pid:$pid: $sthFrontendStats_insert->err : $sthFrontendStats_insert->errstr \n";
    }
    $sthFrontendStats_insert->finish();
}

=head1

  Update record in Frontend statistics db

=cut

sub updateRecord($){
    
    my $pid = shift;
    my $title = getTitle($pid);
    
    print "Updating pid:",$pid,"\n";
    my $frontendStats_update_query = "UPDATE inventory set  cmodel=?, mimetype=?, owner=?, state=?, filesize=?, redcode=?, acccode=?, created=?, modified=?, title=? where oid=?;";  
    my $sthFrontendStats_update = $dbhFrontendStats->prepare($frontendStats_update_query);
    
    $sthFrontendStats_update->execute(
                                            $mongoDbData->{$pid}->{model},
                                            $mongoDbData->{$pid}->{mimetype},
                                            $mongoDbData->{$pid}->{ownerId},
                                            $mongoDbData->{$pid}->{'state'},
                                            $mongoDbData->{$pid}->{fs_size},
                                            $mongoDbData->{$pid}->{red_code},
                                            $mongoDbData->{$pid}->{acc_code},
                                            $mongoDbData->{$pid}->{mtime},
                                            $mongoDbData->{$pid}->{updated_at},
                                            $title,
                                            $pid
                                           );
    if ( $sthFrontendStats_update->err ){
            print "DBI ERROR! pid:$pid : $sthFrontendStats_update->err : $sthFrontendStats_update->errstr \n";
    }
    $sthFrontendStats_update->finish();
}

=head1

  Delete record from Frontend statistics db

=cut

sub deleteRecord($){
    
    my $pid = shift;
    print "Deleting pid:",$pid,"\n";
    my $frontendStats_delete_query = "DELETE from inventory where oid=?;";
    my $sthFrontendStats_delete = $dbhFrontendStats->prepare($frontendStats_delete_query);
    $sthFrontendStats_delete->execute($pid);
    $sthFrontendStats_delete->finish();
    if ( $sthFrontendStats_delete->err ){
            print "DBI ERROR! pid:$pid: $sthFrontendStats_delete->err : $sthFrontendStats_delete->errstr \n";
    }
}



#####################################
#######  Main  ######################
#####################################
# iterate
my $counterInsert = 0;
my $counterUpdate = 0;
my $counterNoUpdate = 0;
my $counterDelete = 0;
foreach my $keyMongoDbData (keys %{$mongoDbData}){
   #if(defined $frontendStats->{$keyMongoDbData} && $frontendStats->{$keyMongoDbData}->{model} ne 'Container'){
        if(defined $frontendStats->{$keyMongoDbData}){
            if($frontendStats->{$keyMongoDbData} lt $mongoDbData->{$keyMongoDbData}->{updated_at}){
                updateRecord($keyMongoDbData);
                $counterUpdate++;
            }else{
              $counterNoUpdate++;
            }
        }else{
            insertRecord($keyMongoDbData);
            $counterInsert++;
        }
  #}
}

foreach my $keyfrontendStats (keys %{$frontendStats}){
      if(not defined $mongoDbData->{$keyfrontendStats}){
          deleteRecord($keyfrontendStats); 
          $counterDelete++;
      }
}

print "search_pattern inserted:",$counterInsert,"\n";
print "search_pattern updated:",$counterUpdate,"\n";
print "search_pattern deleted:",$counterDelete,"\n";
print "search_pattern no updated:",$counterNoUpdate,"\n";


1;