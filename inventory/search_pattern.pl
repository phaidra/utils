#!/usr/bin/perl

use warnings; 
use strict;
use Data::Dumper;
$Data::Dumper::Indent= 1;

use Config::JSON;
use DBI;

=pod

=head1 Generate thumbnails 

perl inventory.pl path/to/config/config.json

=cut


#get config data
my $pathToFile = $ARGV[0];
if(not defined $pathToFile) {
     print "Please enter path to config as a parameter. e.g: perl inventory.pl my/path/to/config/config.json";
     system ("perldoc '$0'"); exit (0); 
}

my $config = Config::JSON->new(pathToFile => $pathToFile);
$config = $config->{config};

my $instaceNumber =  $config->{phaidra_instances}->{instance_number};

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
                                
                                
                                
#connect to Fedora database                                
my $hostFedoramysql12     = $config->{phaidra_instances}->{fedoramysql12}->{host};
my $dbNameFedoramysql12   = $config->{phaidra_instances}->{fedoramysql12}->{dbName};
my $userFedoramysql12     = $config->{phaidra_instances}->{fedoramysql12}->{user};
my $passFedoramysql12     = $config->{phaidra_instances}->{fedoramysql12}->{pass};

my $dbhFedoramysql12 = DBI->connect(          
                                  "dbi:mysql:dbname=$dbNameFedoramysql12;host=$hostFedoramysql12", 
                                  $userFedoramysql12,
                                  $passFedoramysql12,
                                  { RaiseError => 1}
                                ) or die $DBI::errstr;
                                

                                
#read from Frontend Statistics database
my $sthFrontendStats = $dbhFrontendStats->prepare( "SELECT SID, last_update  FROM search_pattern" );
$sthFrontendStats->execute();
my $frontendStats;
while (my @frontendStatsDbrow = $sthFrontendStats->fetchrow_array){
    $frontendStats->{$frontendStatsDbrow[0]} = $frontendStatsDbrow[1];
}


#read from Fedora database
my $sthFedoramysql12 = $dbhFedoramysql12->prepare( "SELECT SID, last_update FROM search_pattern" );
$sthFedoramysql12->execute();
my $fedoramysql12;
while (my @fedoramysql12Dbrow = $sthFedoramysql12->fetchrow_array){
    $fedoramysql12->{$fedoramysql12Dbrow[0]} = $fedoramysql12Dbrow[1];
}


=head1

  Insert new record into Frontend statistics db

=cut

sub insertRecord($){
    
    my $sid = shift;
    print "Inserting sid:",$sid,"\n";
    my $sthFedoramysql12_insert = $dbhFedoramysql12->prepare( "SELECT * FROM search_pattern where SID=?" );
    $sthFedoramysql12_insert->execute($sid);
    while (my @fedoramysql12_insert_Dbrow = $sthFedoramysql12_insert->fetchrow_array){
          my $frontendStats_insert_query = "INSERT INTO `search_pattern` (`SID`, `idsite`, `name`, `session_id`, `pattern`, `last_update`) VALUES (?, ?, ?, ?, ?, ?);";  
          my $sthFrontendStats_insert = $dbhFrontendStats->prepare($frontendStats_insert_query);
          $sthFrontendStats_insert->execute(
                                            $fedoramysql12_insert_Dbrow[0],
                                            $instaceNumber,
                                            $fedoramysql12_insert_Dbrow[1],
                                            $fedoramysql12_insert_Dbrow[2],
                                            $fedoramysql12_insert_Dbrow[3],
                                            $fedoramysql12_insert_Dbrow[4]
                                           );
          $sthFrontendStats_insert->finish();
    }
    $sthFedoramysql12_insert->finish(); 
}

=head1

  Update record in Frontend statistics db

=cut

sub updateRecord($){
    
    my $sid = shift;
    print "Updating sid:",$sid,"\n";
    my $sthFedoramysql12_update = $dbhFedoramysql12->prepare( "SELECT * FROM search_pattern where SID=?" );
    $sthFedoramysql12_update->execute($sid);
    while (my @fedoramysql12_update_Dbrow = $sthFedoramysql12_update->fetchrow_array){
          my $frontendStats_update_query = "UPDATE search_pattern set  name=?, session_id=?, pattern=?, last_update=? where SID=?;";  
          my $sthFrontendStats_update = $dbhFrontendStats->prepare($frontendStats_update_query);
          $sthFrontendStats_update->execute(
                                            $fedoramysql12_update_Dbrow[1],
                                            $fedoramysql12_update_Dbrow[2],
                                            $fedoramysql12_update_Dbrow[3],
                                            $fedoramysql12_update_Dbrow[4],
                                            $fedoramysql12_update_Dbrow[0]
                                           );
          $sthFrontendStats_update->finish();
    }
    $sthFedoramysql12_update->finish(); 

}

=head1

  Delete record from Frontend statistics db

=cut

sub deleteRecord($){
    
    my $sid = shift;
    print "Deleting sid:",$sid,"\n";
    my $frontendStats_delete_query = "DELETE from search_pattern where SID=?;";
    my $sthFrontendStats_delete = $dbhFrontendStats->prepare($frontendStats_delete_query);
    $sthFrontendStats_delete->execute($sid);
    $sthFrontendStats_delete->finish();
}

#####################################
#######  Main  ######################
#####################################
# iterate
my $counterInsert = 0;
my $counterUpdate = 0;
my $counterDelete = 0;
foreach my $keyFedoramysql12 (keys %{$fedoramysql12}){
     if(defined $frontendStats->{$keyFedoramysql12}){
          if($frontendStats->{$keyFedoramysql12} lt $fedoramysql12->{$keyFedoramysql12}){
                updateRecord($keyFedoramysql12);
                $counterUpdate++;
          }
     }else{
          insertRecord($keyFedoramysql12);
          $counterInsert++;
     }
}

foreach my $keyfrontendStats (keys %{$frontendStats}){
      if(not defined $fedoramysql12->{$keyfrontendStats}){
          deleteRecord($keyfrontendStats); 
          $counterDelete++;
      }
}



print "search_pattern inserted:",$counterInsert,"\n";
print "search_pattern updated:",$counterUpdate,"\n";
print "search_pattern deleted:",$counterDelete,"\n";
1;