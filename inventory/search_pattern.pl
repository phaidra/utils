#!/usr/bin/perl

use warnings; 
use strict;
use Data::Dumper;
$Data::Dumper::Indent= 1;

use Config::JSON;
use DBI;

=pod

=head1 Search Pattern  

perl search_pattern.pl path/to/config/config.json 4

=cut


#get config data
my $pathToFile = $ARGV[0];
my $instanceNumber = $ARGV[1];

if(not defined $pathToFile) {
     print "Please enter path to config as first parameter. e.g: perl search_pattern.pl my/path/to/config/config.json 4";
     system ("perldoc '$0'"); exit (0); 
}

if(not defined $instanceNumber) {
     print "Please enter path to config as second parameter. e.g: perl search_pattern.pl my/path/to/config/config.json 4";
     system ("perldoc '$0'"); exit (0); 
}

my $config = Config::JSON->new(pathToFile => $pathToFile);
$config = $config->{config};

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
                                
                                
                                
#connect to phairaUsersDB database      

my @phaidraInstances = @{$config->{phaidra_instances}};

my $curentPhaidraInstance;
foreach (@phaidraInstances){
      if($_->{phaidra_instance}->{instance_number} eq $instanceNumber){
               $curentPhaidraInstance = $_->{phaidra_instance};
      }
}


my $hostPhairaUsersDB     = $curentPhaidraInstance->{phairaUsersDB}->{host};
my $dbNamePhairaUsersDB   = $curentPhaidraInstance->{phairaUsersDB}->{dbName};
my $userPhairaUsersDB     = $curentPhaidraInstance->{phairaUsersDB}->{user};
my $passPhairaUsersDB     = $curentPhaidraInstance->{phairaUsersDB}->{pass};

my $dbhPhairaUsersDB = DBI->connect(          
                                  "dbi:mysql:dbname=$dbNamePhairaUsersDB;host=$hostPhairaUsersDB", 
                                  $userPhairaUsersDB,
                                  $passPhairaUsersDB,
                                  { RaiseError => 1}
                                ) or die $DBI::errstr;
                                

                                
#read from Frontend Statistics database
my $sthFrontendStats = $dbhFrontendStats->prepare( "SELECT SID, last_update  FROM search_pattern" );
$sthFrontendStats->execute();
my $frontendStats;
while (my @frontendStatsDbrow = $sthFrontendStats->fetchrow_array){
    $frontendStats->{$frontendStatsDbrow[0]} = $frontendStatsDbrow[1];
}


#read from phairaUsersDB database 
my $sthPhairaUsersDB = $dbhPhairaUsersDB->prepare( "SELECT SID, last_update FROM search_pattern" );
$sthPhairaUsersDB->execute();
my $phairaUsersDB;
while (my @phairaUsersDBrow = $sthPhairaUsersDB->fetchrow_array){
    $phairaUsersDB->{$phairaUsersDBrow[0]} = $phairaUsersDBrow[1];
}


=head1

  Insert new record into Frontend statistics db 

=cut

sub insertRecord($){
    
    my $sid = shift;
    print "Inserting sid:",$sid,"\n";
    my $sthPhairaUsersDB_insert = $dbhPhairaUsersDB->prepare( "SELECT * FROM search_pattern where SID=?" );
    $sthPhairaUsersDB_insert->execute($sid);
    while (my @phairaUsersDB_insert_Dbrow = $sthPhairaUsersDB_insert->fetchrow_array){
          my $frontendStats_insert_query = "INSERT INTO `search_pattern` (`SID`, `idsite`, `name`, `session_id`, `pattern`, `last_update`) VALUES (?, ?, ?, ?, ?, ?);";  
          my $sthFrontendStats_insert = $dbhFrontendStats->prepare($frontendStats_insert_query);
          $sthFrontendStats_insert->execute(
                                            $phairaUsersDB_insert_Dbrow[0],
                                            $instanceNumber,
                                            $phairaUsersDB_insert_Dbrow[1],
                                            $phairaUsersDB_insert_Dbrow[2],
                                            $phairaUsersDB_insert_Dbrow[3],
                                            $phairaUsersDB_insert_Dbrow[4]
                                           );
          $sthFrontendStats_insert->finish();
    }
    $sthPhairaUsersDB_insert->finish(); 
}

=head1

  Update record in Frontend statistics db

=cut

sub updateRecord($){
    
    my $sid = shift;
    print "Updating sid:",$sid,"\n"; 
    my $sthPhairaUsersDB_update = $dbhPhairaUsersDB->prepare( "SELECT * FROM search_pattern where SID=?" );
    $sthPhairaUsersDB_update->execute($sid);
    while (my @phairaUsersDB_update_Dbrow = $sthPhairaUsersDB_update->fetchrow_array){
          my $frontendStats_update_query = "UPDATE search_pattern set  name=?, session_id=?, pattern=?, last_update=? where SID=?;";  
          my $sthFrontendStats_update = $dbhFrontendStats->prepare($frontendStats_update_query);
          $sthFrontendStats_update->execute(
                                            $phairaUsersDB_update_Dbrow[1],
                                            $phairaUsersDB_update_Dbrow[2],
                                            $phairaUsersDB_update_Dbrow[3],
                                            $phairaUsersDB_update_Dbrow[4],
                                            $phairaUsersDB_update_Dbrow[0]
                                           );
          $sthFrontendStats_update->finish();
    }
    $sthPhairaUsersDB_update->finish(); 

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
foreach my $keyPhairaUsersDB (keys %{$phairaUsersDB}){
     if(defined $frontendStats->{$keyPhairaUsersDB}){
          if($frontendStats->{$keyPhairaUsersDB} lt $phairaUsersDB->{$keyPhairaUsersDB}){
                updateRecord($keyPhairaUsersDB);
                $counterUpdate++;
          }
     }else{
          insertRecord($keyPhairaUsersDB);
          $counterInsert++;
     }
}

foreach my $keyfrontendStats (keys %{$frontendStats}){
      if(not defined $phairaUsersDB->{$keyfrontendStats}){
          deleteRecord($keyfrontendStats); 
          $counterDelete++;
      }
}



print "search_pattern inserted:",$counterInsert,"\n";
print "search_pattern updated:",$counterUpdate,"\n";
print "search_pattern deleted:",$counterDelete,"\n";
1;