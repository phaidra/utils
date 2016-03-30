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
                                
                                
                                
#connect to phaidraUsersDB database      
my @phaidraInstances = @{$config->{phaidra_instances}};

my $currentPhaidraInstance;
foreach (@phaidraInstances){
      if($_->{instance_number} eq $instanceNumber){
               $currentPhaidraInstance = $_;
      }
}


my $hostPhairaUsersDB     = $currentPhaidraInstance->{phaidraUsersDB}->{host};
my $dbNamePhairaUsersDB   = $currentPhaidraInstance->{phaidraUsersDB}->{dbName};
my $userPhairaUsersDB     = $currentPhaidraInstance->{phaidraUsersDB}->{user};
my $passPhairaUsersDB     = $currentPhaidraInstance->{phaidraUsersDB}->{pass};

my $dbhPhairaUsersDB = DBI->connect(          
                                  "dbi:mysql:dbname=$dbNamePhairaUsersDB;host=$hostPhairaUsersDB", 
                                  $userPhairaUsersDB,
                                  $passPhairaUsersDB,
                                  { RaiseError => 1}
                                ) or die $DBI::errstr;
                                

                                
#get record with latest time from Frontend Statistics database
my $latestTimeFrontendStats = 0;
my $sthFrontendStats = $dbhFrontendStats->prepare( "SELECT last_update FROM  search_pattern ORDER BY last_update DESC LIMIT 1;" );
$sthFrontendStats->execute();
while (my @frontendStatsDbrow = $sthFrontendStats->fetchrow_array){
    $latestTimeFrontendStats =  $frontendStatsDbrow[0];
} 
print "latestTimeFrontendStats:\n",$latestTimeFrontendStats,"\n";                                
#exit;   
#read PhaidraUser database with newer or equal $latestTimeFrontendStats and upsert new records to Frontend Statistics database
my $sthPhairaUsersDB = $dbhPhairaUsersDB->prepare( "SELECT * FROM search_pattern where last_update >= \"$latestTimeFrontendStats\" ORDER BY last_update ASC" );
$sthPhairaUsersDB->execute();
my $counterUpsert;
my $frontendStats_upsert_query = "INSERT INTO `search_pattern` (`SID`, `idsite`, `name`, `session_id`, `pattern`, `last_update`)
                                                     values(?, ?, ?, ?, ?, ?) 
                                                     on duplicate key update
                                                                        idsite = values(idsite),
                                                                        name = values(name),
                                                                        session_id = values(session_id),
                                                                        pattern = values(pattern),
                                                                        last_update = values(last_update)
                                                      ";
my $sthFrontendStats_upsert = $dbhFrontendStats->prepare($frontendStats_upsert_query);

while (my @phairaUsers_upsert_Dbrow = $sthPhairaUsersDB->fetchrow_array){
      print "Upserting SID:",$phairaUsers_upsert_Dbrow[0],"\n";
      print "Upserting SID time:",$phairaUsers_upsert_Dbrow[4],"\n";

      $sthFrontendStats_upsert->execute(
                                            $phairaUsers_upsert_Dbrow[0],
                                            $instanceNumber,
                                            $phairaUsers_upsert_Dbrow[1],
                                            $phairaUsers_upsert_Dbrow[2],
                                            $phairaUsers_upsert_Dbrow[3],
                                            $phairaUsers_upsert_Dbrow[4]
                                           );
     print "error writing record with SID: $phairaUsers_upsert_Dbrow[0] .", $dbhFrontendStats->errstr, "\n" if $dbhFrontendStats->errstr;
     

    $counterUpsert++;
}


$dbhPhairaUsersDB->disconnect();
$dbhFrontendStats->disconnect();

print "search_pattern upsert:",$counterUpsert,"\n";
   

1;