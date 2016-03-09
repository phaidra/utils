#!/usr/bin/perl

use warnings; 
use strict;
use Data::Dumper;
$Data::Dumper::Indent= 1;

use Config::JSON;
use DBI;



=pod

=head1 updating piwik_log_link_visit_action

perl piwik_log_link_visit_action.pl path/to/config/config.json

=cut


#get config data
my $pathToFile = $ARGV[0];
if(not defined $pathToFile) {
     print "Please enter path to config as a parameter. e.g: perl piwik_log_link_visit_action.pl my/path/to/config/config.json";
     system ("perldoc '$0'"); exit (0); 
}

my $config = Config::JSON->new(pathToFile => $pathToFile);
$config = $config->{config};


#connect to Frontend statistics database (Hose)
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
                                
                                
                                
#connect to Piwik database                                
my $hostPiwik   = $config->{piwikMysql}->{host};
my $dbNamePiwik  = $config->{piwikMysql}->{dbName};
my $userPiwik     = $config->{piwikMysql}->{user};
my $passPiwik     = $config->{piwikMysql}->{pass};

my $dbhPiwik = DBI->connect(          
                                  "dbi:mysql:dbname=$dbNamePiwik;host=$hostPiwik", 
                                  $userPiwik,
                                  $passPiwik,
                                  { RaiseError => 1}
                           ) or die $DBI::errstr;
                                

                                
#get last record from Frontend Statistics database
my $lastRecordTimeFrontendStats = 0;
my $sthFrontendStats = $dbhFrontendStats->prepare( "SELECT server_time FROM  piwik_log_link_visit_action ORDER BY idlink_va DESC LIMIT 1;" );
$sthFrontendStats->execute();
while (my @frontendStatsDbrow = $sthFrontendStats->fetchrow_array){
    $lastRecordTimeFrontendStats =  $frontendStatsDbrow[0];
}
                                
#read Piwik database newer or equal then last record from Frontend Statistics database and upsert new records to Frontend Statistics database
my $sthPiwik = $dbhPiwik->prepare( "SELECT * FROM piwik_log_link_visit_action where server_time >= \"$lastRecordTimeFrontendStats\" ORDER BY idlink_va ASC" );
$sthPiwik->execute();
my $counterUpsert;
my $frontendStats_upsert_query = "INSERT INTO `piwik_log_link_visit_action` (
                                                                           `idlink_va`,
                                                                           `idsite`,
                                                                           `idvisitor`,
                                                                           `server_time`,
                                                                           `idvisit`,
                                                                           `idaction_url`,
                                                                           `idaction_url_ref`,
                                                                           `idaction_name`,
                                                                           `idaction_name_ref`,
                                                                           `idaction_event_category`,
                                                                           `idaction_event_action`,
                                                                           `time_spent_ref_action`,
                                                                           `custom_var_k1`,
                                                                           `custom_var_v1`,
                                                                           `custom_var_k2`,
                                                                           `custom_var_v2`,
                                                                           `custom_var_k3`,
                                                                           `custom_var_v3`,
                                                                           `custom_var_k4`,
                                                                           `custom_var_v4`,
                                                                           `custom_var_k5`,
                                                                           `custom_var_v5`,
                                                                           `custom_float`
                                                                            )
                                                     values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
                                                     on duplicate key update
                                                                        idsite = values(idsite),
                                                                        idvisitor = values(idvisitor),
                                                                        server_time = values(server_time),
                                                                        idvisit = values(idvisit),
                                                                        idaction_url = values(idaction_url),
                                                                        idaction_url_ref = values(idaction_url_ref),
                                                                        idaction_name = values(idaction_name),
                                                                        idaction_name_ref = values(idaction_name_ref),
                                                                        idaction_event_category = values(idaction_event_category),
                                                                        idaction_event_action = values(idaction_event_action),
                                                                        time_spent_ref_action = values(time_spent_ref_action),
                                                                        custom_var_k1 = values(custom_var_k1),
                                                                        custom_var_v1 = values(custom_var_v1),
                                                                        custom_var_k2 = values(custom_var_k2),
                                                                        custom_var_v2 = values(custom_var_v2),
                                                                        custom_var_k3 = values(custom_var_k3),
                                                                        custom_var_v3 = values(custom_var_v3),
                                                                        custom_var_k4 = values(custom_var_k4),
                                                                        custom_var_v4 = values(custom_var_v4),
                                                                        custom_var_k5 = values(custom_var_k5),
                                                                        custom_var_v5 = values(custom_var_v5),
                                                                        custom_float = values(custom_float)
                                                      ";
my $sthFrontendStats_upsert = $dbhFrontendStats->prepare($frontendStats_upsert_query);

while (my @piwik_upsert_Dbrow = $sthPiwik->fetchrow_array){
      
      print "Upserting idvisit:",$piwik_upsert_Dbrow[0],"\n";
      print "Upserting idvisit time:",$piwik_upsert_Dbrow[3],"\n";

      $sthFrontendStats_upsert->execute(
                                            $piwik_upsert_Dbrow[0],
                                            $piwik_upsert_Dbrow[1],
                                            $piwik_upsert_Dbrow[2],
                                            $piwik_upsert_Dbrow[3],
                                            $piwik_upsert_Dbrow[4],
                                            $piwik_upsert_Dbrow[5],
                                            $piwik_upsert_Dbrow[6],
                                            $piwik_upsert_Dbrow[7],
                                            $piwik_upsert_Dbrow[8],
                                            $piwik_upsert_Dbrow[9],
                                            $piwik_upsert_Dbrow[10],
                                            $piwik_upsert_Dbrow[11],
                                            $piwik_upsert_Dbrow[12],
                                            $piwik_upsert_Dbrow[13],
                                            $piwik_upsert_Dbrow[14],
                                            $piwik_upsert_Dbrow[15],
                                            $piwik_upsert_Dbrow[16],
                                            $piwik_upsert_Dbrow[17],
                                            $piwik_upsert_Dbrow[18],
                                            $piwik_upsert_Dbrow[19],
                                            $piwik_upsert_Dbrow[20],
                                            $piwik_upsert_Dbrow[21],
                                            $piwik_upsert_Dbrow[22]
                                           );
    

    $counterUpsert++;
}


$dbhPiwik->disconnect();
$dbhFrontendStats->disconnect();

print "piwik_log_link_visit_action upsert:",$counterUpsert,"\n";


1;