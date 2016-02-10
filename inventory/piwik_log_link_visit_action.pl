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
                                
                                
#read from Frontend Statistics database
my $sthFrontendStats = $dbhFrontendStats->prepare( "SELECT idlink_va, server_time  FROM piwik_log_link_visit_action" );
$sthFrontendStats->execute();
my $frontendStats;
while (my @frontendStatsDbrow = $sthFrontendStats->fetchrow_array){
    $frontendStats->{$frontendStatsDbrow[0]} = $frontendStatsDbrow[1];
}


#read from Piwik database
my $sthPiwik = $dbhPiwik->prepare( "SELECT idlink_va, server_time FROM piwik_log_link_visit_action" );
$sthPiwik->execute();
my $piwik_log_link_visit_action;
while (my @piwikDBrow = $sthPiwik->fetchrow_array){
    $piwik_log_link_visit_action->{$piwikDBrow[0]} = $piwikDBrow[1];
}


=head1

  Insert new record into Frontend statistics db

=cut

sub insertRecord($){
    
    my $idlink_va = shift;
    print "Inserting idvisit:",$idlink_va,"\n";
    my $sthPiwik_insert = $dbhPiwik->prepare( "SELECT * FROM piwik_log_link_visit_action where idlink_va=?" );
    $sthPiwik_insert->execute($idlink_va);
    while (my @piwik_insert_Dbrow = $sthPiwik_insert->fetchrow_array){
          my $frontendStats_insert_query = "INSERT INTO `piwik_log_link_visit_action` (
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
                                            VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
          my $sthFrontendStats_insert = $dbhFrontendStats->prepare($frontendStats_insert_query);
          $sthFrontendStats_insert->execute(
                                            $piwik_insert_Dbrow[0],
                                            $piwik_insert_Dbrow[1],
                                            $piwik_insert_Dbrow[2],
                                            $piwik_insert_Dbrow[3],
                                            $piwik_insert_Dbrow[4],
                                            $piwik_insert_Dbrow[5],
                                            $piwik_insert_Dbrow[6],
                                            $piwik_insert_Dbrow[7],
                                            $piwik_insert_Dbrow[8],
                                            $piwik_insert_Dbrow[9],
                                            $piwik_insert_Dbrow[10],
                                            $piwik_insert_Dbrow[11],
                                            $piwik_insert_Dbrow[12],
                                            $piwik_insert_Dbrow[13],
                                            $piwik_insert_Dbrow[14],
                                            $piwik_insert_Dbrow[15],
                                            $piwik_insert_Dbrow[16],
                                            $piwik_insert_Dbrow[17],
                                            $piwik_insert_Dbrow[18],
                                            $piwik_insert_Dbrow[19],
                                            $piwik_insert_Dbrow[20],
                                            $piwik_insert_Dbrow[21],
                                            $piwik_insert_Dbrow[22]
                                           );
          $sthFrontendStats_insert->finish();
    }
    $sthPiwik_insert->finish(); 
}

=head1

  Update record in Frontend statistics db

=cut

sub updateRecord($){
    my $idlink_va = shift;
    print "Updating idvisit:",$idlink_va,"\n";
    my $sthPiwik_update = $dbhPiwik->prepare( "SELECT * FROM piwik_log_link_visit_action where idlink_va=?" );
    $sthPiwik_update->execute($idlink_va);
    while (my @piwik_update_Dbrow = $sthPiwik_update->fetchrow_array){
          my $frontendStats_update_query = "UPDATE search_pattern set
                                                              idsite=?,
                                                              idvisitor=?,
                                                              server_time=?,
                                                              idvisit=?,
                                                              idaction_url=?,
                                                              idaction_url_ref=?,
                                                              idaction_name=?,
                                                              idaction_name_ref=?,
                                                              idaction_event_category=?,
                                                              idaction_event_action=?,
                                                              time_spent_ref_action=?,
                                                              custom_var_k1=?,
                                                              custom_var_v1=?,
                                                              custom_var_k2=?,
                                                              custom_var_v2=?,
                                                              custom_var_k3=?,
                                                              custom_var_v3=?,
                                                              custom_var_k4=?,
                                                              custom_var_v4=?,
                                                              custom_var_k5=?,
                                                              custom_var_v5=?,
                                                              custom_float=?
                                                   where idlink_va=?;";
          my $sthFrontendStats_update = $dbhFrontendStats->prepare($frontendStats_update_query);
          $sthFrontendStats_update->execute(
                                            $piwik_update_Dbrow[1],
                                            $piwik_update_Dbrow[2],
                                            $piwik_update_Dbrow[3],
                                            $piwik_update_Dbrow[4],
                                            $piwik_update_Dbrow[5],
                                            $piwik_update_Dbrow[6],
                                            $piwik_update_Dbrow[7],
                                            $piwik_update_Dbrow[8],
                                            $piwik_update_Dbrow[9],
                                            $piwik_update_Dbrow[10],
                                            $piwik_update_Dbrow[11],
                                            $piwik_update_Dbrow[12],
                                            $piwik_update_Dbrow[13],
                                            $piwik_update_Dbrow[14],
                                            $piwik_update_Dbrow[15],
                                            $piwik_update_Dbrow[16],
                                            $piwik_update_Dbrow[17],
                                            $piwik_update_Dbrow[18],
                                            $piwik_update_Dbrow[19],
                                            $piwik_update_Dbrow[20],
                                            $piwik_update_Dbrow[21],
                                            $piwik_update_Dbrow[22],
                                            $piwik_update_Dbrow[0]
                                           );
          $sthFrontendStats_update->finish();
    }
    $sthPiwik_update->finish(); 

}

=head1

  Delete record from Frontend statistics db

=cut

sub deleteRecord($){
    
    my $idlink_va = shift;
    print "Deleting idvisit:",$idlink_va,"\n";
    my $frontendStats_delete_query = "DELETE from piwik_log_link_visit_action where idvisit=?;";
    my $sthFrontendStats_delete = $dbhFrontendStats->prepare($frontendStats_delete_query);
    $sthFrontendStats_delete->execute($idlink_va);
    $sthFrontendStats_delete->finish();
}

#####################################
#######  Main  ######################
#####################################
# insert/update
my $counterInsert = 0;
my $counterUpdate = 0;
my $counterDelete = 0;
foreach my $keyPiwik_log_link_visit_action (keys %{$piwik_log_link_visit_action}){
     if(defined $frontendStats->{$keyPiwik_log_link_visit_action}){
          if($frontendStats->{$keyPiwik_log_link_visit_action} lt $piwik_log_link_visit_action->{$keyPiwik_log_link_visit_action}){
                updateRecord($keyPiwik_log_link_visit_action);
                $counterUpdate++;
          }
     }else{
          insertRecord($keyPiwik_log_link_visit_action);
          $counterInsert++;
     }
}
#delete
foreach my $keyfrontendStats (keys %{$frontendStats}){
      if(not defined $piwik_log_link_visit_action->{$keyfrontendStats}){
          deleteRecord($keyfrontendStats); 
          $counterDelete++;
      }
}



print "search_pattern inserted:",$counterInsert,"\n";
print "search_pattern updated:",$counterUpdate,"\n";
print "search_pattern deleted:",$counterDelete,"\n";
1;