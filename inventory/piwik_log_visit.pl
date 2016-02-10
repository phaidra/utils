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
my $sthFrontendStats = $dbhFrontendStats->prepare( "SELECT idvisit, visit_last_action_time  FROM piwik_log_visit" );
$sthFrontendStats->execute();
my $frontendStats;
while (my @frontendStatsDbrow = $sthFrontendStats->fetchrow_array){
    $frontendStats->{$frontendStatsDbrow[0]} = $frontendStatsDbrow[1];
}


#read from Piwik database
my $sthPiwik = $dbhPiwik->prepare( "SELECT idvisit, visit_last_action_time FROM piwik_log_visit" );
$sthPiwik->execute();
my $piwik_log_visit;
while (my @piwikDBrow = $sthPiwik->fetchrow_array){
    $piwik_log_visit->{$piwikDBrow[0]} = $piwikDBrow[1];
}


=head1

  Insert new record into Frontend statistics db

=cut

sub insertRecord($){
    
    my $idvisit = shift;
    print "Inserting idvisit:",$idvisit,"\n";
    my $sthPiwik_insert = $dbhPiwik->prepare( "SELECT * FROM piwik_log_visit where idvisit=?" );
    $sthPiwik_insert->execute($idvisit);
    while (my @piwik_insert_Dbrow = $sthPiwik_insert->fetchrow_array){
          my $frontendStats_insert_query = "INSERT INTO `piwik_log_visit` (
                                                                           `idvisit`, 
                                                                           `idsite`, 
                                                                           `idvisitor`, 
                                                                           `visitor_localtime`, 
                                                                           `visitor_returning`, 
                                                                           `visitor_count_visits`,
                                                                           `visitor_days_since_last`,
                                                                           `visitor_days_since_order`,
                                                                           `visitor_days_since_first`,
                                                                           `visit_first_action_time`,
                                                                           `visit_last_action_time`,
                                                                           `visit_exit_idaction_url`,
                                                                           `visit_exit_idaction_name`,
                                                                           `visit_entry_idaction_url`,
                                                                           `visit_entry_idaction_name`,
                                                                           `visit_total_actions`,
                                                                           `visit_total_searches`,
                                                                           `visit_total_events`,
                                                                           `visit_total_time`,
                                                                           `visit_goal_converted`,
                                                                           `visit_goal_buyer`,
                                                                           `referer_type`,
                                                                           `referer_name`,
                                                                           `referer_url`,
                                                                           `referer_keyword`,
                                                                           `config_id`,
                                                                           `config_os`,
                                                                           `config_browser_name`,
                                                                           `config_browser_version`,
                                                                           `config_resolution`,
                                                                           `config_pdf`,
                                                                           `config_flash`,
                                                                           `config_java`,
                                                                           `config_director`,
                                                                           `config_quicktime`,
                                                                           `config_realplayer`,
                                                                           `config_windowsmedia`,
                                                                           `config_gears`,
                                                                           `config_silverlight`,
                                                                           `config_cookie`,
                                                                           `location_ip`,
                                                                           `location_browser_lang`,
                                                                           `location_country`,
                                                                           `location_region`,
                                                                           `location_city`,
                                                                           `location_latitude`,
                                                                           `location_longitude`,
                                                                           `custom_var_k1`,
                                                                           `custom_var_v1`,
                                                                           `custom_var_k2`,
                                                                           `custom_var_v2`,
                                                                           `custom_var_k3`,
                                                                           `custom_var_v3`,
                                                                           `custom_var_k4`,
                                                                           `custom_var_v4`,
                                                                           `custom_var_k5`,
                                                                           `location_provider`
                                                                          ) 
                                            VALUES (
                                                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                                                    );";
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
                                            $piwik_insert_Dbrow[22],
                                            $piwik_insert_Dbrow[23],
                                            $piwik_insert_Dbrow[24],
                                            $piwik_insert_Dbrow[25],
                                            $piwik_insert_Dbrow[26],
                                            $piwik_insert_Dbrow[27],
                                            $piwik_insert_Dbrow[28],
                                            $piwik_insert_Dbrow[29],
                                            $piwik_insert_Dbrow[30],
                                            $piwik_insert_Dbrow[31],
                                            $piwik_insert_Dbrow[32],
                                            $piwik_insert_Dbrow[33],
                                            $piwik_insert_Dbrow[34],
                                            $piwik_insert_Dbrow[35],
                                            $piwik_insert_Dbrow[36],
                                            $piwik_insert_Dbrow[37],
                                            $piwik_insert_Dbrow[38],
                                            $piwik_insert_Dbrow[39],
                                            $piwik_insert_Dbrow[40],
                                            $piwik_insert_Dbrow[41],
                                            $piwik_insert_Dbrow[42],
                                            $piwik_insert_Dbrow[43],
                                            $piwik_insert_Dbrow[44],
                                            $piwik_insert_Dbrow[45],
                                            $piwik_insert_Dbrow[46],
                                            $piwik_insert_Dbrow[47],
                                            $piwik_insert_Dbrow[48],
                                            $piwik_insert_Dbrow[49],
                                            $piwik_insert_Dbrow[50],
                                            $piwik_insert_Dbrow[51],
                                            $piwik_insert_Dbrow[52],
                                            $piwik_insert_Dbrow[53],
                                            $piwik_insert_Dbrow[54],
                                            $piwik_insert_Dbrow[55],
                                            $piwik_insert_Dbrow[56]
                                           );
          $sthFrontendStats_insert->finish();
    }
    $sthPiwik_insert->finish(); 
}

=head1

  Update record in Frontend statistics db

=cut

sub updateRecord($){
    
    my $idvisit = shift;
    print "Updating idvisit:",$idvisit,"\n";
    my $sthPiwik_update = $dbhPiwik->prepare( "SELECT * FROM piwik_log_visit where idvisit=?" );
    $sthPiwik_update->execute($idvisit);
    while (my @piwik_update_Dbrow = $sthPiwik_update->fetchrow_array){
          my $frontendStats_update_query = "UPDATE search_pattern set
                                                              idsite=?,
                                                              idvisitor=?,
                                                              visitor_localtime=?,
                                                              visitor_returning=?,
                                                              visitor_count_visits=?,
                                                              visitor_days_since_last=?,
                                                              visitor_days_since_order=?,
                                                              visitor_days_since_first=?,
                                                              visit_first_action_time=?,
                                                              visit_last_action_time=?,
                                                              visit_exit_idaction_url=?,
                                                              visit_exit_idaction_name=?,
                                                              visit_entry_idaction_url=?,
                                                              visit_entry_idaction_name=?,
                                                              visit_total_actions=?,
                                                              visit_total_searches=?,
                                                              visit_total_events=?,
                                                              visit_total_time=?,
                                                              visit_goal_converted=?,
                                                              visit_goal_buyer=?,
                                                              referer_type=?,
                                                              referer_name=?,
                                                              referer_url=?,
                                                              referer_keyword=?,
                                                              config_id=?,
                                                              config_os=?,
                                                              config_browser_name=?,
                                                              config_browser_version=?,
                                                              config_resolution=?,
                                                              config_pdf=?,
                                                              config_flash=?,
                                                              config_java=?,
                                                              config_director=?,
                                                              config_quicktime=?,
                                                              config_realplayer=?,
                                                              config_windowsmedia=?,
                                                              config_gears=?,
                                                              config_silverlight=?,
                                                              config_cookie=?,
                                                              location_ip=?,
                                                              location_browser_lang=?,
                                                              location_country=?,
                                                              location_region=?,
                                                              location_city=?,
                                                              location_latitude=?,
                                                              location_longitude=?,
                                                              custom_var_k1=?,
                                                              custom_var_v1=?,
                                                              custom_var_k2=?,
                                                              custom_var_v2=?,
                                                              custom_var_k3=?,
                                                              custom_var_v3=?,
                                                              custom_var_k4=?,
                                                              custom_var_v4=?,
                                                              custom_var_k5=?,
                                                              custom_var_k5=?,
                                                              location_provider=?
                                                   where idvisit=?;";
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
                                            $piwik_update_Dbrow[23],
                                            $piwik_update_Dbrow[24],
                                            $piwik_update_Dbrow[25],
                                            $piwik_update_Dbrow[26],
                                            $piwik_update_Dbrow[27],
                                            $piwik_update_Dbrow[28],
                                            $piwik_update_Dbrow[29],
                                            $piwik_update_Dbrow[30],
                                            $piwik_update_Dbrow[31],
                                            $piwik_update_Dbrow[32],
                                            $piwik_update_Dbrow[33],
                                            $piwik_update_Dbrow[34],
                                            $piwik_update_Dbrow[35],
                                            $piwik_update_Dbrow[36],
                                            $piwik_update_Dbrow[37],
                                            $piwik_update_Dbrow[38],
                                            $piwik_update_Dbrow[39],
                                            $piwik_update_Dbrow[40],
                                            $piwik_update_Dbrow[41],
                                            $piwik_update_Dbrow[42],
                                            $piwik_update_Dbrow[43],
                                            $piwik_update_Dbrow[44],
                                            $piwik_update_Dbrow[45],
                                            $piwik_update_Dbrow[46],
                                            $piwik_update_Dbrow[47],
                                            $piwik_update_Dbrow[48],
                                            $piwik_update_Dbrow[49],
                                            $piwik_update_Dbrow[50],
                                            $piwik_update_Dbrow[51],
                                            $piwik_update_Dbrow[52],
                                            $piwik_update_Dbrow[53],
                                            $piwik_update_Dbrow[54],
                                            $piwik_update_Dbrow[55],
                                            $piwik_update_Dbrow[56],
                                            $piwik_update_Dbrow[57],
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
    
    my $idvisit = shift;
    print "Deleting idvisit:",$idvisit,"\n";
    my $frontendStats_delete_query = "DELETE from piwik_log_visit where idvisit=?;";
    my $sthFrontendStats_delete = $dbhFrontendStats->prepare($frontendStats_delete_query);
    $sthFrontendStats_delete->execute($idvisit);
    $sthFrontendStats_delete->finish();
}

#####################################
#######  Main  ######################
#####################################
# insert/update
my $counterInsert = 0;
my $counterUpdate = 0;
my $counterDelete = 0;
foreach my $keyPiwik_log_visit (keys %{$piwik_log_visit}){
     if(defined $frontendStats->{$keyPiwik_log_visit}){
          if($frontendStats->{$keyPiwik_log_visit} lt $piwik_log_visit->{$keyPiwik_log_visit}){
                updateRecord($keyPiwik_log_visit);
                $counterUpdate++;
          }
     }else{
          insertRecord($keyPiwik_log_visit);
          $counterInsert++;
     }
}
#delete
foreach my $keyfrontendStats (keys %{$frontendStats}){
      if(not defined $piwik_log_visit->{$keyfrontendStats}){
          deleteRecord($keyfrontendStats); 
          $counterDelete++;
      }
}



print "search_pattern inserted:",$counterInsert,"\n";
print "search_pattern updated:",$counterUpdate,"\n";
print "search_pattern deleted:",$counterDelete,"\n";
1;