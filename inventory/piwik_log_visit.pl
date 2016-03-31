#!/usr/bin/env perl

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
                                

                                
                                
#get record with latest time from Frontend Statistics database
my $latestTimeFrontendStats = 0;
my $sthFrontendStats = $dbhFrontendStats->prepare( "SELECT visit_last_action_time FROM  piwik_log_visit ORDER BY visit_last_action_time DESC LIMIT 1;" );
$sthFrontendStats->execute();
while (my @frontendStatsDbrow = $sthFrontendStats->fetchrow_array){
    $latestTimeFrontendStats =  $frontendStatsDbrow[0];
}                                

#read Piwik database with newer or equal $latestTimeFrontendStats and upsert new records to Frontend Statistics database
my $sthPiwik = $dbhPiwik->prepare( "SELECT * FROM piwik_log_visit where visit_last_action_time >= \"$latestTimeFrontendStats\" ORDER BY visit_last_action_time ASC" );
$sthPiwik->execute();
my $counterUpsert;
my $frontendStats_upsert_query = "INSERT INTO `piwik_log_visit` (
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
                                                     values(
                                                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                                                      ) 
                                                     on duplicate key update
                                                                        idvisit = values(idvisit),
                                                                        idsite = values(idsite),
                                                                        idvisitor = values(idvisitor),
                                                                        visitor_localtime = values(visitor_localtime),
                                                                        visitor_returning = values(visitor_returning),
                                                                        visitor_count_visits = values(visitor_count_visits),
                                                                        visitor_days_since_last = values(visitor_days_since_last),
                                                                        visitor_days_since_order = values(visitor_days_since_order),
                                                                        visitor_days_since_first = values(visitor_days_since_first),
                                                                        visit_first_action_time = values(visit_first_action_time),
                                                                        visit_last_action_time = values(visit_last_action_time),
                                                                        visit_exit_idaction_url = values(visit_exit_idaction_url),
                                                                        visit_exit_idaction_name = values(visit_exit_idaction_name),
                                                                        visit_entry_idaction_url = values(visit_entry_idaction_url),
                                                                        visit_entry_idaction_name = values(visit_entry_idaction_name),
                                                                        visit_total_actions = values(visit_total_actions),
                                                                        visit_total_searches = values(visit_total_searches),
                                                                        visit_total_events = values(visit_total_events),
                                                                        visit_total_time = values(visit_total_time),
                                                                        visit_goal_converted = values(visit_goal_converted),
                                                                        visit_goal_buyer = values(visit_goal_buyer),
                                                                        referer_type = values(referer_type),
                                                                        referer_name = values(referer_name),
                                                                        referer_url = values(referer_url),
                                                                        referer_keyword = values(referer_keyword),
                                                                        config_id = values(config_id),
                                                                        config_os = values(config_os),
                                                                        config_browser_name = values(config_browser_name),
                                                                        config_browser_version = values(config_browser_version),
                                                                        config_resolution = values(config_resolution),
                                                                        config_pdf = values(config_pdf),
                                                                        config_flash = values(config_flash),
                                                                        config_java = values(config_java),
                                                                        config_director = values(config_director),
                                                                        config_quicktime = values(config_quicktime),
                                                                        config_realplayer = values(config_realplayer),
                                                                        config_windowsmedia = values(config_windowsmedia),
                                                                        config_gears = values(config_gears),
                                                                        config_silverlight = values(config_silverlight),
                                                                        config_cookie = values(config_cookie),
                                                                        location_ip = values(location_ip),
                                                                        location_browser_lang = values(location_browser_lang),
                                                                        location_country = values(location_country),
                                                                        location_region = values(location_region),
                                                                        location_city = values(location_city),
                                                                        location_latitude = values(location_latitude),
                                                                        location_longitude = values(location_longitude),
                                                                        custom_var_k1 = values(custom_var_k1),
                                                                        custom_var_v1 = values(custom_var_v1),
                                                                        custom_var_k2 = values(custom_var_k2),
                                                                        custom_var_v2 = values(custom_var_v2),
                                                                        custom_var_k3 = values(custom_var_k3),
                                                                        custom_var_v3 = values(custom_var_v3),
                                                                        custom_var_k4 = values(custom_var_k4),
                                                                        custom_var_v4 = values(custom_var_v4),
                                                                        custom_var_k5 = values(custom_var_k5),
                                                                        location_provider = values(location_provider) 
                                                      ";
                                                      
my $sthFrontendStats_upsert = $dbhFrontendStats->prepare($frontendStats_upsert_query);

while (my @piwik_upsert_Dbrow = $sthPiwik->fetchrow_array){
      # print "Upserting idvisit:",$piwik_upsert_Dbrow[0],"\n";
      # print "Upserting idvisit time:",$piwik_upsert_Dbrow[10],"\n";

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
                                            $piwik_upsert_Dbrow[22],
                                            $piwik_upsert_Dbrow[23],
                                            $piwik_upsert_Dbrow[24],
                                            $piwik_upsert_Dbrow[25],
                                            $piwik_upsert_Dbrow[26],
                                            $piwik_upsert_Dbrow[27],
                                            $piwik_upsert_Dbrow[28],
                                            $piwik_upsert_Dbrow[29],
                                            $piwik_upsert_Dbrow[30],
                                            $piwik_upsert_Dbrow[31],
                                            $piwik_upsert_Dbrow[32],
                                            $piwik_upsert_Dbrow[33],
                                            $piwik_upsert_Dbrow[34],
                                            $piwik_upsert_Dbrow[35],
                                            $piwik_upsert_Dbrow[36],
                                            $piwik_upsert_Dbrow[37],
                                            $piwik_upsert_Dbrow[38],
                                            $piwik_upsert_Dbrow[39],
                                            $piwik_upsert_Dbrow[40],
                                            $piwik_upsert_Dbrow[41],
                                            $piwik_upsert_Dbrow[42],
                                            $piwik_upsert_Dbrow[43],
                                            $piwik_upsert_Dbrow[44],
                                            $piwik_upsert_Dbrow[45],
                                            $piwik_upsert_Dbrow[46],
                                            $piwik_upsert_Dbrow[47],
                                            $piwik_upsert_Dbrow[48],
                                            $piwik_upsert_Dbrow[49],
                                            $piwik_upsert_Dbrow[50],
                                            $piwik_upsert_Dbrow[51],
                                            $piwik_upsert_Dbrow[52],
                                            $piwik_upsert_Dbrow[53],
                                            $piwik_upsert_Dbrow[54],
                                            $piwik_upsert_Dbrow[55],
                                            $piwik_upsert_Dbrow[56]
                                           );

    $counterUpsert++; 
}

$dbhPiwik->disconnect();
$dbhFrontendStats->disconnect();

print "piwik_log_visit upsert:",$counterUpsert,"\n";

1;