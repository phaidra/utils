#!/usr/bin/perl

use warnings; 
use strict;
use Data::Dumper;
$Data::Dumper::Indent= 1;

use Config::JSON;
use DBI;


my $pathToFile = $ARGV[0];
if(not defined $pathToFile) {$pathToFile = '/home/michal/Documents/code/area42/user/mf/statistics2016/bin/init.json'}
print '$pathToFile:', $pathToFile."\n";

my $config = Config::JSON->new(pathToFile => $pathToFile);
$config = $config->{config};


my $host     = $config->{phaidra_instances}->{frontendStatsMysql}->{host};
my $dbName   = $config->{phaidra_instances}->{frontendStatsMysql}->{dbName};
my $user     = $config->{phaidra_instances}->{frontendStatsMysql}->{user};
my $pass     = $config->{phaidra_instances}->{frontendStatsMysql}->{pass};

my $dbMysqlHanler = DBI->connect(          
                                  "dbi:mysql:dbname=$dbName;host=$host", 
                                  $user,
                                  $pass,
                                  { RaiseError => 1}
                                ) or die $DBI::errstr;

$dbMysqlHanler->do(q{SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO"});
$dbMysqlHanler->do(q{/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */});
$dbMysqlHanler->do(q{/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */});
$dbMysqlHanler->do(q{/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */});
$dbMysqlHanler->do(q{/*!40101 SET NAMES utf8 */});

main();







sub main{
    
    createPhaidrasites();
    createUsers();
    createInventory();
    search_pattern();
    piwik_log_link_visit_action();
    piwik_log_visit();
}


sub createPhaidrasites {

     my $queryPhaidrasites = "
            CREATE TABLE IF NOT EXISTS `phaidrasites` (
                   `id` int(10) NOT NULL AUTO_INCREMENT,
                   `tenantid` int(10) DEFAULT NULL,
                   `idsite` int(10) DEFAULT NULL,
                   `namerepo` varchar(100) DEFAULT NULL,
                   `institution` varchar(100) DEFAULT NULL,
                   `logo` varchar(100) DEFAULT NULL,
                   `contact` varchar(100) DEFAULT NULL,
                   `website` varchar(100) DEFAULT NULL,
                   `email` varchar(100) DEFAULT NULL,
                    PRIMARY KEY (`id`),
                    KEY `idx` (`id`)
           ) 
           ENGINE=InnoDB  
           DEFAULT CHARSET=utf8 
           AUTO_INCREMENT=14 ;";
    my $sth = $dbMysqlHanler->prepare($queryPhaidrasites) or die "ERR: can't prepare: ".$DBI::errstr;
    $sth->execute();
    $sth->finish;
}


sub createUsers {

   my $queryUsers = "
          CREATE TABLE IF NOT EXISTS `users` (
              `id` int(10) NOT NULL AUTO_INCREMENT,
              `tenantid` int(10) DEFAULT NULL,
              `name` varchar(45) DEFAULT NULL,
              `lastname` varchar(45) DEFAULT NULL,
              `title` varchar(100) DEFAULT NULL,
              `pass` varchar(45) DEFAULT NULL,
              `created` datetime DEFAULT NULL,
              `registered` datetime DEFAULT NULL,
              `status` varchar(45) DEFAULT NULL,
              `institution` varchar(100) DEFAULT NULL,
              `role` varchar(50) DEFAULT NULL,
              `telephone` varchar(100) DEFAULT NULL,
              `pic` varchar(100) DEFAULT NULL,
              `website` varchar(100) DEFAULT NULL,
              `username` varchar(100) DEFAULT NULL,
              `email` varchar(45) DEFAULT NULL,
              `idsite` int(10) DEFAULT NULL,
              PRIMARY KEY (`id`)
          ) 
          ENGINE=InnoDB  
          DEFAULT CHARSET=utf8 
          AUTO_INCREMENT=108 ;";
   my $sth = $dbMysqlHanler->prepare($queryUsers) or die "ERR: can't prepare: ".$DBI::errstr;
   $sth->execute();

   my $queryUsersAdmin = "INSERT INTO `users` (`id`, `tenantid`, `name`, `lastname`, `title`, `pass`, `created`, `registered`, `status`, `institution`, `role`, `telephone`, `pic`, `website`, `username`, `email`, `idsite`) VALUES
   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
   $sth = $dbMysqlHanler->prepare($queryUsersAdmin) or die "ERR: can't prepare: ".$DBI::errstr;
   $sth->execute(1, 1, 'Admin', undef, undef, 1, undef, undef, 'Active', undef, 'Admin', undef, undef, undef, 'admin', undef, -1);
   $sth->finish;
}


sub createInventory {

   my $queryInventory = "
             CREATE TABLE IF NOT EXISTS `inventory` (
                  `id` int(10) NOT NULL AUTO_INCREMENT,
                  `idsite` int(10) unsigned NOT NULL,
                  `oid` varchar(20) DEFAULT NULL,
                  `cmodel` varchar(45) DEFAULT NULL,
                  `mimetype` varchar(45) DEFAULT NULL,
                  `owner` varchar(45) DEFAULT NULL,
                  `state` varchar(45) DEFAULT NULL,
                  `filesize` int(10) DEFAULT NULL,
                  `redcode` varchar(45) DEFAULT NULL,
                  `acccode` varchar(45) NOT NULL,
                  `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  `created` datetime NOT NULL,
                  `modified` datetime NOT NULL,
                  `title` text,
                   PRIMARY KEY (`id`),
                   KEY `oida` (`oid`),
                   KEY `cmodel` (`cmodel`),
                   KEY `owner` (`owner`),
                   KEY `idsite` (`idsite`)
            ) 
            ENGINE=InnoDB  
            DEFAULT CHARSET=utf8 
            AUTO_INCREMENT=782549 ;";
   my $sth = $dbMysqlHanler->prepare($queryInventory) or die "ERR: can't prepare: ".$DBI::errstr;
   $sth->execute();
   $sth->finish;
}

sub search_pattern {

    my $querySearchPattern = "
             CREATE TABLE IF NOT EXISTS `search_pattern` (
                  `SID` int(11) NOT NULL AUTO_INCREMENT,
                  `idsite` int(10) unsigned NOT NULL,
                  `name` varchar(2048) DEFAULT NULL,
                  `session_id` varchar(128) NOT NULL,
                  `pattern` mediumblob NOT NULL,
                  `last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                   PRIMARY KEY (`SID`)
            ) 
            ENGINE=InnoDB 
            AUTO_INCREMENT=78195 
            DEFAULT CHARSET=utf8;";
    my $sth = $dbMysqlHanler->prepare($querySearchPattern) or die "ERR: can't prepare: ".$DBI::errstr;
    $sth->execute();
    $sth->finish;
}            
    
sub piwik_log_link_visit_action {
 
     my $queryPiwik_log_link_visit_action = "  
           CREATE TABLE IF NOT EXISTS `piwik_log_link_visit_action` (
                  `idlink_va` int(11) unsigned NOT NULL AUTO_INCREMENT,
                  `idsite` int(10) unsigned NOT NULL,
                  `idvisitor` binary(8) NOT NULL,
                  `server_time` datetime NOT NULL,
                  `idvisit` int(10) unsigned NOT NULL,
                  `idaction_url` int(10) unsigned DEFAULT NULL,
                  `idaction_url_ref` int(10) unsigned DEFAULT '0',
                  `idaction_name` int(10) unsigned DEFAULT NULL,
                  `idaction_name_ref` int(10) unsigned NOT NULL,
                  `idaction_event_category` int(10) unsigned DEFAULT NULL,
                  `idaction_event_action` int(10) unsigned DEFAULT NULL,
                  `time_spent_ref_action` int(10) unsigned NOT NULL,
                  `custom_var_k1` varchar(200) DEFAULT NULL,
                  `custom_var_v1` varchar(200) DEFAULT NULL,
                  `custom_var_k2` varchar(200) DEFAULT NULL,
                  `custom_var_v2` varchar(200) DEFAULT NULL,
                  `custom_var_k3` varchar(200) DEFAULT NULL,
                  `custom_var_v3` varchar(200) DEFAULT NULL,
                  `custom_var_k4` varchar(200) DEFAULT NULL,
                  `custom_var_v4` varchar(200) DEFAULT NULL,
                  `custom_var_k5` varchar(200) DEFAULT NULL,
                  `custom_var_v5` varchar(200) DEFAULT NULL,
                  `custom_float` float DEFAULT NULL,
                  PRIMARY KEY (`idlink_va`),
                  KEY `index_idvisit` (`idvisit`),
                  KEY `index_idsite_servertime` (`idsite`,`server_time`),
                  KEY `v1-views` (`custom_var_v1`),
                  KEY `v2-downloads` (`custom_var_v2`),
                  KEY `v3-metadata` (`custom_var_v3`),
                  KEY `v4-detailpage` (`custom_var_v4`),
                  KEY `k3-metadata` (`custom_var_k3`)
           ) 
           ENGINE=InnoDB  
           DEFAULT CHARSET=utf8 
           AUTO_INCREMENT=174012 ;";
    my $sth = $dbMysqlHanler->prepare($queryPiwik_log_link_visit_action) or die "ERR: can't prepare: ".$DBI::errstr;
    $sth->execute();
    $sth->finish;
}


sub piwik_log_visit {
    my $queryPiwik_log_visit = "
             CREATE TABLE IF NOT EXISTS `piwik_log_visit` (
                      `idvisit` int(10) unsigned NOT NULL AUTO_INCREMENT,
                      `idsite` int(10) unsigned NOT NULL,
                      `idvisitor` binary(8) NOT NULL,
                      `visitor_localtime` time NOT NULL,
                      `visitor_returning` tinyint(1) NOT NULL,
                      `visitor_count_visits` smallint(5) unsigned NOT NULL,
                      `visitor_days_since_last` smallint(5) unsigned NOT NULL,
                      `visitor_days_since_order` smallint(5) unsigned NOT NULL,
                      `visitor_days_since_first` smallint(5) unsigned NOT NULL,
                      `visit_first_action_time` datetime NOT NULL,
                      `visit_last_action_time` datetime NOT NULL,
                      `visit_exit_idaction_url` int(11) unsigned DEFAULT '0',
                      `visit_exit_idaction_name` int(11) unsigned NOT NULL,
                      `visit_entry_idaction_url` int(11) unsigned NOT NULL,
                      `visit_entry_idaction_name` int(11) unsigned NOT NULL,
                      `visit_total_actions` smallint(5) unsigned NOT NULL,
                      `visit_total_searches` smallint(5) unsigned NOT NULL,
                      `visit_total_events` smallint(5) unsigned NOT NULL,
                      `visit_total_time` smallint(5) unsigned NOT NULL,
                      `visit_goal_converted` tinyint(1) NOT NULL,
                      `visit_goal_buyer` tinyint(1) NOT NULL,
                      `referer_type` tinyint(1) unsigned DEFAULT NULL,
                      `referer_name` varchar(70) DEFAULT NULL,
                      `referer_url` text NOT NULL,
                      `referer_keyword` varchar(255) DEFAULT NULL,
                      `config_id` binary(8) NOT NULL,
                      `config_os` char(3) NOT NULL,
                      `config_browser_name` varchar(10) NOT NULL,
                      `config_browser_version` varchar(20) NOT NULL,
                      `config_resolution` varchar(9) NOT NULL,
                      `config_pdf` tinyint(1) NOT NULL,
                      `config_flash` tinyint(1) NOT NULL,
                      `config_java` tinyint(1) NOT NULL,
                      `config_director` tinyint(1) NOT NULL,
                      `config_quicktime` tinyint(1) NOT NULL,
                      `config_realplayer` tinyint(1) NOT NULL,
                      `config_windowsmedia` tinyint(1) NOT NULL,
                      `config_gears` tinyint(1) NOT NULL,
                      `config_silverlight` tinyint(1) NOT NULL,
                      `config_cookie` tinyint(1) NOT NULL,
                      `location_ip` varbinary(16) NOT NULL,
                      `location_browser_lang` varchar(20) NOT NULL,
                      `location_country` char(3) NOT NULL,
                      `location_region` char(2) DEFAULT NULL,
                      `location_city` varchar(255) DEFAULT NULL,
                      `location_latitude` float(10,6) DEFAULT NULL,
                      `location_longitude` float(10,6) DEFAULT NULL,
                      `custom_var_k1` varchar(200) DEFAULT NULL,
                      `custom_var_v1` varchar(200) DEFAULT NULL,
                      `custom_var_k2` varchar(200) DEFAULT NULL,
                      `custom_var_v2` varchar(200) DEFAULT NULL,
                      `custom_var_k3` varchar(200) DEFAULT NULL,
                      `custom_var_v3` varchar(200) DEFAULT NULL,
                      `custom_var_k4` varchar(200) DEFAULT NULL,
                      `custom_var_v4` varchar(200) DEFAULT NULL,
                      `custom_var_k5` varchar(200) DEFAULT NULL,
                      `custom_var_v5` varchar(200) DEFAULT NULL,
                      `location_provider` varchar(100) DEFAULT NULL,
                      PRIMARY KEY (`idvisit`),
                      KEY `index_idsite_config_datetime` (`idsite`,`config_id`,`visit_last_action_time`),
                      KEY `index_idsite_datetime` (`idsite`,`visit_last_action_time`),
                      KEY `index_idsite_idvisitor` (`idsite`,`idvisitor`)
               ) 
               ENGINE=InnoDB  
               DEFAULT CHARSET=utf8 
               AUTO_INCREMENT=28923 ;";
    my $sth = $dbMysqlHanler->prepare($queryPiwik_log_visit) or die "ERR: can't prepare: ".$DBI::errstr;
    $sth->execute();
    $sth->finish;
}

1;