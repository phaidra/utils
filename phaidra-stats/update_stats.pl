#!/usr/bin/env perl

use warnings; 
use strict;
use Data::Dumper;
use Log::Log4perl;
use Config::JSON;
use DBI;

=pod

=head1 update_stats
usage:
./update_stats.pl
=cut

my $logconf = q(
  log4perl.category.MyLogger         = INFO, Logfile, Screen
 
  log4perl.appender.Logfile          = Log::Log4perl::Appender::File
  log4perl.appender.Logfile.filename = update_stats.log
  log4perl.appender.Logfile.layout   = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Logfile.layout.ConversionPattern=%d %m%n
  log4perl.appender.Logfile.utf8    = 1
 
  log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
  log4perl.appender.Screen.stderr  = 0
  log4perl.appender.Screen.layout  = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Screen.layout.ConversionPattern=%d %m%n
  log4perl.appender.Screen.utf8   = 1
);

Log::Log4perl::init( \$logconf );
my $log = Log::Log4perl::get_logger("MyLogger");

sub dbError {
  my $msg = shift;
  $log->error($msg);
  return 0;
}

my $cnf = Config::JSON->new(pathToFile => 'config.json');
$cnf = $cnf->{config};

my $fromdb = DBI->connect($cnf->{matomodb}->{dsn}, $cnf->{matomodb}->{user}, $cnf->{matomodb}->{pass}, { RaiseError => 1}) or die $DBI::errstr;
my $todb = DBI->connect($cnf->{statsdb}->{dsn}, $cnf->{statsdb}->{user}, $cnf->{statsdb}->{pass}, { RaiseError => 1}) or die $DBI::errstr;
my $fromsth;
my $tosth;
my $rv;

my @siteids = @{$cnf->{siteids}};

$log->info("Init tables...");
for my $idsite (@siteids) {
  $rv = $todb->do("
    CREATE TABLE IF NOT EXISTS `views_" . $idsite . "` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `pid` varchar(4096) DEFAULT NULL,
      `server_time` datetime NOT NULL,
      `location_country` char(3) DEFAULT NULL,
      `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
      `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (`id`),
      KEY `index_pid` (`pid`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
  ");
  return dbError($todb->errstr) unless $rv;
  $rv = $todb->do("
    CREATE TABLE IF NOT EXISTS `downloads_" . $idsite . "` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `pid` varchar(4096) DEFAULT NULL,
      `server_time` datetime NOT NULL,
      `location_country` char(3) DEFAULT NULL,
      `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
      `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      PRIMARY KEY (`id`),
      KEY `index_pid` (`pid`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
  ");
  return dbError($todb->errstr) unless $rv;
}

for my $idsite (@siteids) {
  $log->info("Updating views table $idsite...");
  $fromsth = $todb->prepare("SELECT MAX(`server_time`) FROM `views_" . $idsite . "`;");
  return dbError($fromdb->errstr) unless $fromsth;
  return dbError($fromdb->errstr) unless $fromsth->execute();
  my $max_server_time;
  $fromsth->bind_columns(\$max_server_time);
  $fromsth->fetch;
  unless ($max_server_time) {
    $max_server_time = '1970-01-01';
  }

  $log->info("Latest server_time: $max_server_time");

  $tosth = $todb->prepare("
    INSERT INTO `views_" . $idsite . "`(pid, server_time, location_country)
    VALUES (?, ?, ?);
  ");
  return dbError($todb->errstr) unless $tosth;

  $fromsth = $fromdb->prepare("
    SELECT LOWER(REGEXP_SUBSTR(name, 'o:\\\\d+')), server_time, location_country
    FROM piwik_log_link_visit_action
      INNER JOIN piwik_log_action on piwik_log_action.idaction = piwik_log_link_visit_action.idaction_url
      INNER JOIN piwik_log_visit on piwik_log_visit.idvisit = piwik_log_link_visit_action.idvisit
    WHERE 
      piwik_log_action.type = 1 AND
      piwik_log_link_visit_action.idsite = ? AND
      piwik_log_action.name like '%/detail/o:%' AND
      piwik_log_link_visit_action.server_time > ?
  ");
  return dbError($fromdb->errstr) unless $fromsth;
  return dbError($fromdb->errstr) unless $fromsth->execute($idsite, $max_server_time);

  my ($idaction, $pid, $ts, $loc);
  $fromsth->bind_columns(\$pid, \$ts, \$loc);
  while ($fromsth->fetch) {
    return dbError($todb->errstr) unless $tosth->execute($pid, $ts, $loc);
  }
}

for my $idsite (@siteids) {
  $log->info("Updating downloads table $idsite...");
  $fromsth = $todb->prepare("SELECT MAX(`idaction`) FROM `downloads_" . $idsite . "`;");
  return dbError($fromdb->errstr) unless $fromsth;
  return dbError($fromdb->errstr) unless $fromsth->execute();
  my $max_server_time;
  $fromsth->bind_columns(\$max_server_time);
  $fromsth->fetch;
  if ($max_server_time) {
    $max_server_time = int($max_server_time);
  } else {
    $max_server_time = 0
  }

  $log->info("Latest idaction: $max_server_time");

  $tosth = $todb->prepare("
    INSERT INTO `downloads_" . $idsite . "`(idaction, pid, server_time, location_country)
    VALUES (?, ?, ?);
  ");
  return dbError($todb->errstr) unless $tosth;

  $fromsth = $fromdb->prepare("
    SELECT idaction, LOWER(REGEXP_SUBSTR(name, 'o:\\\\d+')), server_time, location_country
    FROM piwik_log_link_visit_action
      INNER JOIN piwik_log_action on piwik_log_action.idaction = piwik_log_link_visit_action.idaction_url
      INNER JOIN piwik_log_visit on piwik_log_visit.idvisit = piwik_log_link_visit_action.idvisit
    WHERE 
      piwik_log_action.type = 1 AND
      piwik_log_link_visit_action.idsite = ? AND
      (piwik_log_action.name like '%/download/o:%' OR piwik_log_action.name like '%/open/o:%')AND
      piwik_log_action.idaction > ?
  ");
  return dbError($fromdb->errstr) unless $fromsth;
  return dbError($fromdb->errstr) unless $fromsth->execute($idsite, $max_server_time);

  my ($idaction, $pid, $ts, $loc);
  $fromsth->bind_columns(\$idaction, \$pid, \$ts, \$loc);
  while ($fromsth->fetch) {
    return dbError($todb->errstr) unless $tosth->execute($idaction, $pid, $ts, $loc);
  }
}

$log->info("Done.");

1;
