#!/usr/bin/env perl

use warnings; 
use strict;
use Data::Dumper;
use Log::Log4perl;
use Config::JSON;
use DBI;
use Net::IP qw(:PROC);
use POSIX qw(strftime);
use Digest::SHA qw(sha256_hex);

=pod

=head1 migrate_stats
usage:
./migrate_stats.pl
=cut

# https://developer.matomo.org/guides/database-schema#log-data-persistence-action-types
# Piwik\Tracker\Action::TYPE_PAGE_URL = 1: the action is a URL to a page on the website being tracked.
# Piwik\Tracker\Action::TYPE_OUTLINK = 2: the action is a URL is of a link on the website being tracked. A visitor clicked it.
# Piwik\Tracker\Action::TYPE_DOWNLOAD = 3: the action is a URL of a file that was downloaded from the website being tracked.
# Piwik\Tracker\Action::TYPE_PAGE_TITLE = 4: the action is the page title of a page on the website being tracked.

my $logconf = q(
  log4perl.category.MyLogger         = INFO, Logfile, Screen
 
  log4perl.appender.Logfile          = Log::Log4perl::Appender::File
  log4perl.appender.Logfile.filename = migrate_stats.log
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

sub anonymize_ip {
  my $ipaddress = shift;

  my $ip = new Net::IP($ipaddress);

  unless ($ip) {
    $log->error(Net::IP::Error());
    return '0.0.0.0';
  }

  return ip_bintoip(substr($ip->binip(), 0, 24) . '0' x 8,  $ip->version()) if $ip->version() eq 4;
  return ip_bintoip(substr($ip->binip(), 0, 48) . '0' x 80, $ip->version()) if $ip->version() eq 6;
  return '0.0.0.0';
}

sub create_visitor_id {
    my ($ipaddress, $day) = @_;

    my $hashed_ip = sha256_hex($ipaddress);
    my $visitor_id = sha256_hex($hashed_ip . $day);

    return $visitor_id;
}

my $cnf = Config::JSON->new(pathToFile => 'config.json');
$cnf = $cnf->{config};

my $fromdb = DBI->connect($cnf->{matomodb}->{dsn}, $cnf->{matomodb}->{user}, $cnf->{matomodb}->{pass}, { RaiseError => 1}) or die $DBI::errstr;
my $todb = DBI->connect($cnf->{migrationdb}->{dsn}, $cnf->{migrationdb}->{user}, $cnf->{migrationdb}->{pass}, { RaiseError => 1}) or die $DBI::errstr;

my $fromsth;
my $tosth;
my $rv;

my @siteids = @{$cnf->{siteids}};

$log->info("Init tables...");
for my $idsite (@siteids) {
  $rv = $todb->do("
    CREATE TABLE IF NOT EXISTS `usage_stats_" . $idsite . "` (
      `id` int(11) NOT NULL AUTO_INCREMENT,
      `action` ENUM('info','preview','get','download'),
      `pid` varchar(4096) DEFAULT NULL,
      `ip` char(64) DEFAULT NULL,
      `visitor_id` char(64) DEFAULT NULL,
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
  $log->info("Updating info table $idsite...");

  $tosth = $todb->prepare("
    INSERT INTO `usage_stats_" . $idsite . "`(action, pid, ip, location_country, created, updated, visitor_id)
    VALUES (?, ?, ?, ?, ?, ?, ?);
  ");
  return dbError($todb->errstr) unless $tosth;

#SHA2(piwik_log_visit.idvisitor, 256) AS visitor_id
  $fromsth = $fromdb->prepare("
    SELECT LOWER(REGEXP_SUBSTR(name, 'o:\\\\d+')), server_time, location_country, DATE_FORMAT(server_time, '%Y-%m-%d') AS visitday, INET6_NTOA(location_ip)
    FROM piwik_log_link_visit_action
      INNER JOIN piwik_log_action on piwik_log_action.idaction = piwik_log_link_visit_action.idaction_url
      INNER JOIN piwik_log_visit on piwik_log_visit.idvisit = piwik_log_link_visit_action.idvisit
    WHERE 
      piwik_log_action.type = 1 AND
      piwik_log_link_visit_action.idsite = ? AND
      piwik_log_action.name like '%/detail/o:%'
  ");
  return dbError($fromdb->errstr) unless $fromsth;
  return dbError($fromdb->errstr) unless $fromsth->execute($idsite);

  my ($pid, $ts, $loc, $visitday, $ip);
  $fromsth->bind_columns(\$pid, \$ts, \$loc, \$visitday, \$ip);

  my $ipa;
  my $visitor_id;

  while ($fromsth->fetch) {

    $ipa = anonymize_ip($ip);
    $visitor_id = create_visitor_id($ip, $visitday);

    eval {
     dbError($todb->errstr) unless $tosth->execute('info', $pid, $ipa, $loc, $ts, $ts, $visitor_id);
     }
  }

  $fromsth = $fromdb->prepare("
    SELECT LOWER(REGEXP_SUBSTR(name, 'o:\\\\d+')), server_time, location_country, DATE_FORMAT(server_time, '%Y-%m-%d') AS visitday, INET6_NTOA(location_ip)
    FROM piwik_log_link_visit_action
      INNER JOIN piwik_log_action on piwik_log_action.idaction = piwik_log_link_visit_action.idaction_url
      INNER JOIN piwik_log_visit on piwik_log_visit.idvisit = piwik_log_link_visit_action.idvisit
    WHERE 
      (((piwik_log_action.type = 3) AND (name like '%o:%')) OR ( (piwik_log_action.type = 1) AND ( (name like '%/download/o:%') OR (name like '%/open/o:%') OR (name like '%/downloadwebversion/o:%') OR (name like '%/openwebversion/o:%') ) )) AND
      piwik_log_link_visit_action.idsite = ?
  ");
  return dbError($fromdb->errstr) unless $fromsth;
  return dbError($fromdb->errstr) unless $fromsth->execute($idsite);

  $fromsth->bind_columns(\$pid, \$ts, \$loc, \$visitday, \$ip);
  while ($fromsth->fetch) {

    $ipa = anonymize_ip($ip);
    $visitor_id = create_visitor_id($ip, $visitday);
eval {
     dbError($todb->errstr) unless $tosth->execute('download', $pid, $ip, $loc, $ts, $ts, $visitor_id);
     }
  }
}

$log->info("Done.");

1;
