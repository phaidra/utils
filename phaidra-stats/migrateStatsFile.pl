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

my $logconf = q(
  log4perl.category.MyLogger         = DEBUG, Logfile, Screen

  log4perl.appender.Logfile          = Log::Log4perl::Appender::File
  log4perl.appender.Logfile.filename = migrate_stats.log
  log4perl.appender.Logfile.layout   = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Logfile.layout.ConversionPattern=%d %p %m%n
  log4perl.appender.Logfile.utf8     = 1

  log4perl.appender.Screen           = Log::Log4perl::Appender::Screen
  log4perl.appender.Screen.stderr    = 0
  log4perl.appender.Screen.layout    = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Screen.layout.ConversionPattern=%d %p %m%n
  log4perl.appender.Screen.utf8      = 1
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

my $fromdb = DBI->connect($cnf->{matomodb}->{dsn}, $cnf->{matomodb}->{user}, $cnf->{matomodb}->{pass}, { RaiseError => 1, PrintError => 0 }) or die $DBI::errstr;

my $fromsth;

my @siteids = @{$cnf->{siteids}};
my $batch_size = 1000;  # Adjust this as needed

$log->info("Init tables...");
for my $idsite (@siteids) {
  my $sql_filename = "usage_stats_${idsite}.sql";
  open my $sql_file, '>', $sql_filename or die "Could not open file '$sql_filename': $!";

  print $sql_file "CREATE TABLE IF NOT EXISTS `usage_stats_${idsite}` (\n";
  print $sql_file "  `id` int(11) NOT NULL AUTO_INCREMENT,\n";
  print $sql_file "  `action` ENUM('info','preview','get','download'),\n";
  print $sql_file "  `pid` varchar(4096) DEFAULT NULL,\n";
  print $sql_file "  `ip` char(64) DEFAULT NULL,\n";
  print $sql_file "  `visitor_id` char(64) DEFAULT NULL,\n";
  print $sql_file "  `location_country` char(3) DEFAULT NULL,\n";
  print $sql_file "  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n";
  print $sql_file "  `updated` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n";
  print $sql_file "  PRIMARY KEY (`id`),\n";
  print $sql_file "  KEY `index_pid` (`pid`)\n";
  print $sql_file ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n\n";

  $log->info("Writing info for table $idsite...");

  my $action_type_queries = [
    { type => 'info', condition => "piwik_log_action.type = 1 AND piwik_log_action.name like '%/detail/o:%'" },
    { type => 'download', condition => "(((piwik_log_action.type = 3) AND (name like '%o:%')) OR ( (piwik_log_action.type = 1) AND ( (name like '%/download/o:%') OR (name like '%/open/o:%') OR (name like '%/downloadwebversion/o:%') OR (name like '%/openwebversion/o:%') ) ))" }
  ];

  foreach my $query_info (@$action_type_queries) {
    my $type = $query_info->{type};
    my $condition = $query_info->{condition};
    $log->info("query info: " . $type);
    $fromsth = $fromdb->prepare("
      SELECT LOWER(REGEXP_SUBSTR(name, 'o:\\\\d+')), server_time, location_country, DATE_FORMAT(server_time, '%Y-%m-%d') AS visitday, INET6_NTOA(location_ip)
      FROM piwik_log_link_visit_action
        INNER JOIN piwik_log_action ON piwik_log_action.idaction = piwik_log_link_visit_action.idaction_url
        INNER JOIN piwik_log_visit ON piwik_log_visit.idvisit = piwik_log_link_visit_action.idvisit
      WHERE 
        $condition AND piwik_log_link_visit_action.idsite = ?
    ");

    if (!$fromsth) {
      $log->error("Failed to prepare statement: " . $fromdb->errstr);
      next;
    }

    if (!$fromsth->execute($idsite)) {
      $log->error("Failed to execute statement: " . $fromdb->errstr);
      next;
    }

    my ($pid, $ts, $loc, $visitday, $ip);
    $fromsth->bind_columns(\$pid, \$ts, \$loc, \$visitday, \$ip);

    my @values;
    while ($fromsth->fetch) {
      if (!defined $pid || !defined $ts || !defined $loc || !defined $visitday || !defined $ip) {
        $log->warn("Some column values are undefined, skipping this row.");
        next;
      }

      my $ipa = anonymize_ip($ip);
      my $visitor_id = create_visitor_id($ip, $visitday);

      push @values, "('$type', '$pid', '$ipa', '$loc', '$ts', '$ts', '$visitor_id')";

      if (@values == $batch_size) {
        print $sql_file "INSERT INTO `usage_stats_${idsite}`(action, pid, ip, location_country, created, updated, visitor_id) VALUES " . join(", ", @values) . ";\n";
        @values = ();
      }
    }

    # Handle the remaining values
    if (@values) {
      print $sql_file "INSERT INTO `usage_stats_${idsite}`(action, pid, ip, location_country, created, updated, visitor_id) VALUES " . join(", ", @values) . ";\n";
    }

  }
  $log->info("closing $sql_filename");
  close $sql_file;
}

$log->info("Done.");

1;
