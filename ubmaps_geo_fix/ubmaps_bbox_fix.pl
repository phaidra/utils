#!/usr/bin/env perl

use strict;
use warnings;
use Data::Dumper;
$Data::Dumper::Indent= 1;
use Mojo::Util qw(slurp);
use Mojo::JSON qw(encode_json decode_json);
use Mojo::UserAgent;
use Mojo::URL;


=pod

=head1 ubmaps_bbox_fix.pl -f pidfile

  -f pidfile

=cut

my $pidsfile;
while (defined (my $arg= shift (@ARGV)))
{
  if ($arg =~ /^-/)
  {
  	   if ($arg eq '-f') { $pidsfile = shift (@ARGV); }
    else { system ("perldoc '$0'"); exit (0); }
  }
}


my $bytes = slurp $pidsfile;	
my $pids = decode_json($bytes);

for my $pid (@{$pids->{pids}}){	

	my $url = "https://user:pass".'@'."services.phaidra.univie.ac.at/api/object/$pid/geo";

	print "[".scalar localtime."] pid=[$pid] ", "processing \n"; 

	my $ua = Mojo::UserAgent->new;
	my $tx = $ua->get($url);	
  	if (my $res = $tx->success) {  		
  		#print Dumper($res)."\n";
  		my $md = $res->json;
  		for my $pm (@{$md->{metadata}->{geo}->{kml}->{document}->{placemark}}){
  			for my $coord (@{$pm->{polygon}->{outerboundaryis}->{linearring}->{coordinates}}){
  				my $lat = $coord->{latitude};
  				my $lon = $coord->{longitude};
  				$coord->{latitude} = $lon;
  				$coord->{longitude} = $lat;
  			}

  			my $json = encode_json($md);

  			my $ua2 = Mojo::UserAgent->new;
			$ua2->post($url => form => { metadata => $json });
			if (my $res = $tx->success) {  
				print "pid=[$pid] success \n";
			}else{
				print "[".scalar localtime."] pid=[$pid] ", "ERROR saving GEO:\n"; 
				if($tx->res->json){
					if($tx->res->json->{alerts}){
						print Dumper($tx->res->json->{alerts})."\n";
					}
				}
				print Dumper($tx->error)."\n"; 		
				
			}

  		}
    	
	}else{
		print "[".scalar localtime."] pid=[$pid] ", "ERROR getting GEO:\n"; 
		if($tx->res->json){
			if($tx->res->json->{alerts}){
				print Dumper($tx->res->json->{alerts})."\n";
			}
		}
		print Dumper($tx->error)."\n"; 		
		
	}


}