#!/usr/bin/env perl

use utf8;
use strict;
use warnings;
use Data::Dumper;
$Data::Dumper::Indent= 1;
use Mojo::File;
use Mojo::ByteStream qw(b);
use Mojo::Util qw(url_escape);
use Mojo::JSON qw(encode_json decode_json);
use Mojo::UserAgent;
use Mojo::URL;

=pod

=head1 fix_20207_role_and_id.pl -pidfile pidfile -username user -password pass -baseurl phaidra-sandbox.univie.ac.at

  -pidfile pidfile -username -password -baseurl

=cut

sub fixMetadata($$){
  my $pid = shift;
  my $md = shift;
  my @gnds;
  for my $top (@{$md->{metadata}->{uwmetadata}}){
    for my $ch (@{$top->{children}}){
      if ($ch->{xmlname} eq 'identifiers') {
        for my $id (@{$ch->{children}}){
          if ($id->{xmlname} eq 'identifier') {
            if ($id->{ui_value} =~ m/http:\/\/d-nb\.info\/gnd\/(.+)$/) {
              push @gnds, $1;
              $id->{ui_value} = "";
            }
          }
        }
      }
    }

    if ($top->{xmlname} eq 'lifecycle') {
      for my $ch (@{$top->{children}}){
        if ($ch->{xmlname} eq 'contribute') {
          for my $contrchild (@{$ch->{children}}){
            if ($contrchild->{xmlname} eq 'entity') {
              for my $entitychild (@{$contrchild->{children}}){
                if ($entitychild->{xmlname} eq 'institution') {
                  if($entitychild->{ui_value} eq 'Universität Wien, DLE Bibliotheks- und Archivwesen: Sammlungen'){
                    for my $contrchild (@{$ch->{children}}){
                      if ($contrchild->{xmlname} eq 'role') {
                        $contrchild->{ui_value} = 'http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/voc_3/1552154';
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  # get histkult node
  my $histkult;
  for my $top (@{$md->{metadata}->{uwmetadata}}){
    if ($top->{xmlname} eq 'histkult') {
      $histkult = $top;
    }
  }

  unless($histkult) {
    $histkult = {
      datatype => "Node",
      input_type => "node",
      xmlname => "histkult",
      xmlns => "http://phaidra.univie.ac.at/XML/metadata/histkult/V1.0",
      children => []
    };
    push @{$md->{metadata}->{uwmetadata}}, $histkult;
  }

  for my $gnd (@gnds) {
    push @{$histkult->{children}}, {
      datatype => "Node",
      input_type => "node",
      xmlname => "reference_number",
      xmlns => "http://phaidra.univie.ac.at/XML/metadata/histkult/V1.0",
      children => [
        {
          datatype => "Vocabulary",
          input_type => "select",
          ui_value => "http://phaidra.univie.ac.at/XML/metadata/histkult/V1.0/voc_25/1562802",
          xmlname => "reference",
          xmlns => "http://phaidra.univie.ac.at/XML/metadata/histkult/V1.0"
        },
        {
          datatype => "CharacterString",
          input_type => "input_text",
          ui_value => $gnd,
          xmlname => "number",
          xmlns => "http://phaidra.univie.ac.at/XML/metadata/histkult/V1.0"
        }
      ]
    }
  }
}

my $pidsfile;
my $username;
my $password;
my $baseurl;
while (defined (my $arg= shift (@ARGV)))
{
  if ($arg =~ /^-/)
  {
    if ($arg eq '-pidsfile') { $pidsfile = shift (@ARGV); }
    elsif ($arg eq '-username') { $username = shift (@ARGV); }
    elsif ($arg eq '-password') { $password = shift (@ARGV); }
    elsif ($arg eq '-baseurl') { $baseurl = shift (@ARGV); }
    else { system ("perldoc '$0'"); exit (0); }
  }
}

my $path = Mojo::File->new($pidsfile);
my $bytes = $path->slurp;
my $pids = decode_json($bytes);

for my $pid (@{$pids->{pids}}){

  my $url = "https://$username:".url_escape($password).'@'."services.$baseurl/api/object/$pid/uwmetadata";

  print "[".scalar localtime."] pid=[$pid] url[$url]\n";

  print "[".scalar localtime."] pid=[$pid] ", "processing \n";

  my $ua = Mojo::UserAgent->new;
  my $getres = $ua->get($url)->result;
  
  if($getres->is_error){
    print "[".scalar localtime."] pid=[$pid] ", "ERROR getting UWMETADATA: ".$getres->code." ".$getres->message."\n";
    if($getres->json){
      print Dumper($getres->json)."\n";
    }
    next;
  }

  if ($getres->is_success) {
    my $md = $getres->json;

    fixMetadata($pid, $md);

    my $json = b(encode_json($md))->decode('UTF-8');

    my $postres = $ua->post($url => form => { metadata => $json })->result;

    if ($postres->is_success) {
      print "[".scalar localtime."] pid=[$pid] ", "success \n";
    }else{
      print "[".scalar localtime."] pid=[$pid] ", "ERROR saving UWMETADATA:".$postres->code." ".$postres->message."\n";
      if($postres->json){
        print Dumper($postres->json)."\n";
      }
    }

  }else{
    print "[".scalar localtime."] pid=[$pid] ", "ERROR getting UWMETADATA\n";
  }

}