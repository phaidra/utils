#!/usr/bin/env perl

=pod

=head1 pidgen.pl -apihost -apiuser -apipass -volume -issue
  -apihost - phaidra api host
  -apiuser - phaidra api user
  -apipass - phaidra api pass
  -volume - journal volume
  -issue - journal issue
=cut

use strict;
use warnings;
use utf8;
use Data::Dumper;
use Log::Log4perl;
use Mojo::UserAgent;
use Mojo::File;
use Mojo::JSON qw(from_json encode_json decode_json);
use POSIX qw(strftime);

my $logconf = q(
  log4perl.category.MyLogger         = DEBUG, Logfile, Screen
 
  log4perl.appender.Logfile          = Log::Log4perl::Appender::File
  log4perl.appender.Logfile.filename = /var/log/phaidra/pidgen.log
  log4perl.appender.Logfile.layout   = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Logfile.layout.ConversionPattern=%d %m%n
 
  log4perl.appender.Screen         = Log::Log4perl::Appender::Screen
  log4perl.appender.Screen.stderr  = 0
  log4perl.appender.Screen.layout  = Log::Log4perl::Layout::PatternLayout
  log4perl.appender.Screen.layout.ConversionPattern=%d %m%n
);

Log::Log4perl::init( \$logconf );
my $log = Log::Log4perl::get_logger("MyLogger");

my $apihost;
my $apiuser;
my $apipass;
my $volume;
my $issue;

while (defined (my $arg= shift (@ARGV)))
{
  if ($arg =~ /^-/)
  {
  	   if ($arg eq '-apihost') { $apihost = shift (@ARGV); }
  	elsif ($arg eq '-apiuser') { $apiuser = shift (@ARGV); }
    elsif ($arg eq '-apipass') { $apipass = shift (@ARGV); }
    elsif ($arg eq '-volume') { $volume = shift (@ARGV); }
    elsif ($arg eq '-issue') { $issue = shift (@ARGV); }
    else { 
      print "$log->error: bad parameters.\n";
      exit (0); 
    }
  }
}

unless(
    defined ($apihost) || 
    defined ($apiuser) || 
    defined ($apipass) ||
    defined ($volume) ||
    defined ($issue)
  ){
	print  "$log->error: Missing parameters.\n";
	exit (0);
}

my $ua = Mojo::UserAgent->new;
my $res;
my $apiurl = "https://$apiuser:$apipass\@$apihost";

$log->$log->info("started");

my $crfilepath = Mojo::File->new('crossref.json');
my $crossref = from_json $crfilepath->slurp;

for my $o (@{$crossref->{message}->{items}}) {

  next unless (defined($o->{volume})) && ($o->{volume} eq $volume);
  next unless (defined($o->{issue})) && ($o->{issue} eq $issue);
  next unless ($o->{type} eq "article-journal" || $o->{type} eq "journal-article"); # give me a break?
  next if ($o->{DOI} eq '10.31751/966');

  # http://dx.doi.org/10.31751/778
  my $doi = $o->{DOI};
  my $id = '';
  if($doi =~ m/(.+)\/(\d+)$/g){
    $id = $2;
  }
  $log->$log->info("[$doi] id: ".$id);

  my $dom = $ua->get("https://www.gmth.de/zeitschrift/artikel/$id.aspx")->result->dom;
  
  my $language = ''; # citation_language
  my $languagenode = $dom->at('meta[name="citation_language"]');
  if($languagenode){
    $language = $languagenode->attr('content');
    #$log->debug("[$doi] language: ".$language);
  }

  my $abstract = ''; # citation_abstract
  my $abstractnode = $dom->at('meta[name="citation_abstract"]');
  if($abstractnode){
    $abstract = $abstractnode->attr('content');
    #$log->debug("[$doi] abstract: ".$abstract);
  }
  my $uwmetadata = getUwmetadata($o, $abstract, $language);
  
  $log->debug("[$doi] uwmetadata: \n".Dumper($uwmetadata));

=cut
  my $asset = Mojo::Asset::File->new(path => $filepath);
  my $post = $ua->post("$apiurl/picture/create", form => { metadata => b(encode_json($uwmetadata))->decode('UTF-8'), file => { file => $asset }, mimetype => $mime });
  my $pid;
  if (my $res = $post->success) {
    $pid = $res->json->{pid};
  }else{
    $log->$log->error("[$filename] $log->error creating new object: ".Dumper($post->$log->error));
    next;
  }

  $log->info("[$filename] [$pid] upload successful");
    
  # add to parent
  addToCollection($config, $ua, $apiurl, $pid, $collectionpid);
=cut
}

#$log->info(Dumper(\@objects));

sub addToCollection($$$$){
  my $config = shift;
  my $ua = shift;
  my $apiurl = shift;
  my $pid = shift;
  my $collpid = shift;

  my $members = {
    "metadata" => {
      "members" => [ 
        { "pid" => $pid }
      ] 
    }
  };
  my $post_members = $ua->post("$apiurl/collection/$collpid/members/add" => form => { metadata => b(encode_json($members))->decode('UTF-8') });
  if (my $res = $post_members->success) {
    $log->info("[$pid] added to collection [$collpid]");
  }else{
    $log->error("[$pid] to collection [$collpid]: ".Dumper($post_members->$log->error));
  }
}

sub getUwmetadata($$) {
  my $o = shift;
  my $abstract = shift;
  my $language = shift;

  $language = 'xx' if $language eq '';

  my %md = (
  "metadata" => {
    "uwmetadata" => [
      {
          "datatype" => "Node",
          "input_type" => "node",
          "xmlname" => "general",
          "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0",
          "children"=> [
              {
                  "attributes" => [
                      {
                          "input_type" => "select",
                          "ui_value" => $language,
                          "xmlname" => "lang"
                      }
                  ],
                  "datatype" => "LangString",
                  "input_type" => "input_text",
                  "ui_value" => $o->{title}[0],
                  "xmlname" => "title",
                  "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
              },
              {
                  "datatype" => "Language",
                  "input_type" => "select",
                  "ui_value" => $language,
                  "xmlname" => "language",
                  "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
              },
              {
                  "attributes" => [
                      {
                          "input_type" => "select",
                          "ui_value" => "xx",
                          "xmlname" => "lang"
                      }
                  ],
                  "datatype" => "LangString",
                  "input_type" => "input_text",
                  "ui_value" => $abstract,
                  "xmlname" => "description",
                  "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
              },
              {
                "xmlns": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0",
                "xmlname": "identifiers",
                "input_type": "node",
                "datatype": "Node"
                "children": [
                  {
                    "xmlns": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0",
                    "xmlname": "resource",
                    "input_type": "select",
                    "ui_value": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552101",
                    "datatype": "Vocabulary"
                  },
                  {
                    "xmlns": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0",
                    "xmlname": "identifier",
                    "input_type": "input_text",
                    "ui_value": $o->{ISSN}[0],
                    "datatype": "CharacterString"
                  }
                ],
          
              }
          ]
      },
      {
        "datatype" => "Node",
        "input_type" => "node",
        "xmlname" => "lifecycle",
        "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0",
        "children" => []
      },
      {
          "children" => [		           
            {
                "datatype" => "License",
                "input_type" => "select",
                "ui_value" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/voc_21/16",
                "xmlname" => "license",
                "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
            }
          ],
          "datatype" => "Node",
          "input_type" => "node",
          "xmlname" => "rights",
          "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
      },
      {
          "datatype" => "Node",
          "input_type" => "node",
          "xmlname" => "classification",
          "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
      },
      {
          "loaded_value_lang": "",
          "xmlns": "http://phaidra.univie.ac.at/XML/metadata/digitalbook/V1.0",
          "disabled": 0,
          "loaded": 0,
          "xmlname": "digitalbook",
          "children": [
            {
              "xmlns": "http://phaidra.univie.ac.at/XML/metadata/digitalbook/V1.0",              
              "xmlname": "name_magazine",
              "input_type": "input_text_lang",
              "ui_value": $o->{container-title},
              "datatype": "LangString"
            },
            {
              "xmlns": "http://phaidra.univie.ac.at/XML/metadata/digitalbook/V1.0",              
              "xmlname": "volume",
              "input_type": "input_text",
              "ui_value": $o->{volume},
              "datatype": "CharacterString"
            },
            {
              "xmlns": "http://phaidra.univie.ac.at/XML/metadata/digitalbook/V1.0",              
              "xmlname": "booklet",
              "input_type": "input_text",
              "ui_value": $o->{issue},
              "datatype": "CharacterString"
            },
            {
              "xmlns": "http://phaidra.univie.ac.at/XML/metadata/digitalbook/V1.0",              
              "xmlname": "publisher",
              "input_type": "input_text",
              "ui_value": $o->{publisher},
              "datatype": "CharacterString"
            },
            {
              "xmlns": "http://phaidra.univie.ac.at/XML/metadata/digitalbook/V1.0",              
              "xmlname": "releaseyear",
              "input_type": "input_datetime",
              "ui_value": $o->{issued}->{date-parts}[0][0],
              "datatype": "DateTime"
            }
		      ]
      }
	} 
  );

  if($o->{author}){
    my $i = 0;
    for my $author (@{$o->{author}}){

    my $cont = {
      "datatype" => "Node",
      "input_type" => "node",
      "xmlname" => "contribute",
      "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0",
      "attributes" => [
          {
              "input_type" => "input_text",
              "ui_value" => "1",
              "xmlname" => "data_order"
          }
      ],
      "children" => [
          {
              "datatype" => "Vocabulary",
              "input_type" => "select",
              "ui_value" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/voc_3/46",
              "xmlname" => "role",
              "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
          },
          {
            "datatype" => "Node",
            "input_type" => "node",
            "xmlname" => "entity",
            "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0",
            "attributes" => [
                {
                    "input_type" => "input_text",
                    "ui_value" => $i,
                    "xmlname" => "data_order"
                }
            ],
            "children" => [
                {
                    "datatype" => "CharacterString",
                    "input_type" => "input_text",
                    "ui_value" => $author->{given},
                    "xmlname" => "firstname",
                    "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/entity"
                },
                {
                    "datatype" => "CharacterString",
                    "input_type" => "input_text",
                    "ui_value" => $author->{family},
                    "xmlname" => "lastname",
                    "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/entity"
                }
            ]                          
          }
      ]
    };

    for my $n (@{$md{metadata}{uwmetadata}}){
      if($n->{xmlname} eq 'lifecycle'){
        push @{$n->{children}}, $cont;
      }
    }

    $i++;
  }

  return \%md;
}

sub getColUwmetadata($$) {
  my $config = shift;
  my $name = shift;

  my %md = (
    "metadata" => {
		  "uwmetadata" => [
		    {
		        "children"=> [
		            {
		                "attributes" => [
		                    {
		                        "input_type" => "select",
		                        "ui_value" => "de",
		                        "xmlname" => "lang"
		                    }
		                ],
		                "datatype" => "LangString",
		                "input_type" => "input_text",
		                "ui_value" => $name,
		                "xmlname" => "title",
		                "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
		            },		            
		            {
		                "datatype" => "Language",
		                "input_type" => "select",
		                "ui_value" => "de",
		                "xmlname" => "language",
		                "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
		            },
		            {
		                "attributes" => [
		                    {
		                        "input_type" => "select",
		                        "ui_value" => "de",
		                        "xmlname" => "lang"
		                    }
		                ],
		                "datatype" => "LangString",
		                "input_type" => "input_text",
		                "ui_value" => $name,
		                "xmlname" => "description",
		                "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
		            }
		        ],
		        "datatype" => "Node",
		        "input_type" => "node",
		        "xmlname" => "general",
		        "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
		    },
		    {
          "datatype" => "Node",
          "input_type" => "node",
          "xmlname" => "lifecycle",
          "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0",
          "children" => [
		            {
                  "datatype" => "Node",
                  "input_type" => "node",
                  "xmlname" => "contribute",
                  "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0",
                  "attributes" => [
                      {
                          "input_type" => "input_text",
                          "ui_value" => "0",
                          "xmlname" => "data_order"
                      }
                  ],
                  "children" => [
                      {
                          "datatype" => "Vocabulary",
                          "input_type" => "select",
                          "ui_value" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/voc_3/46",
                          "xmlname" => "role",
                          "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
                      },
                      {
                        "datatype" => "Node",
                        "input_type" => "node",
                        "xmlname" => "entity",
                        "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0",
                        "attributes" => [
                            {
                                "input_type" => "input_text",
                                "ui_value" => "0",
                                "xmlname" => "data_order"
                            }
                        ],
                        "children" => [
                            {
                                "datatype" => "CharacterString",
                                "input_type" => "input_text",
                                "ui_value" => "Franz",
                                "xmlname" => "firstname",
                                "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/entity"
                            },
                            {
                                "datatype" => "CharacterString",
                                "input_type" => "input_text",
                                "ui_value" => "Sachslehner",
                                "xmlname" => "lastname",
                                "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/entity"
                            }
                        ]            
                      }
                  ]
		            }
          ]
		    },
		    {
		        "children" => [      
		            {
		                "datatype" => "License",
		                "input_type" => "select",
		                "ui_value" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/voc_21/16",
		                "xmlname" => "license",
		                "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
		            }
            ],
		        "datatype" => "Node",
		        "input_type" => "node",
		        "xmlname" => "rights",
		        "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
		    },
		    {
		        "datatype" => "Node",
		        "input_type" => "node",
		        "xmlname" => "classification",
		        "xmlns" => "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0"
		    }
      ]
    }
  );

  return \%md;
}

sub getColMembers($$$$) {
  my $config = shift;
  my $ua = shift;
  my $apiurl = shift;
  my $pid = shift;

  my $get = $ua->get("$apiurl/collection/$pid/members");
  if ($res = $get->success) {
    return $res->json->{metadata}->{members};
  }else{
    $log->error("[$pid] $log->error getting collection members: ".Dumper($get->$log->error));
  }
}

exit 0;
