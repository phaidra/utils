#!/usr/bin/perl -w


#OK!

use strict;
use warnings;

use Data::Dumper;

use YAML::Syck;
use HTTP::Request::Common qw{ POST };
use JSON; # imports encode_json, decode_json, to_json and from_json.
use Mojo::Base 'Mojolicious';
use Mojo::ByteStream qw(b);
use utf8;
use Encode;





my $file = '/my/path/file.csv';


my $config = YAML::Syck::LoadFile("/etc/phaidra.yml");

my $fedoraadminuser  = $config->{"fedoraadminuser"};
my $fedoraadminpass = $config->{"fedoraadminpass"};
my $phaidraapibaseurl  = $config->{"phaidraapibaseurl"};

my @base = split('/',$phaidraapibaseurl);
my $scheme = "https";

#use sandbox API
#$fedoraadminpass = 'xxxxxx';
#$base[0] = 'services.phaidra-sandbox.univie.ac.at';

#use local API
#$base[0] = '127.0.0.1:3000';
#$base[1] = '';
#$scheme = "http";


#  o:15992 on u:scholar-sandbox

print $phaidraapibaseurl,"\n";


my $json_identifier;
$json_identifier = '{
        "data_order": "",
        "input_type": "node",
        "value_lang": "",
        "loaded_ui_value": "",
        "labels": {
                "en": "Identifiers",
                "de": "Identifikatoren",
                "it": "Identificatori",
                "sr": "identifikatori"
        },
        "loaded_value": "",
        "ordered": 0,
        "field_order": 10,
        "children": [{
                "vocabularies": [{
                        "terms": [{
                                "labels": {
                                        "en": "HTTP/WWW",
                                        "de": "HTTP/WWW",
                                        "sr": "HTTP/WWW",
                                        "it": "HTTP/WWW"
                                },
                                "uri": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552102"
                        }, {
                                "uri": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552099",
                                "labels": {
                                        "de": "DOI",
                                        "it": "DOI",
                                        "sr": "DOI",
                                        "en": "DOI"
                                }
                        }, {
                                "labels": {
                                        "it": "eISSN",
                                        "de": "eISSN",
                                        "en": "eISSN"
                                },
                                "uri": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552256"
                        }, {
                                "labels": {
                                        "de": "PI",
                                        "it": "PI",
                                        "sr": "PI",
                                        "en": "PI"
                                },
                                "uri": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552105"
                        }, {
                                "uri": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552104",
                                "labels": {
                                        "de": "ISMN",
                                        "sr": "ISMN",
                                        "it": "ISMN",
                                        "en": "ISMN"
                                }
                        }, {
                                "labels": {
                                        "en": "ISSN",
                                        "sr": "ISSN",
                                        "it": "ISSN",
                                        "de": "ISSN"
                                },
                                "uri": "http:// phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552101"
                        }, {
                                "uri": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552100",
                                "labels": {
                                        "en": "ISBN",
                                        "it": "ISBN",
                                        "sr": "ISBN",
                                        "de": "ISBN"
                                }
                        }, {
                                "uri": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552255",
                                "labels": {
                                        "it": "PrintISSN",
                                        "de": "PrintISSN",
                                        "en": "PrintISSN"
                                }
                        }, {
                                "uri": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552103",
                                "labels": {
                                        "sr": "URN",
                                        "it": "URN",
                                        "de": "URN",
                                        "en": "URN"
                                }
                        }, {
                                "uri": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552151",
                                "labels": {
                                        "it": "Numero-AC",
                                        "sr": "AC-broj",
                                        "de": "AC-Nummer",
                                        "en": "AC-Number"
                                }
                        }],
                        "description": "Universität Wien Objektidentifikatoren",
                        "namespace": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/"
                }],
                "xmlns": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0",
                "disabled": 0,
                "xmlname": "resource",
                "datatype": "Vocabulary",
                "loaded_value": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552256",
                "field_order": 1,
                "ordered": 0,
                "cardinality": "1",
                "value_lang": "",
                "loaded_ui_value": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552256",
                "labels": {
                        "de": "Quelle",
                        "it": "Standard",
                        "sr": "izvor",
                        "en": "Resource"
                },
                "data_order": "",
                "input_type": "select",
                "hidden": 0,
                "loaded": 1,
                "input_regex": "^.*$",
                "value": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552256",
                "help_id": "helpmeta_124",
                "loaded_value_lang": "",
                "ui_value": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552256",
                "mandatory": 0
        }, {
                "input_type": "input_text",
                "data_order": "",
                "value_lang": "",
                "labels": {
                        "en": "Identifier",
                        "de": "Identifikator",
                        "it": "Identificatore",
                        "sr": "identifikator"
                },
                "loaded_ui_value": "1873-3468",
                "field_order": 2,
                "ordered": 0,
                "loaded_value": "1873-3468",
                "cardinality": "1",
                "xmlns": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0",
                "disabled": 0,
                "datatype": "CharacterString",
                "xmlname": "identifier",
                "ui_value": "1873-3468",
                "mandatory": 0,
                "value": "",
                "loaded_value_lang": "",
                "help_id": "helpmeta_125",
                "input_regex": "^.*$",
                "loaded": 1,
                "hidden": 0
        }],
        " cardinality": "*",
        "xmlns": "http://phaidra.univie.ac.at/XML/metadata/extended/V1.0",
        "disabled": 0,
        "xmlname": "identifiers",
        "datatype": "Node",
        "ui_value": "",
        "mandatory": 0,
        "value": "",
        "help_id": "helpmeta_123",
        "loaded_value_lang": "",
        "hidden": 0,
        "input_regex": "",
        "loaded": 1
}';

my $json_bytes = encode('UTF-8', $json_identifier);
my $identifier_hash = JSON->new->utf8->decode($json_bytes);




sub updatedDOIAndURL {

   my $pid = shift;
   my $new_doi = shift;
   my $new_url = shift;

   if((defined $new_doi && $new_doi ne "") || (defined $new_url && $new_url ne "") ){

      my $url = Mojo::URL->new;
      $url->scheme($scheme);
      $url->userinfo("$fedoraadminuser:$fedoraadminpass");
      $url->host($base[0]);
  
      if(exists($base[1])){
         $url->path($base[1]."/object/$pid/uwmetadata");
      }else{
         $url->path("/object/$pid/uwmetadata/");
      }
      $url->userinfo("$fedoraadminuser:$fedoraadminpass");
      my $ua = Mojo::UserAgent->new;
       
      print "updatedDOIAndURL url get:", $url,"\n";  
      my $tx = $ua->get($url);
      if (my $res1 = $tx->success) {       
          my $objectHasURL = 0;
          my $objectHasDOI = 0;
          my $adviserString = '';
          my @adviserArray;
          my $content = $res1->json;
          
          if(defined $content->{metadata}->{uwmetadata}[0]->{children}){ # children in general
            for (my $j=0; $j <= (scalar @{$content->{metadata}->{uwmetadata}[0]->{children}}) - 1; $j++) {
               if(defined $content->{metadata}->{uwmetadata}[0]->{children}[$j]->{xmlname}){
                  if( $content->{metadata}->{uwmetadata}[0]->{children}[$j]->{xmlname} eq 'identifiers' ){                            
                            my $IsDOI = 0;
                            my $IsURL = 0;
                            my $arrayLenght = (scalar @{$content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}}) - 1;
                            for(my $k = 0; $k <=  $arrayLenght; $k++){ # children in identifiers
                                if(defined $content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{xmlname} && defined $content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{ui_value}){
                                        if($content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{xmlname} eq 'resource' && $content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{ui_value} eq 'http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552099'){
                                                $IsDOI = 1;
                                                $objectHasDOI = 1;
                                        }
                                        if($content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{xmlname} eq 'resource' && $content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{ui_value} eq 'http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552102'){
                                                $IsURL = 1;
                                                $objectHasURL = 1;
                                        }
                                        # http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552102
                                        if($content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{xmlname} eq 'identifier'){
                                            if($IsDOI == 1){
                                                  my $OldDOIValue = $content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{ui_value};
                                                  print "DOI value:",$OldDOIValue, "\n";
                                                  $content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{ui_value} = $new_doi if(defined $new_doi && $new_doi ne "");
                                            }
                                            if($IsURL == 1){
                                                  my $OldURIValue = $content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{ui_value};
                                                  print "URL old value:",$OldURIValue, "\n";
                                                  if($new_url eq 'DELETEURL' || $new_url eq '\'DELETEURL\''){
                                                        print "deleting j:", $j, "\n";
                                                        splice @{$content->{metadata}->{uwmetadata}[0]->{children}}, $j, 1;
 
                                                  }else{
                                                        print "Updating uri with new value:", $new_url, "\n";
                                                        $content->{metadata}->{uwmetadata}[0]->{children}[$j]->{children}[$k]->{ui_value} = $new_url if(defined $new_url && $new_url ne "");
                                                  }
                                            }
                                       }
                                }
                          }
                  }
               }
            }
          }
          if(!$objectHasDOI){
               if(defined $new_doi && $new_doi ne ""){
                    $identifier_hash->{children}[0]->{loaded_value}    = 'http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552099';
                    $identifier_hash->{children}[0]->{loaded_ui_value} = 'http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552099';
                    $identifier_hash->{children}[0]->{value}           = 'http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552099';
                    $identifier_hash->{children}[0]->{ui_value}        = 'http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552099';
 
                    $identifier_hash->{children}[1]->{loaded_value}    = $new_doi;
                    $identifier_hash->{children}[1]->{loaded_ui_value} = $new_doi;
                    $identifier_hash->{children}[1]->{value}           = $new_doi;
                    $identifier_hash->{children}[1]->{ui_value}        = $new_doi;
      
                    push $content->{metadata}->{uwmetadata}[0]->{children}, $identifier_hash;
               }
          }
          if(!$objectHasURL){
               if(defined $new_url && $new_url ne ""){
                    $identifier_hash->{children}[0]->{loaded_value}    = 'http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552102';
                    $identifier_hash->{children}[0]->{loaded_ui_value} = 'http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552102';
                    $identifier_hash->{children}[0]->{value}           = 'http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552102';
                    $identifier_hash->{children}[0]->{ui_value}        = 'http://phaidra.univie.ac.at/XML/metadata/extended/V1.0/voc_31/1552102';
 
                    $identifier_hash->{children}[1]->{loaded_value}    = $new_url;
                    $identifier_hash->{children}[1]->{loaded_ui_value} = $new_url;
                    $identifier_hash->{children}[1]->{value}           = $new_url;
                    $identifier_hash->{children}[1]->{ui_value}        = $new_url;
      
                    push $content->{metadata}->{uwmetadata}[0]->{children}, $identifier_hash;
              }
         }
       
         my $json_str = b(encode_json({ metadata => { uwmetadata => $content->{metadata}->{uwmetadata} } }))->decode('UTF-8');
         print "updatedDOIAndURL url post:", $url,"\n";    
         my $tx2 = $ua->post($url => form => { metadata => $json_str } );
         if (my $res2 = $tx2->success) {
                 print Dumper("test_deleteme post success.", $tx2->res->json);
         }else {
                 if(defined($tx2->res->json)){
                      if(exists($tx2->res->json->{alerts})) {
                           print Dumper("Error test_deleteme for uwmetadata: alerts4: ",$tx2->res->json->{alerts});        
                      }else{
                           print Dumper("Error test_deleteme for uwmetadata: json2: ",$tx2->res->json);        
                      }
                 }else{
                           print Dumper("Error test_deleteme for uwmetadata2: ",$tx2->error);        
                 }
         }
      }else {
         print Dumper($tx);
      }

   }else{
       print "Excel INFO: Doi and Url are empty!\n";
   }


}


sub removePdfModel($){
  
     my $pid = shift;
     print Dumper($pid);
     #o:3414 o:3415 o:3416 on entw
     
     my $url = Mojo::URL->new;
     $url->scheme($scheme);
     $url->host($base[0]);
     $url->userinfo("$fedoraadminuser:$fedoraadminpass");
     if(exists($base[1])){
             $url->path($base[1]."/object/$pid/relationship/remove");
     }else{
             $url->path("/object/$pid/relationship/remove");
     }
     print "removePdfModel url:", $url, "\n";
     
     my $ua = Mojo::UserAgent->new;
     my $tx = $ua->post($url => form => {
                                       predicate => 'info:fedora/fedora-system:def/model#hasModel',
                                       object => 'info:fedora/cmodel:PDFDocument',
                                   } );
     if (my $res = $tx->success) {
                 print Dumper("remove PdfModel success.", $tx->res->json);
     }else {
                 if(defined($tx->res->json)){
                      if(exists($tx->res->json->{alerts})) {
                           print Dumper("Error removePdfModel: alerts: ",$tx->res->json->{alerts});        
                      }else{
                           print Dumper("Error removePdfModel: json: ",$tx->res->json);        
                      }
                 }else{
                           print Dumper("Error removePdfModel: ",$tx->error);        
                 }
     }

}


sub addResource($){

        my $pid = shift;
        
        my $url = Mojo::URL->new;
        $url->scheme($scheme);
        $url->host($base[0]);
        $url->userinfo("$fedoraadminuser:$fedoraadminpass");
        if(exists($base[1])){
             $url->path($base[1]."/object/$pid/relationship/add");
        }else{
             $url->path("/object/$pid/relationship/add");
        }
        
        print "addResource url:", $url, "\n";
        
        my $ua = Mojo::UserAgent->new;
        my $tx = $ua->post($url => form => {
                                       predicate => 'info:fedora/fedora-system:def/model#hasModel',
                                       object => 'info:fedora/cmodel:Resource',
                                   } );
        if (my $res = $tx->success) {
                 print Dumper("add resource success.", $tx->res->json);
        }else {
                 if(defined($tx->res->json)){
                      if(exists($tx->res->json->{alerts})) {
                           print Dumper("Error addResource: alerts: ",$tx->res->json->{alerts});        
                      }else{
                           print Dumper("Error addResource: json: ",$tx->res->json);        
                      }
                 }else{
                           print Dumper("Error addResource: ",$tx->error);        
                 }
        }
}

sub addLink($$){
 
        my $pid = shift;
        my $link = shift;
        
        my $url = Mojo::URL->new;
        $url->scheme($scheme);
        $url->host($base[0]);
        $url->userinfo("$fedoraadminuser:$fedoraadminpass");
        if(exists($base[1])){
                $url->path($base[1]."/object/$pid/datastream/LINK");
        }else{
                $url->path("/object/$pid/datastream/LINK");
        }
        
        print "addLink url:", $url, "\n";
        
        my $ua = Mojo::UserAgent->new;
        my $tx = $ua->post($url => form => {
                                       mimetype => 'text/html',
                                       controlgroup => 'R',
                                       dslabel => 'Link to external resource',
                                       location => $link
                                   } );
        if (my $res = $tx->success) {
                 print Dumper("add link success.", $tx->res->json);
        }else {
                 if(defined($tx->res->json)){
                      if(exists($tx->res->json->{alerts})) {
                           print Dumper("Error addLink: alerts: ",$tx->res->json->{alerts});        
                      }else{
                           print Dumper("Error addLink: json: ",$tx->res->json);        
                      }
                 }else{
                           print Dumper("Error addLink: ",$tx->error);        
                 }
        }
}
 
sub deleteFormatNodeFromUWMeta($){

      my $pid = shift;

      
      my $url = Mojo::URL->new;
      $url->scheme($scheme);
      #$url->host($base[0]);
      $url->host('services.phaidra-sandbox.univie.ac.at');
      #$url->userinfo("$fedoraadminuser:$fedoraadminpass");
      $url->userinfo("PhaidraAdmin:h0yOJ29X");
      if(exists($base[1])){
                $url->path($base[1]."/object/$pid/uwmetadata");
      }else{
                $url->path("/object/$pid/uwmetadata");
      }
      print 'deleteFormatNodeFromUWMeta $url get:',$url,"\n";
      my $ua = Mojo::UserAgent->new;
      my $tx = $ua->get($url);
      my $content;
      if (my $res = $tx->success) {
                 #set format in uwmetadata of Resource cModel to ''
                 $content = $tx->res->json;
                 
                 
                 my $filename = 'identifiers.json';
                            open(my $fh, '>', $filename) or die "Could not open file '$filename' $!";
                            print $fh encode_json( $content ) ;
                            close $fh;
                 
                 
                 if(defined $content->{metadata}->{uwmetadata}[2]->{children}){
                     my $childrenLenght = (scalar @{$content->{metadata}->{uwmetadata}[2]->{children}}) - 1;
                     for (my $i=0; $i <= $childrenLenght; $i++) {
                         if(defined $content->{metadata}->{uwmetadata}[2]->{children}[$i]->{xmlname}){
                              if($content->{metadata}->{uwmetadata}[2]->{children}[$i]->{xmlname} eq 'format'){
                                  $content->{metadata}->{uwmetadata}[2]->{children}[$i]->{loaded_ui_value} = '';
                                  $content->{metadata}->{uwmetadata}[2]->{children}[$i]->{ui_value} = '';
                                  $content->{metadata}->{uwmetadata}[2]->{children}[$i]->{loaded_value} = '';
                              }
                         }
                     }
                 }
      }else {
                 if(defined($tx->res->json)){
                      if(exists($tx->res->json->{alerts})) {
                           print Dumper("Error deleteFormatNodeFromUWMeta: alerts: ",$tx->res->json->{alerts});        
                      }else{
                           print Dumper("Error deleteFormatNodeFromUWMeta: json: ",$tx->res->json);        
                      }
                 }else{
                           print Dumper("Error deleteFormatNodeFromUWMeta: ",$tx->error);        
                 }
     }
     
     
                            
     
     
     my $json_str = b(encode_json({ metadata => { uwmetadata => $content->{metadata}->{uwmetadata} } }))->decode('UTF-8'); 
     print 'deleteFormatNodeFromUWMeta $url post:',$url,"\n";
     my $tx2 = $ua->post($url => form => { metadata => $json_str } );
     if (my $res2 = $tx2->success) {
                 print Dumper("test_deleteme post success.", $tx2->res->json);
     }else {
                 if(defined($tx2->res->json)){
                      if(exists($tx2->res->json->{alerts})) {
                           print Dumper("Error test_deleteme for uwmetadata: alerts4: ",$tx2->res->json->{alerts});        
                      }else{
                           print Dumper("Error test_deleteme for uwmetadata: json2: ",$tx2->res->json);        
                      }
                 }else{
                           print Dumper("Error test_deleteme for uwmetadata2: ",$tx2->error);        
                 }
     }
}
 
 

 
 
 
###########################################################
###########################################################
#############       Main     ##############################
###########################################################
###########################################################

open(my $data, '<', $file) or die "Could not open '$file' $!\n";
#pid, DOI alt, DOI neu, URL alt, URL neu, RESOURCE-Link
my $i = 0;
while (my $line = <$data>) {
  chomp $line;
  $i++;
  if($i == 1){ next;} #skip first row with headers
  my @fields = split "," , $line;
 
  $fields[0]=~s/^\'+//g;
  $fields[0]=~s/\'+$//g;
  $fields[0]=~s/^ +//g;
  $fields[0]=~s/ +$//g;
  $fields[2]=~s/^\'+//g;
  $fields[2]=~s/\'+$//g;
  $fields[2]=~s/^ +//g;
  $fields[2]=~s/ +$//g;
  $fields[4]=~s/^\'+//g;
  $fields[4]=~s/\'+$//g;
  $fields[4]=~s/^ +//g;
  $fields[4]=~s/ +$//g;
  $fields[5]=~s/^\'+//g;
  $fields[5]=~s/\'+$//g;
  $fields[5]=~s/^ +//g;
  $fields[5]=~s/ +$//g;
    
  removePdfModel($fields[0]);
  addResource($fields[0]);
  addLink($fields[0], $fields[5]);
  updatedDOIAndURL($fields[0], $fields[2], $fields[4]);
  deleteFormatNodeFromUWMeta($fields[0]);
}

1;