#!/usr/bin/perl -w

use strict;
use warnings;

use Data::Dumper;
use lib;
use JSON;
use utf8;
use MongoDB;
#use Encode qw(is_utf8 decode encode);
use Encode;


# find /media/phaidra-entw_root/home/folcanm4/650/ -type f -iname *.jpg | awk '{print "\"" $0 "\""}' | xargs exiftool -j -Title -Description -Creator | tee /home/michal/Documents/code/area42/user/mf/650Jahren/650Jahren.json

# find /media/phaidra-entw_root/home/folcanm4/650/ -type f -iname *.pdf | awk '{print "{\"institution\":\"" $0 "\"}"}' | tee /home/michal/Documents/code/area42/user/mf/650Jahren/650JahrenPdf.json


sub getUwmetadata($$$$);

my $mongoDbConnection = MongoDB::MongoClient->new(
        #host => "mongodb://mongo.example.com/",
        host => 'localhost',
        username => '',
        password => '',
        
    );
my $mongoDb = $mongoDbConnection->get_database('650Jahren');
my $collectionBags = $mongoDb->get_collection('bags');

#find /media/phaidra-entw_root/home/folcanm4/650/ -type f -iname *.jpg | awk '{print "\"" $0 "\""}' | xargs exiftool -j -Title -Description -Creator | tee /home/michal/Documents/code/area42/user/mf/650Jahren/650Jahren.json

my $filename = '650Jahren.json';

my $json_text_jpg = do {
   open(my $json_fh, "<:encoding(UTF-8)", $filename)
      or die("Can't open \$filename\": $!\n");
   local $/;
   <$json_fh>
};

$filename = '650JahrenPdf.json';
my $json_text_pdf = do {
   open(my $json_fh2, "<:encoding(UTF-8)", $filename)
      or die("Can't open \$filename\": $!\n");
   local $/;
   <$json_fh2>
};


#utf8::encode($json_text_jpg);
my $json = JSON->new;
my $data_jpg = $json->decode($json_text_jpg);
my $data_pdf = $json->decode($json_text_pdf);


#print Dumper($data_pdf);exit;

#print Dumper($data_jpg);
#exit;

my $defaultDescription = 'Die Universität Wien feierte im Jahr 2015 ihr 650. Gründungsjubiläum. Aus diesem Anlass öffnete eine der ältesten und größten Hochschulen Europas ihre Tore einer breiten Öffentlichkeit. Die vielfältigen Fachbereiche, Fakultäten und Zentren der Universität begleiteten das Jubiläumsjahr mit zahlreichen Aktivitäten. Das Angebot beinhaltete Vorträge, Kongresse und Symposien, Spezialvorlesungen und Seminare aber auch Ausstellungen, Konzerte, Sportevents und Performances. Die Vermittlung der Relevanz von Forschung und Lehre stand dabei im Mittelpunkt.';

my $i = 1;
foreach my $picture (@{$data_jpg}) {
    next if $i > 3;
    #print $picture->{Creator}, "\n";
    #
    #exit;
    $picture->{assignee} = "hudakr4";
    my @filePath = split /\//, $picture->{SourceFile};
    my $fileName = pop @filePath;
    $fileName =~ s/ //g;
    $fileName =~ s/-//g;
    $fileName =~ s/_//g;
    my $badid = "650jahre".$fileName;
    $picture->{bagid} = $badid;
    $picture->{file} = $fileName;
    $picture->{created} = time;
    $picture->{updated} = time;
    $picture->{project} = "650jahre";
    $picture->{status} = "new";
    $picture->{tags} = ();
    my $folderid = join("/", @filePath);
    $picture->{folderid} = $folderid;
    $picture->{label} = $picture->{Description};
    $picture->{metadata}->{uwmetadata} = getUwmetadata($fileName, $picture->{Title}, $picture->{Description}, $picture->{Creator});

    #print Dumper($picture);
    
    $collectionBags->insert($picture);
    
    
    $i++;
    #print Dumper($picture);
    
}

my $j = 0;
foreach my $pdf (@{$data_pdf}) {
    next if $j > 3;
    print "ssss\n";
    $pdf->{assignee} = "hudakr4";
    my @filePath = split /\//, $pdf->{path};
    my $fileName = pop @filePath;
    $fileName =~ s/ //g;
    $fileName =~ s/-//g;
    $fileName =~ s/_//g;
    my $badid = "650jahre".$fileName;
    $pdf->{bagid} = $badid;
    $pdf->{file} = $fileName;
    $pdf->{created} = time;
    $pdf->{updated} = time;
    $pdf->{project} = "650jahre";
    $pdf->{status} = "new";
    $pdf->{tags} = ();
    my $folderid = join("/", @filePath);
    $pdf->{folderid} = $folderid;
    $pdf->{label} = $defaultDescription;
    $pdf->{Creator} = 'Universität Wien';
    $pdf->{metadata}->{uwmetadata} = getUwmetadata($fileName, $pdf->{Title}, $pdf->{Description}, $pdf->{Creator});
    $collectionBags->insert($pdf);
    $j++;
}
 


sub getUwmetadata($$$$){

      my $myfileName = shift;
      my $title = shift;
      my $description = shift;
      my $creator = shift;
      my @fileNameArray;
      my $fileExtension;
      my $uwmetaTitle;
      if(defined $title){
           $uwmetaTitle = $title;
      }else{
           @fileNameArray = split /\./, $myfileName;
           $fileExtension = pop @fileNameArray;
           $uwmetaTitle = join("/", @fileNameArray);
      }
      
      my $uwmetaDescription;
      if(defined $description){
           $uwmetaDescription = $description;
      }else{
           $uwmetaDescription = 'Die Universität Wien feierte im Jahr 2015 ihr 650. Gründungsjubiläum. Aus diesem Anlass öffnete eine der ältesten und größten Hochschulen Europas ihre Tore einer breiten Öffentlichkeit. Die vielfältigen Fachbereiche, Fakultäten und Zentren der Universität begleiteten das Jubiläumsjahr mit zahlreichen Aktivitäten. Das Angebot beinhaltete Vorträge, Kongresse und Symposien, Spezialvorlesungen und Seminare aber auch Ausstellungen, Konzerte, Sportevents und Performances. Die Vermittlung der Relevanz von Forschung und Lehre stand dabei im Mittelpunkt.';
      }
      
      my $uwmetaFirstName = '';
      my $uwmetaLastName = '';
      if(defined $creator){
           my @creatorArray = split / /, $creator;
           $uwmetaFirstName = $creatorArray[0];
           $uwmetaLastName = $creatorArray[1];
           #$uwmetaFirstName = encode('UTF-8', $uwmetaFirstName);
           #$uwmetaFirstName = utf8::downgrade($uwmetaFirstName);
           #$uwmetaFirstName = utf8::upgrade($uwmetaFirstName)
           #$uwmetaLastName = encode('UTF-8', $uwmetaLastName);
      }
      #my $uwmetaFirstName = decode('UTF-8', $uwmetaFirstName);
      #print "Is this utf8: ",is_utf8($uwmetaFirstName) ? "Yes" : "No", "\n";
      #print "Is this valid: ",utf8::valid($uwmetaFirstName) ? "Yes" : "No", "\n";
      #$uwmetaFirstName = decode_utf8( $uwmetaFirstName );
      #$uwmetaFirstName = encode('UTF-8', $uwmetaFirstName);
      print $uwmetaFirstName, "\n";
      print Dumper($uwmetaFirstName);
      #exit;
      
      my $uwmeta_string = '
      

      [
      {
        "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0",
        "xmlname": "general",
        "children": [
          {
            "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0",
            "xmlname": "title",
            "ui_value": "'.$uwmetaTitle.'",
            "value_lang": "de",
            "datatype": "LangString"
          },
          {
            "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0",
            "xmlname": "language",
            "ui_value": "de",
            "datatype": "Language"
          },
          {
            "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0",
            "xmlname": "description",
            "ui_value": "'.$uwmetaDescription.'",
            "value_lang": "de",
            "datatype": "LangString"
          }
        ]
      },
      {
        "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0",
        "xmlname": "lifecycle",
        "children": [
          {
            "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0",
            "xmlname": "contribute",
            "data_order": "0",
            "ordered": 1,
            "children": [
              {
                "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0",
                "xmlname": "role",
                "ui_value": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0\/voc_3\/1552095",
                "datatype": "Vocabulary"
              },
              {
                "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0",
                "xmlname": "entity",
                "data_order": "0",
                "ordered": 1,
                "children": [
                  {
                    "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0\/entity",
                    "xmlname": "firstname",
                    "ui_value": "'.$uwmetaFirstName.'",
                    "datatype": "CharacterString"
                  },
                  {
                    "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0\/entity",
                    "xmlname": "lastname",
                    "ui_value": "'.$uwmetaLastName.'",
                    "datatype": "CharacterString"
                  }
                ]
              }
            ]
          }
        ]
      },
      {
        "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0",
        "xmlname": "rights",
        "children": [
          {
            "xmlns": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0",
            "xmlname": "license",
            "ui_value": "http:\/\/phaidra.univie.ac.at\/XML\/metadata\/lom\/V1.0\/voc_21\/1",
            "datatype": "License"
          }
        ]
      }
    ]


      ';
        
      
        my $json_bytes = encode('UTF-8', $uwmeta_string);
        my $uwmeta_hash = JSON->new->utf8->decode($json_bytes);
        

        return $uwmeta_hash;
        


}












1;