#!/usr/bin/perl -w

use strict;
use warnings;

use Data::Dumper;
use lib;
use JSON;
use utf8;
use MongoDB;
use Encode;
use File::Find;
use File::Find::Rule;



# find /media/phaidra-entw_root/var/www/fuseki/650JahreUniversitatWien/ -type f -iname *.jpg | awk '{print "\"" $0 "\""}' | xargs exiftool -j -Title -Description -Creator -XPComment | tee /home/michal/Documents/code/area42/user/mf/650Jahren/650Jahren2.json

# find /media/phaidra-entw_root/home/folcanm4/650/ -type f -iname *.jpg | awk '{print "\"" $0 "\""}' | xargs exiftool -j -Title -Description -Creator | tee /home/michal/Documents/code/area42/user/mf/650Jahren/650Jahren.json

# find /media/phaidra-entw_root/home/folcanm4/650/ -type f -iname *.pdf | awk '{print "{\"institution\":\"" $0 "\"}"}' | tee /home/michal/Documents/code/area42/user/mf/650Jahren/650JahrenPdf.json


sub getUwmetadata($$$$);

my $mongoDbConnection = MongoDB::MongoClient->new(
        #host => "mongodb://mongo.example.com/",
        host => 'localhost',
        username => '',
        password => '',
    );

my $mongoDb = $mongoDbConnection->get_database('bagger');

my $collectionBags = $mongoDb->get_collection('bags');
my $collectionFolders = $mongoDb->get_collection('folders');

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


my $json = JSON->new;
my $data_jpg = $json->decode($json_text_jpg);
my $data_pdf = $json->decode($json_text_pdf);


my $defaultDescription = 'Die Universität Wien, eine der ältesten und größten Hochschulen Europas, feierte im Jahr 2015 ihr 650. Gründungsjubiläum. Aus diesem Anlass öffnete sie ihre Tore einer breiten Öffentlichkeit und bot ein attraktives Jubiläumsprogramm. Institute, Fachbereiche, Fakultäten und Zentren, wie auch die Dienstleistungseinheiten Bibliothek, Veranstaltungsmanagement und Öffentlichkeitsarbeit, der Alumniverband und die Kinderuni Wien bereicherten das Jubiläumsjahr mit zahlreichen eigenen Aktivitäten. Das Jahresprogramm umfasste unterschiedlichste Formate wie Vorträge, Kongresse und Symposien, Vortragsreihen, Ausstellungen, Konzerte, Sportevents und Filmabende. Das gemeinsame Ziel aller Veranstaltung war es, der Öffentlichkeit einen Einblick in die Arbeit der Universität zu vermitteln und auf die vielfältigen positiven Effekte und den Mehrwert der universitären Lehre und Forschung für die Gesellschaft hinzuweisen.';

sub addFolders($);

#!!!!!!!!!!!!!!!!!!!!!!!
# update 'folders' mongoDb collection
#addFolders('/home/michal/Documents/code/area42/user/mf/angularjs/bagger/data/650jahren/folders/in/650');
addFolders('/home/michal/Documents/code/area42/user/mf/angularjs/bagger/data/650jahren/folders/in/650 Jahre Universität Wien');

#exit;



my $i = 1;
foreach my $picture (@{$data_jpg}) {
    $picture->{assignee} = "jubilaeumb46";
    my @filePath = split /\//, $picture->{SourceFile};

    my $fileName = pop @filePath;
    my $folderid = pop @filePath;
    my $fileNameId = $fileName;
    if ($fileNameId =~ m/[^a-zA-Z0-9]/){
           #print "The string contains non-alphanumeric characters";
           $fileNameId =~ s/[^a-zA-Z\d\s:]//g;
           $fileNameId =~ s/ //g;
    }
    if ($folderid =~ m/[^a-zA-Z0-9]/){
           $folderid =~ s/[^a-zA-Z\d\s:]//g;
           $folderid =~ s/ //g;
    }
    my $bagid = "650jahren".$folderid.$fileNameId;
    $picture->{bagid} = $bagid;
    $picture->{file} = $fileName;
    $picture->{created} = time;
    $picture->{updated} = time;
    $picture->{project} = "650jahren";
    $picture->{status} = "new";
    $picture->{tags} = ();
   
    $picture->{folderid} = $folderid;
    $picture->{label} = $picture->{Description};
    $picture->{metadata}->{uwmetadata} = getUwmetadata($fileName, $picture->{Title}, $picture->{Description}, $picture->{Creator});
    
    $collectionBags->insert($picture);
    
    $i++;    
}



my $j = 0;
foreach my $pdf (@{$data_pdf}) {
    $pdf->{assignee} = "jubilaeumb46";
    my @filePath = split /\//, $pdf->{path};
    my $fileName = pop @filePath;
    my $folderid = pop @filePath;
    my $fileNameId = $fileName;
    if ($fileNameId =~ m/[^a-zA-Z0-9]/){
           #print "The string contains non-alphanumeric characters";
           $fileNameId =~ s/[^a-zA-Z\d\s:]//g;
           $fileNameId =~ s/ //g;
    }
    if ($folderid =~ m/[^a-zA-Z0-9]/){
           $folderid =~ s/[^a-zA-Z\d\s:]//g;
           $folderid =~ s/ //g;
    }
    my $badid = "650jahren".$folderid.$fileNameId;
    print '$badid pdf:',$badid,"\n";
    $pdf->{bagid} = $badid;
    $pdf->{file} = $fileName;
    $pdf->{created} = time;
    $pdf->{updated} = time;
    $pdf->{project} = "650jahren";
    $pdf->{status} = "new";
    $pdf->{tags} = ();
    $pdf->{folderid} = $folderid;
    #$pdf->{folderid} = '/home/michal/Documents/code/area42/user/mf/angularjs/bagger/data/650jahren/folders/in';
    $pdf->{label} = $defaultDescription;
    $pdf->{Creator} = 'Universität Wien';
    $pdf->{metadata}->{uwmetadata} = getUwmetadata($fileName, $pdf->{Title}, $pdf->{Description}, $pdf->{Creator});
    $collectionBags->insert($pdf);
    $j++;
}


# update 'folders' mongoDb collection
sub addFolders($){

     my $path = shift;

     
     my @array = File::Find::Rule->directory->in($path);
     foreach my $folderPath (@array){
         my @folderIdArray = split /\//, $folderPath;
         my $folderId = pop @folderIdArray;
         if ($folderId =~ m/[^a-zA-Z0-9]/){
              $folderId =~ s/[^a-zA-Z\d\s:]//g;
              $folderId =~ s/ //g;
         }
         print 'folderid1:', Dumper($folderId);
         my $folderHash;
         $folderHash->{status} = 'active';
         $folderHash->{project} = '650jahren';
         $folderHash->{name} = $folderId;
         $folderHash->{path} = $folderPath;
         $folderHash->{created} = time;
         $folderHash->{updated} = time;
         $folderHash->{folderid} = $folderId;
         
         $folderHash->{folderid} = decode('UTF-8', $folderHash->{folderid});
         
         $folderHash->{name} = decode('UTF-8', $folderHash->{name});
         $folderHash->{path} = decode('UTF-8', $folderHash->{path});

         $collectionFolders->update({folderid => $folderHash->{folderid}}, {'$set' => {
                                                                                     status => $folderHash->{status},
                                                                                     project => $folderHash->{project},
                                                                                     path => $folderHash->{path},
                                                                                     name => $folderHash->{name},
                                                                                     created => $folderHash->{created},
                                                                                     updated => $folderHash->{updated}
                                                                                 }}, 
                                                                          {'upsert' => 1});

         
     }
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
           $uwmetaTitle = join(" ", @fileNameArray);
      }
      $uwmetaTitle =~ s/_/ /g;
      
      my $uwmetaDescription;
      if(defined $description){
           $uwmetaDescription = $description;
      }else{
           $uwmetaDescription = $defaultDescription;
      }
      
      my $uwmetaFirstName = '';
      my $uwmetaLastName = '';
      if(defined $creator){
           my @creatorArray = split / /, $creator;
           $uwmetaFirstName = $creatorArray[0];
           $uwmetaLastName = $creatorArray[1];
      }
            
     my $institutionNode = '{
                    "xmlns": "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/entity",
                    "xmlname": "institution",
                    "input_type": "input_text",
                    "ui_value": "Universität Wien",
                    "datatype": "CharacterString"
                  },
                  {
                    "xmlns": "http://phaidra.univie.ac.at/XML/metadata/lom/V1.0/entity",
                    "xmlname": "type",
                    "input_type": "input_text",
                    "ui_value": "institution",
                    "datatype": "CharacterString"
                  }
                  ';

   my $nameNode = '{
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
                  }';
      
  my $entityChildren;
  if( ($uwmetaFirstName eq 'Universität' || $uwmetaFirstName eq 'universität' || $uwmetaFirstName eq 'Universitat' || $uwmetaFirstName eq 'universitat') && ( $uwmetaLastName eq 'Wien' || $uwmetaLastName eq 'wien')){
        $entityChildren = $institutionNode;
  }elsif( ($uwmetaFirstName eq 'Universitätsbibliothek' || $uwmetaFirstName eq 'universitätsbibliothek' || $uwmetaFirstName eq 'Universitatsbibliothek' || $uwmetaFirstName eq 'universitatsbibliothek') && ( $uwmetaLastName eq 'Wien' || $uwmetaLastName eq 'wien') ){
        $entityChildren = $institutionNode;
  }else{
        $entityChildren = $nameNode;
  }
      
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
                "children": ['.$entityChildren.']
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