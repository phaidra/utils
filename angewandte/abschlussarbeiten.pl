#!/usr/bin/perl -w

use strict;
use warnings;

use Data::Dumper;

use LWP::UserAgent;
use HTTP::Request::Common qw{ POST };
use JSON;

=head1

 Create csv file with Created date, PID, Adviser, Title DE, Title EN , Abstract DE and Abstract EN
 Phaidra Angewandte

=cut



my $url_lucene = 'https://services.phaidra.bibliothek.uni-ak.ac.at/api/search/lucene';
my @fields = [];
push @fields, 'PID';
push @fields, 'fgs.createdDate';
push @fields, 'uw.general.title.de';
push @fields, 'uw.general.title.en';


#get hits
my $ua      = LWP::UserAgent->new();
my $request = POST( $url_lucene, [ 
                             'q' => 'uw.lifecycle.contribute.role:1552167',
                             'fields' => \@fields,
                             'from'   => 0,
                             'limit'   => 10,
                             'reverse'=> 1,
                             'sort'=> 'fgs.createdDate,STRING',
                          ] );
my $content = $ua->request($request);

my $content_hash;
if ($content->is_success) {
    $content_hash = decode_json $content->decoded_content;
}
else {
    die $content->status_line;
}

my $request2 = POST( $url_lucene, [ 
                             'q' => 'uw.lifecycle.contribute.role:1552167',
                             'fields' => \@fields,
                             'from'   => 0,
                             'limit'   => $content_hash->{hits},
                             #'limit'   => 10,
                             'reverse'=> 1,
                             'sort'=> 'fgs.createdDate,STRING',
                          ] );
my $content2 = $ua->request($request2);
my $content2_array;                         
if ($content2->is_success) {
    $content2_array = decode_json $content2->decoded_content;
}
else {
    die $content2->status_line;
}


sub getAbstract($$){

     my $content = shift;
     my $language = shift;
     
     my $abstract;
     if(defined $content->{metadata}->{uwmetadata}[0]->{children}){
            my $childrenLenght = (scalar @{$content->{metadata}->{uwmetadata}[0]->{children}}) - 1;
            for (my $i=0; $i <= $childrenLenght; $i++) {
                   if(defined $content->{metadata}->{uwmetadata}[0]->{children}[$i]->{xmlname} && defined $content->{metadata}->{uwmetadata}[0]->{children}[$i]->{value_lang}){
                         if(
                            $content->{metadata}->{uwmetadata}[0]->{children}[$i]->{xmlname} eq 'description' && 
                            $content->{metadata}->{uwmetadata}[0]->{children}[$i]->{value_lang} eq $language
                           ){
                                    $abstract = $content->{metadata}->{uwmetadata}[0]->{children}[$i]->{loaded_ui_value};
                         }
                   }
            }
     }
     
     return $abstract;

}

sub getadviser($){

     my $content = shift;
     
     my $adviserString = '';
     my @adviserArray;
     if(defined $content->{metadata}->{uwmetadata}[1]->{children}){
            for (my $j=0; $j <= (scalar @{$content->{metadata}->{uwmetadata}[0]->{children}}) - 1; $j++) {
               if(defined $content->{metadata}->{uwmetadata}[1]->{children}[$j]->{xmlname}){
                  if( $content->{metadata}->{uwmetadata}[1]->{children}[$j]->{xmlname} eq 'contribute' ){
                            my $isAdviser = 0;
                            my $arrayLenght = (scalar @{$content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}}) - 1;
                            for(my $k = 0; $k <=  $arrayLenght; $k++){
                                if(defined $content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{xmlname}){
                                        if($content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{xmlname} eq 'role'){
                                                my $ui_value = $content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{ui_value};
                                                my @array = split /\//, $ui_value;
                                                my $roleCode = $array[-1];
                                                if($roleCode == 1552167){
                                                       $isAdviser = 1;
                                                }
                                        }
                                        if($content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{xmlname} eq 'entity'){
                                           if($isAdviser == 1){
                                                  my $adviser;
                                                  for(my $m = 0; $m <=  (scalar @{$content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{children}}) - 1; $m++){
                                                          # print '$m:',$m,"\n";
                                                          if(defined $content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{children}[$m]){
                                                                  if($content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{children}[$m]->{xmlname} eq 'firstname'){
                                                                           $adviser->{firstname} = $content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{children}[$m]->{ui_value}
                                                                  }
                                                                  if($content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{children}[$m]->{xmlname} eq 'lastname'){
                                                                           $adviser->{lastname} = $content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{children}[$m]->{ui_value}
                                                                  }
                                                                  if($content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{children}[$m]->{xmlname} eq 'title1'){
                                                                           $adviser->{title1} = $content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{children}[$m]->{ui_value}
                                                                  }
                                                                  if($content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{children}[$m]->{xmlname} eq 'title2'){
                                                                           $adviser->{title2} = $content->{metadata}->{uwmetadata}[1]->{children}[$j]->{children}[$k]->{children}[$m]->{ui_value}
                                                                  }
                                                          }
                                                  }
                                                  if(defined $adviser){
                                                          push @adviserArray, $adviser;
                                                  }
                                           }
                                       }
                                }
                          }
                  }
               }
            }
            my $adviserArrayLength = (scalar @adviserArray) - 1;
            for(my $v = 0; $v <= $adviserArrayLength ; $v++){
                    if(
                        (defined $adviserArray[$v]->{firstname} || defined $adviserArray[$v]->{lastname}) &&
                        ( $adviserArray[$v]->{firstname} ne '' || $adviserArray[$v]->{lastname} ne '')
                      ){
                            $adviserString = $adviserString.$adviserArray[$v]->{firstname}.' '.$adviserArray[$v]->{lastname}.', '.$adviserArray[$v]->{title1}.'  ';
                    }
            }
           
     }
     return $adviserString;

}



my $w = 1;
my $filename = 'abschlussarbeiten.csv';
open(my $fh, '>:encoding(UTF-8)', $filename) or die "Could not open file '$filename' $!";
print $fh "Created date|PID|Adviser|Title DE|Title EN|Abstract DE|Abstract EN\n";
foreach (@{$content2_array->{objects}})
{
      my $url = "https://services.phaidra.bibliothek.uni-ak.ac.at/api/object/".$_->{PID}."/uwmetadata";
      my $content3 = $ua->get($url);
      my $content_hash3;
      if ($content3->is_success) {
            $content_hash3 = decode_json $content3->decoded_content;
      }
      else {
            die $content3->status_line;
      }
      
      my $abstractDe = getAbstract($content_hash3, 'de');
      my $abstractEn = getAbstract($content_hash3, 'en');
      my $adviser = getadviser($content_hash3);
     
      $_->{abstractDe} = $abstractDe;
      $_->{abstractEn} = $abstractEn;
      $_->{adviser} = $adviser;
      
    
     
      $_->{'fgs.createdDate'}     = '' if not defined  $_->{'fgs.createdDate'};
      $_->{PID}                   = '' if not defined  $_->{PID};
      $_->{adviser}               = '' if not defined  $_->{adviser};
      $_->{'uw.general.title.de'} = '' if not defined  $_->{'uw.general.title.de'};
      $_->{'uw.general.title.en'} = '' if not defined  $_->{'uw.general.title.en'};
      $_->{abstractDe}            = '' if not defined  $_->{abstractDe};
      $_->{abstractEn}            = '' if not defined  $_->{abstractEn};
      
      # remove quotes at the beginning and end of entry
      $_->{'fgs.createdDate'}=~s/^\"+//g;
      $_->{'fgs.createdDate'}=~s/\"+$//g;
      $_->{PID}=~s/^\"+//g;
      $_->{PID}=~s/\"+$//g;
      $_->{adviser}=~s/^\"+//g;
      $_->{adviser}=~s/\"+$//g;
      $_->{'uw.general.title.de'}=~s/^\"+//g;
      $_->{'uw.general.title.de'}=~s/\"+$//g;
      $_->{'uw.general.title.en'}=~s/^\"+//g;
      $_->{'uw.general.title.en'}=~s/\"+$//g;
      $_->{abstractDe}=~s/^\"+//g;
      $_->{abstractDe}=~s/\"+$//g;
      $_->{abstractEn}=~s/^\"+//g;
      $_->{abstractEn}=~s/\"+$//g;
      
      # write results int csv file
      print $fh $_->{'fgs.createdDate'}.'|'.$_->{PID}.'|'.$_->{adviser}.'|'.$_->{'uw.general.title.de'}.'|'.$_->{'uw.general.title.en'}.'|'.$_->{abstractDe}.'|'.$_->{abstractEn}."\n";
 
      print $w." of $content_hash->{hits} \n";
      $w++;
}
close $fh;

1;