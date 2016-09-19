#!/usr/bin/perl -w

use strict;
use warnings;
use YAML::Syck;
use XML::LibXML;
use DBI;
use DBD::mysql;
use Encode;

use Data::Dumper;
use XML::LibXML::PrettyPrint;


sub getPhaidraDB($)
{
    my $config=shift;
    my $connect_info=$config->{'Model::PhaidraDB'}->{'connect_info'};
    my $dbh=DBI->connect($connect_info->[0], $connect_info->[1], $connect_info->[2], $connect_info->[3]);
    return $dbh;
}


sub writeToFile($$) {
    
    my $rdf = shift;
    my $fileName = shift;
    
    my $fileName2 = "testingAllPhadiraVoc/".$fileName;
    open(my $fh, '>:encoding(UTF-8)', $fileName2) or die "Could not open file '$fileName2' $!";
    print $fh $rdf->toString;
    close $fh;

    
=head1
    
    my $document = XML::LibXML->new->parse_file($fileName2);
    my $pp = XML::LibXML::PrettyPrint->new(indent_string => "  ");
    $pp->pretty_print($document); # modified in-place

    
    open( $fh, '>:encoding(UTF-8)', $fileName2) or die "Could not open file '$fileName2' $!";
    print $fh $document->toString;
    close $fh;

=cut
 
}




sub getVocabulary() {

        my $skosConceptScheme = "http://www.w3.org/2004/02/skos/core#ConceptScheme";
        my $defaultScheme = 'http://phaidra.org/2016/04/vocabularies';
        my $phaidra_url = 'http://phaidra.org/2016/04/vocabularies';
        
        #my $config = YAML::Syck::LoadFile('/etc/phaidra.yml');
        my $config = YAML::Syck::LoadFile('/etc/phaidra/phaidra.univie.ac.at/phaidra.yml');
        
        my $dbh = getPhaidraDB($config);
        my $sth;
        my $ss;
        my $type_xml = XML::LibXML::Document->new('1.0', 'utf-8');
        
        
        
        my @vocabulariesToConvert = ();
        
        #  WHERE v.vid = 28   WHERE v.vid > 20 and v.vid < 31              WHERE v.vid > 28 and v.vid < 50  WHERE v.vid < 27
        #  WHERE v.vid = 28 and tv.preferred = 1
        #  WHERE v.vid = 1 and tv.preferred = 1
        
        #  FROM vocabulary_entry ve INNER JOIN vocabulary v ON ve.vid = v.vid INNER JOIN taxon_vocentry tv ON ve.veid = tv.veid
        #  FROM vocabulary_entry ve INNER JOIN vocabulary v ON ve.vid = v.vid
        
        #  WHERE v.vid < 44 AND v.vid > 29
        
          # WHERE v.vid != 0 and v.vid != 9 and v.vid != 18 and v.vid != 19 and v.vid != 20 and v.vid != 26 and v.vid != 30 and 
          # v.vid != 33  and v.vid != 34 and v.vid != 35  and v.vid != 39 and v.vid != 42 and v.vid != 43 
          # and v.vid != 27 and v.vid != 28 and v.vid != 29 and v.vid != 44
        
        #  WHERE v.vid != 27 and v.vid != 28 and v.vid != 29 and v.vid != 44 and tv.preferred = 1
        
        
          #WHERE v.vid = 2  or v.vid = 3  or v.vid = 4  or v.vid = 5  or v.vid = 6  or v.vid = 10 or
          #v.vid = 11 or v.vid = 12 or v.vid = 13 or v.vid = 14 or v.vid = 15 or v.vid = 16 or
          #v.vid = 17 or v.vid = 22 or v.vid = 23 or v.vid = 24 or v.vid = 25 or v.vid = 31 or
          #v.vid = 32 or v.vid = 36 or v.vid = 38 or v.vid = 40
        
        
        $ss = qq/SELECT ve.vid, v.description, ve.veid, ve.isocode, ve.entry 
                FROM vocabulary_entry ve INNER JOIN vocabulary v ON ve.vid = v.vid INNER JOIN taxon_vocentry tv ON ve.veid = tv.veid
                WHERE (v.vid = 30  or v.vid = 33) and tv.preferred = 1
                ORDER BY ve.vid,ve.veid ASC/;
        $sth = $dbh->prepare($ss) or print $dbh->errstr;
        $sth->execute();
        my($vid, $description, $veid, $isocode, $entry);
        $sth->bind_columns(\$vid, \$description, \$veid, \$isocode, \$entry) or print $dbh->errstr;
        my $tmp_vid = -1;
        my $tmp_veid = -1;
        my ($tmpElement, $tmpChild);
        
        my $root = $type_xml->createElement("rdf:RDF");
        $root->setAttribute("xmlns:rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
        $root->setAttribute("xmlns:rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        $root->setAttribute("xmlns:skos", "http://www.w3.org/2004/02/skos/core#");
        
        my $conceptScheme;
        $conceptScheme = $type_xml->createElement("skos:ConceptScheme");
                        
        my $labelEn = $type_xml->createElement("skos:prefLabel");
        $labelEn->setAttribute("xml:lang", 'en');
        $labelEn->appendText("UWClassAndEuropean");
        $conceptScheme->appendChild($labelEn);
                        
        my $labelDe = $type_xml->createElement("skos:prefLabel");
        $labelDe->setAttribute("xml:lang", 'de');
        $labelDe->appendText("UWClassAndEuropean");
        $conceptScheme->appendChild($labelDe);
                        
        my $labelIt = $type_xml->createElement("skos:prefLabel");
        $labelIt->setAttribute("xml:lang", 'it');
        $labelIt->appendText("UWClassAndEuropean");
        $conceptScheme->appendChild($labelIt);
                        
        my $labelSr = $type_xml->createElement("skos:prefLabel");
        $labelSr->setAttribute("xml:lang", 'sr');
        $labelSr->appendText("UWClassAndEuropean");
        $conceptScheme->appendChild($labelSr); 
         
        
        while ($sth->fetch) {
                print "vid:",$vid,"\n";
                print "description:",$description,"\n";
                print "veid:",$veid,"\n";
                print "isocode:",$isocode,"\n";
                print "entry:",$entry,"\n";
                print "=============:","\n";
        
                # my $descriptionUrl = $description;
                # $descriptionUrl =~ tr/ //d;

                if ($veid != $tmp_veid) {
                        $tmpChild = $type_xml->createElement("rdf:Description");
                        my $id = $phaidra_url.'/'.'voc_'.$vid.'/'.$veid;
                        $tmpChild->setAttribute("rdf:about", $id);
                        $root->appendChild($tmpChild);
                        
                        $conceptScheme->setAttribute("rdf:about", $defaultScheme.'/UWClassAndEuropean');
                        
                        my $type = $type_xml->createElement("rdf:type");
                        $type->setAttribute("rdf:resource", 'http://www.w3.org/2004/02/skos/core#Concept');
                        $tmpChild->appendChild($type);
                
                
                        my $notation = $type_xml->createElement("skos:notation");
                        $notation->appendText($description);
                        $tmpChild->appendChild($notation);
                
                        $tmp_veid = $veid;
                        

#=head1                  
                        
                        # get altLabel
                        my $nonPreferedVeid = getNonprefered($veid);
                        foreach my $nonPrefered (@{$nonPreferedVeid}) {
                             my $ss3 = qq/SELECT isocode, entry from vocabulary_entry where veid = $nonPrefered;/;
                             my $sth3 = $dbh->prepare($ss3) or print $dbh->errstr;
                             $sth3->execute();
                             my ($isocode2, $entry2);
                             $sth3->bind_columns(\$isocode2, \$entry2) or print $dbh->errstr;
                             while ($sth3->fetch) {
                                     my $nonPreferedNode = $type_xml->createElement("skos:altLabel");
                                     $nonPreferedNode->setAttribute("xml:lang", $isocode2);
                                     $nonPreferedNode->appendText($entry2);
                                     $tmpChild->appendChild($nonPreferedNode);
                             }
                        }
                      

                      
                        #get narrower and broader concepts
                        my $ss_taxon_vocentry = qq/SELECT tid from taxon_vocentry where veid = $veid;/;
                        my $sth_taxon_vocentry = $dbh->prepare($ss_taxon_vocentry) or print $dbh->errstr;
                        $sth_taxon_vocentry->execute();
                        my $tid;
                        $sth_taxon_vocentry->bind_columns(\$tid) or print $dbh->errstr;
                        my @parent;
                        my @children;
                        while ($sth_taxon_vocentry->fetch) {
                             #broader
                             my $ss_taxon_parent = qq/SELECT TID_parent FROM taxon WHERE TID = $tid;/;
                             my $sth_taxon_parent = $dbh->prepare($ss_taxon_parent) or print $dbh->errstr;
                             $sth_taxon_parent->execute();
                             my $parent;
                             $sth_taxon_parent->bind_columns(\$parent) or print $dbh->errstr;
                             my $id2HashBroader;
                             while ($sth_taxon_parent->fetch) {
                                   # DO STUFS WITH PARENT
                                   if(defined $parent){
                                           my $vidAndVeid = getVidAndVeid($parent);
                                           foreach(@{$vidAndVeid}){
                                                   # add broader if not already exists
                                                   my $id2 = $phaidra_url.'/'.'voc_'.$_->{vid}.'/'.$_->{veid};
                                                   if(not defined $id2HashBroader->{$id2}){
                                                        my $broader = $type_xml->createElement("skos:broader");
                                                        $broader->setAttribute("rdf:resource", $id2);
                                                        $tmpChild->appendChild($broader);
                                                   }
                                                   $id2HashBroader->{$id2} = 1;
                                           }
                                   }else{
                                           my $inScheme = $type_xml->createElement("skos:inScheme");
                                           $inScheme->setAttribute("rdf:resource", $defaultScheme.'/UWClassAndEuropean');
                                           $tmpChild->appendChild($inScheme); 
                                   
                                           my $topConceptOf = $type_xml->createElement("skos:topConceptOf");
                                           $topConceptOf->setAttribute("rdf:resource", $defaultScheme.'/UWClassAndEuropean');
                                           $tmpChild->appendChild($topConceptOf);
                                   }
                             }
                             
                             #narrower
                             my $ss_taxon_children = qq/SELECT TID FROM taxon WHERE TID_parent = $tid;/;
                             my $sth_taxon_children = $dbh->prepare($ss_taxon_children) or print $dbh->errstr;
                             $sth_taxon_children->execute();
                             my @children;
                             my $child;
                             $sth_taxon_children->bind_columns(\$child) or print $dbh->errstr;
                             my $id2HashNarrower;
                             while ($sth_taxon_children->fetch) {
                                   # DO STUFS WITH CHILDREN
                                   if(defined $child){
                                           my $vidAndVeid = getVidAndVeid($child);
                                           foreach(@{$vidAndVeid}){
                                                   # add narrower if not already exists
                                                   my $id2 = $phaidra_url.'/'.'voc_'.$_->{vid}.'/'.$_->{veid};
                                                   if(not defined $id2HashNarrower->{$id2}){
                                                        my $narrower = $type_xml->createElement("skos:narrower");
                                                        $narrower->setAttribute("rdf:resource", $id2);
                                                        $tmpChild->appendChild($narrower);
                                                   }
                                                   $id2HashNarrower->{$id2} = 1;
                                           }
                                   }
                             }
                        }
                        
#=cut
                        
                 }
                 
                 my $prefLabel = $type_xml->createElement("skos:prefLabel");
                 $prefLabel->setAttribute("xml:lang", $isocode);
                 $prefLabel->appendText($entry);
                 $tmpChild->appendChild($prefLabel);

        }
            
        $root->appendChild($tmpChild); #add last vocabulary(concept) , 461433 example veid
        $root->appendChild($conceptScheme);
        writeToFile($root, 'phaidra_univien_and_euprojects.rdf');
            
            
        $type_xml->setDocumentElement($root);
        return $type_xml;
}


sub getVidAndVeid($){
    
    my $tid = shift;
    
    my $config = YAML::Syck::LoadFile('/etc/phaidra/phaidra.univie.ac.at/phaidra.yml');
    my $dbh = getPhaidraDB($config);
    my $ss = qq/SELECT tv.veid, ve.vid
                FROM taxon_vocentry tv INNER JOIN vocabulary_entry ve ON tv.veid = ve.veid
                WHERE tv.tid = $tid
                ORDER BY ve.vid,ve.veid ASC/;
    
    my $sth = $dbh->prepare($ss) or print $dbh->errstr;
    $sth->execute();
    my ($veid, $vid);
    $sth->bind_columns(\$veid, \$vid) or print $dbh->errstr;
    my @result;
    while ($sth->fetch) {
        my $recordHash;
        $recordHash->{veid} = $veid;
        $recordHash->{vid} = $vid;
        push @result, $recordHash;
    }
    
    return \@result;
    
}

sub getNonprefered($){
    
    my $veid = shift;
    
    print "veid: ",$veid,"\n";
    
    my $config = YAML::Syck::LoadFile('/etc/phaidra/phaidra.univie.ac.at/phaidra.yml');
    my $dbh = getPhaidraDB($config);
    my $ss = qq/SELECT tid from taxon_vocentry where veid = $veid;/;
    my $sth = $dbh->prepare($ss) or print $dbh->errstr;
    $sth->execute();
    my $tid;
    $sth->bind_columns(\$tid) or print $dbh->errstr;
    
    my @nonprefered;
    while ($sth->fetch) {
         print "tid: ",$tid,"\n";
         my $ss2 = qq/SELECT veid, preferred from taxon_vocentry where tid = $tid and preferred = 0;/;
         my $sth2 = $dbh->prepare($ss2) or print $dbh->errstr;
         $sth2->execute();
         my ($veid, $preferred);
         $sth2->bind_columns(\$veid, \$preferred) or print $dbh->errstr;

         while ($sth2->fetch) {
                push @nonprefered, $veid;
         }
    }
    return \@nonprefered;
 
}

sub  trim($) { 
    my $s = shift; 
    $s =~ s/^\s+|\s+$//g; 
    return $s 
};


my $tmp = getVocabulary();

#print $tmp->toString;

1;
