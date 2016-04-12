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
    
    my $fileName2 = "testingAll6Vocabularies/".$fileName;
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
        #my $phaidra_url = 'http://phaidra.univien.ac.at';
        my $phaidra_url = 'http://phaidra.org';
        
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
        
        $ss = qq/SELECT ve.vid, v.description, ve.veid, ve.isocode, ve.entry 
                FROM vocabulary_entry ve INNER JOIN vocabulary v ON ve.vid = v.vid
                WHERE v.vid != 0 and v.vid != 9 and v.vid != 18 and v.vid != 19 and v.vid != 20 and v.vid != 26 and v.vid != 30 and 
                v.vid != 33  and v.vid != 34 and v.vid != 35  and v.vid != 39 and v.vid != 42 and v.vid != 43 
                and v.vid != 27 and v.vid != 28 and v.vid != 29 and v.vid != 44
                ORDER BY ve.vid,ve.veid ASC/;
        $sth = $dbh->prepare($ss) or print $dbh->errstr;
        $sth->execute();
        my($vid, $description, $veid, $isocode, $entry);
        $sth->bind_columns(\$vid, \$description, \$veid, \$isocode, \$entry) or print $dbh->errstr;
        my $tmp_vid = -1;
        my $tmp_veid = -1;
        my ($tmpElement, $tmpChild);
        my $root = $type_xml->createElement("vocabularies");
        my $vocabularyFileName;
        my $descriptionHasTopConcept;
        
        
        
        while ($sth->fetch) {
                print "vid:",$vid,"\n";
                print "description:",$description,"\n";
                print "veid:",$veid,"\n";
                print "isocode:",$isocode,"\n";
                print "entry:",$entry,"\n";
                print "=============:","\n";
                 
                my $descriptionUrl = $description;
                $descriptionUrl =~ tr/ //d;
                 
                if ($vid != $tmp_vid) {
                        if(defined($tmpElement)) {
                                $root->appendChild($tmpElement);
                                $tmpElement->appendChild($descriptionHasTopConcept);
                                writeToFile($tmpElement, $vocabularyFileName.'.rdf');
                        }
                        $tmpElement = $type_xml->createElement("rdf:RDF");
                        $tmpElement->setAttribute("xmlns:rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
                        $tmpElement->setAttribute("xmlns:rdfs", "http://www.w3.org/2000/01/rdf-schema#");
                        $tmpElement->setAttribute("xmlns:skos", "http://www.w3.org/2004/02/skos/core#");
                        $tmp_vid = $vid;
                
                
                
                        $descriptionHasTopConcept = $type_xml->createElement("skos:ConceptScheme");
                        $descriptionHasTopConcept->setAttribute("rdf:about", $defaultScheme.'/'.$descriptionUrl);
                        
                        my $labelEn = $type_xml->createElement("skos:prefLabel");
                        $labelEn->setAttribute("xml:lang", 'en');
                        $labelEn->appendText("$description");
                        $descriptionHasTopConcept->appendChild($labelEn);
                        
                        my $labelDe = $type_xml->createElement("skos:prefLabel");
                        $labelDe->setAttribute("xml:lang", 'de');
                        $labelDe->appendText("$description");
                        $descriptionHasTopConcept->appendChild($labelDe);
                        
                        my $labelIt = $type_xml->createElement("skos:prefLabel");
                        $labelIt->setAttribute("xml:lang", 'it');
                        $labelIt->appendText("$description");
                        $descriptionHasTopConcept->appendChild($labelIt);
                        
                        my $labelSr = $type_xml->createElement("skos:prefLabel");
                        $labelSr->setAttribute("xml:lang", 'sr');
                        $labelSr->appendText("$description");
                        $descriptionHasTopConcept->appendChild($labelSr);

                
                
                }
                if ($veid != $tmp_veid) {
                        $tmpChild = $type_xml->createElement("rdf:Description");
                        my $id = $phaidra_url.'/'.'voc_'.$vid.'/'.$veid;
                        $tmpChild->setAttribute("rdf:about", $id);
                        $tmpElement->appendChild($tmpChild);
                        
                        
                        $vocabularyFileName = "vocabulary".$vid."_".$description;
                        $vocabularyFileName =~ tr/://d;
                
                        my $type = $type_xml->createElement("rdf:type");
                        $type->setAttribute("rdf:resource", 'http://www.w3.org/2004/02/skos/core#Concept');
                        $tmpChild->appendChild($type);
                
                
                        $tmp_veid = $veid;
                        
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
                             my $ss_taxon_parent = qq/SELECT TID_parent FROM taxon WHERE TID = $tid;/;
                             my $sth_taxon_parent = $dbh->prepare($ss_taxon_parent) or print $dbh->errstr;
                             $sth_taxon_parent->execute();
                             my $parent;
                             $sth_taxon_parent->bind_columns(\$parent) or print $dbh->errstr;
                             my $id2HashBroader;
                             while ($sth_taxon_parent->fetch) {
                                   # DO STUFS WITH PARENT $parent
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
                                           $inScheme->setAttribute("rdf:resource", $defaultScheme.'/'.$descriptionUrl);
                                           $tmpChild->appendChild($inScheme); 
                                   
                                           my $topConceptOf = $type_xml->createElement("skos:topConceptOf");
                                           $topConceptOf->setAttribute("rdf:resource", $defaultScheme.'/'.$descriptionUrl);
                                           $tmpChild->appendChild($topConceptOf);
                                   }
                             }
                             
                             my $ss_taxon_children = qq/SELECT TID FROM taxon WHERE TID_parent = $tid;/;
                             my $sth_taxon_children = $dbh->prepare($ss_taxon_children) or print $dbh->errstr;
                             $sth_taxon_children->execute();
                             my @children;
                             my $child;
                             $sth_taxon_children->bind_columns(\$child) or print $dbh->errstr;
                             my $id2HashNarrower;
                             while ($sth_taxon_children->fetch) {
                                   # DO STUFS WITH CHILDREN $child   <skos:narrower rdf:resource="http://skos.um.es/unescothes/C00969"/>
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
                        
                 }
                 my $prefLabel = $type_xml->createElement("skos:prefLabel");
                 $prefLabel->setAttribute("xml:lang", $isocode);
                 $prefLabel->appendText($entry);
                 $tmpChild->appendChild($prefLabel);

        }
                 
        $root->appendChild($tmpElement); #add last vocabulary  461433 example veid
        $tmpElement->appendChild($descriptionHasTopConcept);
        writeToFile($tmpElement, $vocabularyFileName.'.rdf');  #ok!
            
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
    
#=head1
    
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
 
#=cut
 
}

sub  trim($) { 
    my $s = shift; 
    $s =~ s/^\s+|\s+$//g; 
    return $s 
};


my $tmp = getVocabulary();
#print $tmp->toString;

1;
