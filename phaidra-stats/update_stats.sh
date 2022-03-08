#!/bin/bash

exec 2>&1

source /home/adminp7/perl5/perlbrew/etc/bashrc

perlbrew exec -q perl /usr/local/phaidra/utils/phaidra-stats/update_stats.pl "$@"
