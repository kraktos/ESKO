#!/bin/bash
awk -F'\t' '{if($9 >= 0.95) {print $2";"$3";"$4";"$9}}' $1 > highAll.csv
awk -F';' '$1 !~ /[0-9]/ && $3 !~ /[0-9]/  {print $1";"$2";"$3";"$4}' highAll.csv > noDigitHighAll.csv

