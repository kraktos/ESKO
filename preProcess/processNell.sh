#!/bin/bash
awk -F'\t' '{ print $1","$2","$3","$5}' $1 > x.csv
sed '/concept:latitudelongitude/d' x.csv > y.csv
sed '/concept:atdate/d' y.csv > z.csv
sed '/haswikipediaurl/d' z.csv > w.csv
sed 's/concept://g' w.csv > v.csv
sed '/medicalprocedure/d' v.csv > m.csv
sed '/everypromotedthing/d' m.csv > n.csv
sed '1d;$d' n.csv > Nell_new.csv
rm x.csv
rm y.csv
rm z.csv
rm w.csv
rm v.csv
rm m.csv
rm n.csv
