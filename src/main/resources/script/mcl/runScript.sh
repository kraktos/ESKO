#!/bin/bash


for (( c=2; c<=30; c++ ))
  do
     ./bin/mcl  $1 --abc -I $c -o /home/adutta/git/DATA/clusters/mcl.beta.$2.inf.$c.output
 done