#!/bin/bash

DIR="/home/adutta/git/ESKO"
ROCKIT="/home/adutta/rockit"

# CHANGE TO THE RELEVANT DIRECTORY
cd $DIR/

echo " \n\n ========== RUNNING BOOTSTRAP FOR  " $1 " ITERATION " $2 " ============ "

java -jar ../DATA/EXECUTABLES/ESKO_BOOTSTRAPPER.jar $1 CONFIG.cfg

cat $ROCKIT'/modelBasic.mln' 'src/main/resources/output/ds_'$1'/domRanEvidenceBS.db'  > $ROCKIT'/model.mln'

cd $ROCKIT/


java -Xmx20G -jar rockit-0.3.228.jar -input model.mln -data $DIR'/src/main/resources/output/ds_'$1'/AllEvidence.db' -output $DIR'/src/main/resources/output/ds_'$1'/outAll.db'

# COPY FILES

cp $DIR'/src/main/resources/output/ds_'$1'/outAll.db' $DIR'/src/main/resources/output/ds_'$1'/out.db'

cp $DIR'/src/main/resources/output/ds_'$1'/domRanEvidenceBS.db' $DIR'/src/main/resources/output/ds_'$1'/domRanEvidence_A'$2'.db'



