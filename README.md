# ESKO
Enriching Structured Knowledge Base from Open Information Extraction

Suppose, you are in this location "/home/unixUser/" the rest of the setup follows based on this directory structure.

### Compile
mvn clean package


##Setup
CONFIG.cfg = all the parameters and setup values are provided here with respective descriptions. Some of the important parameters you need to set. These are placed in order of importance in the config file. 

####1. Data process
look into folder /preProcess for bash scripts to process the NELL input. We are necessarily not looking into entity-vs-literal relations. 

***./processNell.sh NELL.08m.920.esv.csv***

***./processReverb.sh reverb_clueweb_tuples-1.1.txt***

####2. CONFIG file changes
alter the param **OIE_DATA_PATH** in CONFIG.cfg to set the location of newly generated file (Nell/reverb). Consider **INSTANCE_THRESHOLD** as well. 

####3. Script Generation for IM 
***java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.setup.ScriptGenarator CONFIG.cfg <#machines>***
this generates a scripts for the IM pipeline under the folder  **/src/main/resources/script/** in the name of **WF.x.PIPELINE.Ny.sh** where 'x' is the workflow number 1,2,3 and 'y' is the node number.
 
 
####4. Database Setup
Clean up any pre existing instance matchings with the SQL 

***delete from wikiStat.OIE_REFINED;***  This can take some time. Please be patient.


##Running IM

Open ***/src/main/resources/script/MAPPER.sh*** and change the root directory as defined under **DIR** and rockit installation location defined under **ROCKIT**. Similarly, change these two values in the file ***/src/main/resources/script/BOOTSTRAPPER.sh***

Very important, browse to the location where the scripts lie

***cd src/main/resources/script/***

Then issue the IM script command for running the whole pipeline.

***./WF.x.PIPELINE.Ny.sh*** for running the Workflow x on Node y

All the MLN related files, evidences will be generated for each relation or relation cluster (depending on the workflow) under the location **/src/main/resources/output/**. At this point all the instance mappings have been generated and stored in the respective output folders. Issu the following command to read those mappings and write to the database (table OIE_REFINED).

***java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.DBPMappingsLoader CONFIG.cfg***

##Running PM

There is a clear need to distinguish between the evidence facts and the target facts (which will lead to knowledge generation). The following line does that. It generate two files **/DATA/fPlus.dat** and  **/DATA/fMinus.dat**.

***java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.evaluation.OIEFactSeperator CONFIG.cfg***


## Execution

### Download the source
Download the compressed file from here. This would create two folders as,
/home/unixUser/DATA/ and /home/unixUser/ESKO/. An extrensive java documentation is available under /home/unixUser/ESKO/doc/.

### Running the Pipeline
It must be noted the whole application is developed as three different components and not as an one click step.


#### Instance Matching
After the inflating the compressed file, browse to the location /home/unixUser/ESKO/ and  run from the command line


./WF.1.PIPELINE.sh for running the Workflow 1 

./WF.2.PIPELINE.sh for running the Workflow 2

./WF.3.PIPELINE.sh for running the Workflow 3



The outputs will be generated under /home/unixUser/ESKO/src/main/resources/output/

This module, takes an OIE file as input, and produces instance mappings for the subject and object terms and dumps them into the database. The next step, uses these refined mappings to process further.

#### Clustering

The first step is to split the given input OIE data set file into pairs  of OIE realtions. For instance, a given OIE data set with n relations will have n*(n-1)/2 pairs. These pairs are computed and splitted into different files.
To achieve this, run the following

**java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.engine.PairSplitter CONFIG.cfg <type of file> <#machines>**

Once splitted, send the files to different machines, (probably scp), and run the following on each different machines..

**java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.engine.ComputeSimilarity CONFIG.cfg <type of Sim> <pairFile>**

e.g.
java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.engine.ComputeSimilarity CONFIG.cfg OV ../DATA/pairs.All.OIE.csv

This distributed computing speeds up the pairwise scoring, and once done, needs to be merged (manually).

After merging, we should have a pair wise relations with scores, one for wordnet, one for overlap scores.
Combine these in the ratio of beta, using the following 

**java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.analysis.BetaSearcher <WN pairwise score file> <Overlap pairwise score file>**


run mcl clustering, on each combination. 
After clustering run,
**java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.analysis.ClusterAnalyzer CONFIG.cfg**

This will compute a markov score for the optimised clusters for a given beta, b and given inflation, i.
Find the optimal. and set it in the CONFIG file. The file will be in /home/unixUser/DATA/clusters/cluster.beta.b.inf.i.out

Further optimise for optimal clusters
**java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.cluster.analysis.ClusterOptimizer CONFIG.cfg <partially optimised file from previous step>**





#### Knowledge Generation


## Evaluation


