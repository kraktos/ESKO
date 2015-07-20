# ESKO
Enriching Structured Knowledge Base from Open Information Extraction

Suppose, you are in this location "/home/unixUser/" the rest of the setup follows based on this directory structure.

##Project Structure

some root directory, lets say **/home/adutta/git**. The full project structure should look like this.

|**/home/adutta/git**

|---**ESKO**

|------pom.xml

|------CONFIG.cfg

|------src (and doc and rest)


|---**DATA**(OIE data files)

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

####3. Script Generation
***java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.setup.ScriptGenarator CONFIG.cfg <#machines>***
this generates a scripts for the IM pipeline under the folder  **/src/main/resources/script/** in the name of **WF.x.PIPELINE.Ny.sh** where 'x' is the workflow number 1,2,3 and 'y' is the node number.
 
 
####4. Database Setup
Since, we use the most frequent wikipedia sense in out IM step, the whole sql file can be downlaoded from web.informatik.uni-mannheim.de/adutta/wikiPrep.tar.gz

This is a self contained sql and issuing the command 

***source wikiPrep.sql*** 

from the mySQL command prompt will install the data in the DB. Clean up any pre existing instance matchings with the SQL 

***delete from wikiStat.OIE_REFINED;***  This can take some time. Please be patient.


##Instance Matching (IM)

Open ***/src/main/resources/script/MAPPER.sh*** and change the root directory as defined under **DIR** and rockit installation location defined under **ROCKIT**. Similarly, change these two values in the file ***/src/main/resources/script/BOOTSTRAPPER.sh***

Very important, browse to the location where the scripts lie

***cd src/main/resources/script/***

Then issue the IM script command for running the whole pipeline.

***./WF.x.PIPELINE.Ny.sh*** for running the Workflow x on Node y

All the MLN related files, evidences will be generated for each relation or relation cluster (depending on the workflow) under the location **/src/main/resources/output/**. At this point all the instance mappings have been generated and stored in the respective output folders. Issu the following command to read those mappings and write to the database (table OIE_REFINED).

***java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.DBPMappingsLoader CONFIG.cfg***

##Property Matching (PM)

There is a clear need to distinguish between the evidence facts and the target facts (which will lead to knowledge generation). The following line does that. It generate two files **/DATA/fPlus.dat** and  **/DATA/fMinus.dat**. Since it queries DBpedia endpoint, it can take sometime.

***java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.evaluation.OIEFactSeperator CONFIG.cfg***

###PM: Rule based approach

1. Since this is based on generating association rules, first, run the association generator. internally it uses the **/DATA/fPlus.dat** as generated in the former step.

***java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.propertyMap.GenerateAssociations CONFIG.cfg <0/1>***

The last argument says, if you want to use the refined output (1) from the IM step or use top-1 (0) mappings.

2. Once the associations are generated, use the following command to actually generate the mappings.

***java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.propertyMap.RegressionAnalysis CONFIG.cfg threshold***

Set threshold to any desired value between 0-100, this is a percentage, so 3 means 3%. The actual property mappings are generated in a file as **/DATA/PROPERTY_MAPPINGS_DIRECT_THRESH_<threshold>_WF_#.tsv**. Start playing with it. # denotes the workflow you set in the CONFIG file.

###PM: Cluster based approach


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

## Additional

You might be interested to gain some initial insights into the input data set. Here you have few utility classes which would generate some data files. The R scripts are also provided. Using those, it is possible to generate plots showing the data behaviours. 

###1.Instance distributions

This class generates a file called **/DATA/oie.properties.instances.distribution.tsv**. This file has two columns, first one is "Instance Count"  and second is "No. of relations with that instance count", Hence, an entry like (233, 4) means there are 4 relations in the input data set with 233 instances each.

***java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.statistics.DataAnalyzer CONFIG.cfg***

The associated R script plots this behavior by plotting the instance counts range on x-axis as a buckets, with the y-axis denoting the frequencty or no. of relations falling into that bucket.




