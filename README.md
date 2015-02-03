# ESKO
Enriching Structured Knowledge Base from Open Information Extraction

Suppose, you are in this location "/home/unixUser/" the rest of the setup follows based on this directory structure.

### Compile
mvn clean package install


##Setup
CONFIG.cfg = all the parameters and setup values are provided here with respective descriptions.

### General setup

### Database setup

## Execution

### Download the source
Download the compressed file from here. This would create two folders as,
/home/unixUser/DATA/ and /home/unixUser/ESKO/. An extrensive java documentation is available under /home/unixUser/ESKO/doc/.

### Running the Pipeline
It must be noted the whole application is developed as three different components and not as an one click step.


#### Instance Matching
After the inflating the compressed file, browse to the location /home/unixUser/ESKO/ and  run from the command line

java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.DBPMappingsLoader CONFIG.cfg

./WF.1.PIPELINE.sh for running the Workflow 1 

./WF.2.PIPELINE.sh for running the Workflow 2

./WF.3.PIPELINE.sh for running the Workflow 3



The outputs will be generated under /home/unixUser/ESKO/src/main/resources/output/

This module, takes an OIE file as input, and produces instance mappings for the subject and object terms and dumps them into the database. The next step, uses these refined mappings to process further.

#### Clustering

#### Knowledge Generation


## Evaluation



# ESKO
Enriching Structured Knowledge Base from Open Information Extraction

Suppose, you are in this location "/home/unixUser/" the rest of the setup follows based on this directory structure.

### Compile
mvn clean package install


##Setup
CONFIG.cfg = all the parameters and setup values are provided here with respective descriptions.

### General setup

### Database setup

## Execution

### Download the source
Download the compressed file from here. This would create two folders as,
/home/unixUser/DATA/ and /home/unixUser/ESKO/. An extrensive java documentation is available under /home/unixUser/ESKO/doc/.

### Running the Pipeline
It must be noted the whole application is developed as three different components and not as an one click step.


#### Instance Matching
After the inflating the compressed file, browse to the location /home/unixUser/ESKO/src/main/resources/script/ from the command line and type

./WF.1.PIPELINE.sh for running the Workflow 1 

./WF.2.PIPELINE.sh for running the Workflow 2

./WF.3.PIPELINE.sh for running the Workflow 3


After this is over, run the following from the command line
java -cp target/ESKO-0.0.1-SNAPSHOT-jar-with-dependencies.jar code.dws.core.DBPMappingsLoader CONFIG.cfg


The outputs will be generated under /home/unixUser/ESKO/src/main/resources/output/

This module, takes an OIE file as input, and produces instance mappings for the subject and object terms and dumps them into the database. The next step, uses these refined mappings to process further.

#### Clustering

#### Knowledge Generation


## Evaluation



