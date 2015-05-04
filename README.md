DynamicLOD
==========

Source code of Dynamic LOD Cloud based on DataIDs files. More info about DataID Unit please check http://wiki.dbpedia.org/coop/DataIDUnit. 


## Requirements
This project uses external tools that you must install before start using.
We use MongoDB to save relevant metadata for creation of linksets. Thus, for MongoDB the default installation is sufficient: `sudo apt-get install mongodb-server`. We also need rapper tool to parse files and you can install via apt-get `sudo apt-get install raptor-utils`. To compile and run the project, you need maven `sudo apt-get install maven` (version > 3.x).

Important!!! After cloning the project from this repository, please access the folder /resources and edit the properties configuration file.

## How to use

#### Instalation process

After cloning the project, open the project root folder and type: `mvn clean install`. Maven will then download all dependencies and compile the project.


#### Starting Jetty server

In order to run the project you need to start the Jetty server using the following command:
`mvn jetty:start`

 Now the server must be acessible at the address:
`http://localhost:8080/dataid/index.xhtml` .

After adding dataid files, you should create the links beetween datasets. For this, click "Update Linksets" and wait all linksets to be created.

