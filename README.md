
Maven Project for Scala Spark exercise
=====================================

# Introduction

This archive contains an Maven project for Scala Spark application.

# Details

The `pom.xml` dependencies : -

* Spark 2.3.2
* scala 2.11.11
* SLF4J
* LOG4J (acts as logging implementation for SLF4J)
* grizzled-slf4 a Scala specific wrapper for SLF4J.
* typesafe for config.
* scalatest for testing.

# Properties to configure 
  file : src/main/resources/application.conf 
  * app_name          --Appname
  * master            --cluster (local[*]/yarn)
  * num_partitions    --number of partitions
  * output_dir        --Output directory to where the output of the program written
  * title.ratings.tsv --title.ratings file path
  * title.basics.tsv  --title.basics file path
  * name.basics.tsv   --name.basics file path
  * title.principals.tsv --title.basics file path
  * title.akas.tsv    -- title.akas file path

# Pom includes two exec goals: -
* `exec:exec@run-local` - run the code using local spark standalone cluster
* `exec:exec@run-yarn` - run the code on a remote yarn cluster. In order for this to work the core-site.xml and yarn-site.xml configuration files from the remote cluster must be copied into the spark-remote/conf directory.

# run tests -
Either right click project and select Run As -> Maven test (eclpse IDE) /run maven command- `mvn clean test` 

# run spark stanalone cluster
run maven command - `mvn clean install exec:exec@run-local -DskipTest`

# run spark yarn cluster
run maven command - `mvn clean install exec:exec@run-yarn -DskipTest`

**(In order for this to work the core-site.xml and yarn-site.xml configuration files from the remote cluster must be copied into the spark-remote/conf directory).

# Output files (in output directory)
* top-movies.txt --contains top 20 movies(Ignored `averageNumberOfVotes` calculation, since its a costant, no differennce in the outcome)
* often-credited-persons.txt --List of persons who are most often credited
* title-akas.txt --Different titles of the top 20 movies

# Data Files 
* Documentation - http://www.imdb.com/interfaces/
* Download - https://datasets.imdbws.com/
