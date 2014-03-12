# Monitrix Loader

A backend-loader for Cassandra, to be used with the Monitrix Webapp, based on Apache commons-io and the LMAX RingBuffer/Disruptor pattern. 

# Installation and Configuration

TODO...

Before you first launch the loader, you need to create the schema in Cassandra. (File: create_tables.cql)

# Developer Information

* Make sure you have a running Cassandra instance available (current version at time of writing is 2.0.6). 
  Tip: run ``{cassandra-dir}/bin/cassandra -f`` to start Cassandra in the foreground). 
* Create the schema as instructed above.
* The loader is a Maven project and comes with a launch configuration (Maven exec plugin) for convenience. 
  Configure the command line arguments in the ``pom.xml`` before starting the loader. (A sample log file is
  included in this project at ``src/test/resources/crawl_100k.tar.gz`` - you need to uncompress this file
  before launching the loader!).

```
<commandlineArgs>src/test/resources/crawl_100k.log crawl_uris localhost 10000 2 minute</commandlineArgs>
    
# 1st arg: path to the Heritrix crawl log file
# 2nd arg: ???
# 3rd arg: Cassandra host
# 4th arg: ingest batch size (?)
# 5th arg: log file revisit interval (time unit can by 'second', 'minute', 'hour', 'day') (?)
```

* Once the loader is running, you are ready to launch the Monitrix Webapp.

# Troubleshooting

Note: on my machine, I had problems retrieving all managed dependencies. Make sure you have a settings.xml in your ~/.m2 folder like that:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<settings xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.1.0 http://maven.apache.org/xsd/settings-1.1.0.xsd" xmlns="http://maven.apache.org/SETTINGS/1.1.0"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <servers>
    <server>
      <username>admin</username>
      <id>central</id>
    </server>
    <server>
      <username>admin</username>
      <id>snapshots</id>
    </server>
  </servers>
  <activeProfiles>
    <activeProfile>artifactory</activeProfile>
  </activeProfiles>
  <!-- This seems to be *really* important -->
  <mirrors>
    <mirror>
      <id>central-mirror</id>
      <url>http://repo.maven.apache.org/maven2</url>
      <mirrorOf>*,!eclipselink</mirrorOf>
    </mirror>
  </mirrors>
</settings> 
```
 
