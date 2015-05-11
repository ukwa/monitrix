# monitrix

monitrix is a monitoring/analytics frontend for the Heritrix 3 Web crawler. Visit the [Wiki](http://github.com/ukwa/monitrix/wiki) 
for information about: 

* [installing monitrix](http://github.com/ukwa/monitrix/wiki/Installation)
* [using monitrix](http://github.com/ukwa/monitrix/wiki/A-Guided-Tour-of-monitrix)
* monitrix internals:
  * [technical overview](http://github.com/ukwa/monitrix/wiki/Technical-Overview)
  * [source code layout](http://github.com/ukwa/monitrix/wiki/Project-Layout)
  * [the monitrix JSON API](http://github.com/ukwa/monitrix/wiki/JSON-API) 

## Developers: Quick Start

To __start monitrix__ in development mode, change into the project root folder and type  ``play run``.
The application will be at [http://localhost:9000](http://localhost:9000). 

To __generate an Eclipse project__, type ``play eclipse``.

## Getting Data into monitrix

To load data into monitrix, enter the 'Admin' section, and enter the absolute path of a log file in the
form. The log file should immediately appear in the list above the form, with status 'CATCHING UP' (or
'PENDING', followed shortly thereafter by 'CATCHING UP'). monitrix will now load the log file into 
the database. After the upload is complete, monitrix will continuously check the log file for updates.
__Warning:__ Loading data takes time! On my machine, a 10 GB log sample currently takes about 1 hour to
process!

Alternatively, you can also populate the database 'manually' using either of the following Java utilities,
located in the /test folder of the project:

* ``uk.bl.monitrix.util.BatchLogProcessor`` will load a log file into the database in one go and then terminate.
* ``uk.bl.monitrix.util.IncrementalLogProcessor`` will load a log file into the database, and then continue
  to monitor that file (and incrementally sync the DB) until it is terminated forcefully. 


## Ideas

It may be possible to do pretty much all of this just using Kibana?

* https://github.com/arcus-io/docker-logstash
* https://github.com/dockerfile/elasticsearch
* https://github.com/balsamiq/docker-kibana
