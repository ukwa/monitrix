# Monitrix Webapp

## Installation & Configuration

* Install the [Play Framework Version 2.2.2](http://downloads.typesafe.com/play/2.2.2/play-2.2.2.zip).
* Open the file ``conf/application.conf`` and configure Cassandra access
  * Set ``cassandra.host``
  * Set ``cassandra.port``, or leave the line commented out to use the default Cassandra port (9160)
  * Set the ``cassandra.keyspace`` name
* To launch the application in development mode on the default port (9000), type ``play run``, or ``play "run {port-number}"`` for a custom port
* To launch the application in production mode on the default port, type ``play start`` or ``play "start {port-number}"`` for a custom port  
* Cassandra tips:
  * To launch cassandra from the commandline, in foreground, use ``sudo bin/cassandra -f``
  * To enter the CQL shell use ``bin/cqlsh``
  * In the shell, to drop the entire crawl_uris keyspace, use `drop keyspace crawl_uris ;`
