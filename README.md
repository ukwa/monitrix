# monitrix

A monitoring system for Heritrix 3.

## Getting Started

To start the application in development mode, change into the project root folder and type 

    play run
    
The application will be at [http://localhost:9000](http://localhost:9000). To (properly) shut down, hit CTRL-D. 

To generate an Eclipse project, type

    play eclipsify
    
## Using MongoDB

monitrix needs access to a [MongoDB](http://www.mongodb.org) NoSQL database server.

* Use the ``mongod`` command to start MongoDB (hint: ``mongod --help``)
* MongoDB provides a Web admin dashboard  at [http://localhost:28017](http://localhost:28017). Make sure
  you start MongoDB with the ``--rest`` option (when in dev mode) to enable full dashboard functionality.
* Use ``mongo <dbname> --eval "db.dropDatabase()"`` to drop a DB 

## Loading Test Data into MongoDB

monitrix doesn't load data into MongoDB yet. You can load data manually with the the following Java application
class, located in the /test folder:

    uk.bl.monitrix.util.LogProcessor
    
Edit the path to the Heritrix log you want to load, and start the application e.g. from your IDE.
