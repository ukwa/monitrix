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

To __generate an Eclipse project__, type ``play eclipsify``.

## Getting Data into monitrix

monitrix doesn't load data by itself (yet). At the moment, you need to load data 'manually', using the Java utility 
``uk.bl.monitrix.util.BatchLogProcessor`` that's located in the /test folder of the project. Edit the path to
the Heritrix log you want to load, and start the application (e.g. from your IDE). 

__Warning:__ Loading data takes time! On my machine, a 10 GB log sample currently takes about 50 minutes to process!

## MongoDB Cheat Sheet

* Use the ``mongod`` command to start MongoDB (hint: ``mongod --help``)
* The MongoDB admin dashboard is at  [http://localhost:28017](http://localhost:28017). Make sure
  you start MongoDB with the ``--rest`` option (when in dev mode) to enable full dashboard functionality. (Note: on my system
  ``sudo mongod --dbpath /var/lib/mongodb --rest`` works fine.)
* Use ``mongo monitrix --eval "db.dropDatabase()"`` to drop the monitrix DB (replacing 'monitrix' with your database name, in case you changed the default.) 
* MongoDB REST interface docs are [here](http://www.mongodb.org/display/DOCS/Http+Interface#HttpInterface-SimpleRESTInterface).
