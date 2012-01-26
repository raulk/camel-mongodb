Camel MongoDB component
=======================

The camel-mongodb allows you to interact with MongoDB databases directly from your Camel routes, without creating beans and, for most uses,
without having to create custom processors either.

Designed from the ground up to be simple, lightweight and convenient, here are the highlighted features:

 * Supports the following MongoDB operations, as a producer endpoint: 
    * Query operations: findById, findOneByQuery, findAll, count (support for group and mapReduce is slated for future versions)
    * Write operations: insert, save, update
    * Delete operations: remove 
    * Other operations: getDbStats, getColStats (to automate monitoring via Camel)
 * Tailable cursor consumer endpoint. Can bind a consumer to a capped collection and keep consuming real-time as documents are inserted. 
   See http://www.mongodb.org/display/DOCS/Tailable+Cursors.
 * Persistent tail tracking. If you shutdown a tailable cursor consumer, you can ensure it picks up from where it left off by enabling
   persistent tail tracking. Supports any datatype supported by MongoDB (String, Dates, ObjectId, etc.) for its increasing correlation key.
 * Automatic cursor regeneration for tailable consumer endpoints after server timeouts. Can set a custom cursorRegenerationDelay.
 * Paging support via skip() and limit(). Values specified in message headers.
 * Supports upserts (atomic insert/update) and multiUpdates in the update operation.
 * Query operations support field filtering (to only fetch specific fields from matching documents) and sorting.
 * Simple and extensible endpoint configuration, revolving around a org.mongodb.Mongo instance that you create in your Registry
 * Database and collection to bind to are configurable as endpoint options, but can be dynamic for each Exchange processed (via Message Headers)
   For increased throughput, you need to set dynamicity=true in the endpoint to advise the component to compute the DB/Collection for each 
   incoming exchange.
 * Can reuse same Mongo instance for as many endpoints as you wish.
 * WriteConcern can be set at the endpoint level or at the Exchange level, using a standard one (see constant fields in MongoDB's 
   WriteConcern Javadoc) or creating a custom one in your Registry.
 * Quickly instruct producer endpoints to call getLastError() after each write operation without setting a custom WriteConcern by using
   option invokeGetLastError=true.
 
To run the unit tests
---------------------

Make sure you have a MongoDB instance running somewhere. 
If you are running it on localhost and on port 27017 (default port), you need not do anything else.
Otherwise, ensure the appropriate connection URI is specified in the src/test/mongodb.test.properties file.

- Initial contribution by Raúl Kripalani.