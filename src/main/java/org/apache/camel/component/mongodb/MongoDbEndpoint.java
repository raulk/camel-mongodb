/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.mongodb;

import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.impl.DefaultMessage;
import org.apache.camel.util.ObjectHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

/**
 * Represents a MongoDb endpoint.
 */
public class MongoDbEndpoint extends DefaultEndpoint {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDbEndpoint.class);
    private Mongo mongoConnection;
    private String database;
    private String collection;
    private MongoDbOperation operation;
    private boolean createCollection = true;
    private boolean invokeGetLastError = false;
    private WriteConcern writeConcern;
    private WriteConcern writeConcernRef;
    // ReadPreference not used
    private ReadPreference readPreference;
    private boolean dynamicity = false;
    // tailable cursor consumer by default
    private MongoDbConsumerType consumerType;
    private long cursorRegenerationDelay = 1000l;
    private String tailTrackIncreasingField;
    
    // persitent tail tracking
    private boolean persistentTailTracking = false;
    private String persistentId;
    private String tailTrackDb;
    private String tailTrackCollection;
    private String tailTrackField;
    
    private MongoDbTailTrackingConfig tailTrackingConfig;
    
    private DBCollection dbCollection;
    private DB db;

    public MongoDbEndpoint() {
    }

    public MongoDbEndpoint(String uri, MongoDbComponent component) {
        super(uri, component);
    }

    @SuppressWarnings("deprecation")
    public MongoDbEndpoint(String endpointUri) {
        super(endpointUri);
    }

    public Producer createProducer() throws Exception {
        validateOptions('P');
        initializeConnection();
        return new MongoDbProducer(this);
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        validateOptions('C');
        // we never create the collection
        createCollection = false;
        initializeConnection();
                
        // select right consumer type
        if (consumerType == null) {
            consumerType = MongoDbConsumerType.tailable;
        }
        
        Consumer consumer = null;
        if (consumerType == MongoDbConsumerType.tailable) {
            consumer = new MongoDbTailableCursorConsumer(this, processor);
        } else {
            throw new CamelMongoDbException("Consumer type not supported: " + consumerType);
        }
        
        return consumer;
    }

    private void validateOptions(char role) throws IllegalArgumentException {
        // make our best effort to validate, options with defaults are checked against their defaults, which is not always a guarantee that
        // they haven't been explicitly set, but it is enough
        if (role == 'P') {
            if (!ObjectHelper.isEmpty(consumerType) || persistentTailTracking || !ObjectHelper.isEmpty(tailTrackDb) || 
                    !ObjectHelper.isEmpty(tailTrackCollection) || !ObjectHelper.isEmpty(tailTrackField) || cursorRegenerationDelay != 1000l) {
                throw new IllegalArgumentException("consumerType, tailTracking, cursorRegenerationDelay options cannot appear on a producer endpoint");
            }
        } else if (role == 'C'){
            if (!ObjectHelper.isEmpty(operation) || !ObjectHelper.isEmpty(writeConcern) || writeConcernRef != null || 
                    readPreference != null || dynamicity || invokeGetLastError) {
                throw new IllegalArgumentException("operation, writeConcern, writeConcernRef, readPreference, dynamicity, invokeGetLastError " +
                		"options cannot appear on a consumer endpoint");
            }
            
            if (consumerType == MongoDbConsumerType.tailable) {
                if (tailTrackIncreasingField == null) {
                    throw new IllegalArgumentException("tailTrackIncreasingField option must be set for tailable cursor MongoDB consumer endpoint");
                }
                if (persistentTailTracking && (ObjectHelper.isEmpty(persistentId))) {
                    throw new IllegalArgumentException("persistentId is compulsory for persistent tail tracking");
                }
            }
            
        } else {
            throw new IllegalArgumentException("Unknown endpoint role");
        }
    }

    public boolean isSingleton() {
        return true;
    }
    
    /**
     * Initialises the MongoDB connection using the Mongo object provided to the endpoint
     * @throws CamelMongoDbException
     */
    public void initializeConnection() throws CamelMongoDbException {
        LOG.info("Initialising MongoDb endpoint: {}", this.toString());
        if (database == null || collection == null) {
            throw new CamelMongoDbException("Missing required endpoint configuration: database and/or collection");
        }
        db = mongoConnection.getDB(database);
        if (db == null) {
            throw new CamelMongoDbException("Could not initialise MongoDbComponent. Database " + database + " does not exist.");
        }
        if (!createCollection && !db.collectionExists(collection)) {
            throw new CamelMongoDbException("Could not initialise MongoDbComponent. Collection " + collection + " and createCollection is false.");
        }
        dbCollection = db.getCollection(collection);
        
        LOG.info("MongoDb component initialised and endpoint bound to MongoDB collection with the following paramters. Address list: {}, Db: {}, Collection: {}", new Object[] { mongoConnection.getAllAddress().toString(), db.getName(), dbCollection.getName() });
    }
    
    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getCollection() {
        return collection;
    }

    public void setOperation(String operation) throws CamelMongoDbException {
        try {
            this.operation = MongoDbOperation.valueOf(operation);
        } catch (IllegalArgumentException e) {
            throw new CamelMongoDbException("Operation not supported", e);
        }
    }

    public MongoDbOperation getOperation() {
        return operation;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getDatabase() {
        return database;
    }

    /**
     * Create collection if it doesn't exist. Default is true.
     */
    public void setCreateCollection(boolean createCollection) {
        this.createCollection = createCollection;
    }

    public boolean isCreateCollection() {
        return createCollection;
    }

    public DB getDb() {
        return db;
    }

    public DBCollection getDbCollection() {
        return dbCollection;
    }
    
    public void setMongoConnection(Mongo mongoConnection) {
        this.mongoConnection = mongoConnection;
    }

    public Mongo getMongoConnection() {
        return mongoConnection;
    }

    /**
     * Set the WriteConcern for write operations on MongoDB. 
     * @param writeConcern the standard name of the WriteConcern, as per the definition of WriteConcern.valueOf(String s) method
     * @see <a href="http://api.mongodb.org/java/current/com/mongodb/WriteConcern.html#valueOf(java.lang.String)">possible options</a>
     */
    public void setWriteConcern(String writeConcern) {
        this.writeConcern = WriteConcern.valueOf(writeConcern);
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public void setInvokeGetLastError(boolean invokeGetLastError) {
        this.invokeGetLastError = invokeGetLastError;
    }

    public boolean isInvokeGetLastError() {
        return invokeGetLastError;
    }

    /**
     * Set the WriteConcern for write operations on MongoDB, passing in a custom WriteConcern you have instantiated in the Registry.
     * You can also use standard WriteConcerns by passing in their key. See the {@link #setWriteConcern(String) setWriteConcern} method.
     * @param writeConcern the name of the bean in the registry that represents the WriteConcern to use
     */
    public void setWriteConcernRef(String writeConcernRef) {
        WriteConcern wc = this.getCamelContext().getRegistry().lookup(writeConcernRef, WriteConcern.class);
        if (wc == null) {
            LOG.error("Camel MongoDB component could not find the WriteConcern in the Registry. Verify that the " +
            		"provided bean name ({}) is correct. Aborting initialization.", writeConcernRef);
            throw new IllegalArgumentException("Camel MongoDB component could not find the WriteConcern in the Registry");   
        }
    
        this.writeConcernRef = wc;
    }

    public WriteConcern getWriteConcernRef() {
        return writeConcernRef;
    }
    
    /**
     * Applies validation logic specific to this endpoint type. If everything succeeds, continues initialization
     */
    @Override
    public void start() throws Exception {
        if (writeConcern != null && writeConcernRef != null) {
            LOG.error("Cannot set both writeConcern and writeConcernRef at the same time. Respective values: {}, {}. " +
            		"Aborting initialization.", new Object[] {writeConcern, writeConcernRef});
            throw new IllegalArgumentException("Cannot set both writeConcern and writeConcernRef at the same time on MongoDB endpoint"); 
        }
        
        setWriteReadOptionsOnConnection();
        super.start();
    }
    
    
    public Exchange createMongoDbExchange(DBObject dbObj) {
        Exchange exchange = new DefaultExchange(this.getCamelContext(), getExchangePattern());
        Message message = new DefaultMessage();
        message.setHeader(MongoDbConstants.DATABASE, database);
        message.setHeader(MongoDbConstants.COLLECTION, collection);
        message.setHeader(MongoDbConstants.FROM_TAILABLE, true);
        
        message.setBody(dbObj);
        exchange.setIn(message);
        return exchange;
    }

    private void setWriteReadOptionsOnConnection() {
        // Set the WriteConcern
        if (writeConcern != null) {
            mongoConnection.setWriteConcern(writeConcern);
        }
        else if (writeConcernRef != null) {
            mongoConnection.setWriteConcern(writeConcernRef);
        }
        
        // Set the ReadPreference
        if (readPreference != null) {
            mongoConnection.setReadPreference(readPreference);
        }
    }

    public void setReadPreference(String readPreference) {
        Class<?>[] innerClasses = ReadPreference.class.getDeclaredClasses();
        for (Class<?> inClass : innerClasses) {
            if (inClass.getSuperclass() == ReadPreference.class && inClass.getName().equals(readPreference)) {
                try {
                    this.readPreference = (ReadPreference) inClass.getConstructor((Class<?>) null).newInstance((Object[]) null);
                } catch (Exception e) {
                   continue;
                }
                break;
            }
        }
        
        LOG.error("Could not resolve specified ReadPreference of type {}. Read preferences are resolved from inner " +
        		"classes of com.mongodb.ReadPreference.", readPreference);
        throw new IllegalArgumentException("MongoDB endpoint could not resolve specified ReadPreference");
    }

    public ReadPreference getReadPreference() {
        return readPreference;
    }

    public void setDynamicity(boolean dynamicity) {
        this.dynamicity = dynamicity;
    }

    public boolean isDynamicity() {
        return dynamicity;
    }

    public void setConsumerType(String consumerType) throws CamelMongoDbException {
        try {
            this.consumerType = MongoDbConsumerType.valueOf(consumerType);
        } catch (IllegalArgumentException e) {
            throw new CamelMongoDbException("Consumer type not supported", e);
        }
    }

    public MongoDbConsumerType getConsumerType() {
        return consumerType;
    }
    
    public String getTailTrackDb() {
        return tailTrackDb;
    }

    public void setTailTrackDb(String tailTrackDb) {
        this.tailTrackDb = tailTrackDb;
    }

    public String getTailTrackCollection() {
        return tailTrackCollection;
    }

    public void setTailTrackCollection(String tailTrackCollection) {
        this.tailTrackCollection = tailTrackCollection;
    }

    public String getTailTrackField() {
        return tailTrackField;
    }

    public void setTailTrackField(String tailTrackField) {
        this.tailTrackField = tailTrackField;
    }

    public void setPersistentTailTracking(boolean persistentTailTracking) {
        this.persistentTailTracking = persistentTailTracking;
    }

    public boolean isPersistentTailTracking() {
        return persistentTailTracking;
    }

    public void setTailTrackIncreasingField(String tailTrackIncreasingField) {
        this.tailTrackIncreasingField = tailTrackIncreasingField;
    }

    public String getTailTrackIncreasingField() {
        return tailTrackIncreasingField;
    }

    public MongoDbTailTrackingConfig getTailTrackingConfig() {
        if (tailTrackingConfig == null) {
            tailTrackingConfig = new MongoDbTailTrackingConfig(persistentTailTracking, tailTrackIncreasingField, 
                    tailTrackDb == null ? database : tailTrackDb, tailTrackCollection, tailTrackField, getPersistentId());
        }
        return tailTrackingConfig;       
    }

    public void setCursorRegenerationDelay(long cursorRegenerationDelay) {
        this.cursorRegenerationDelay = cursorRegenerationDelay;
    }

    public long getCursorRegenerationDelay() {
        return cursorRegenerationDelay;
    }

    public void setPersistentId(String persistentId) {
        this.persistentId = persistentId;
    }

    public String getPersistentId() {
        return persistentId;
    }

}
