# Cassandra SimpleCQLMapper
The SimpleCQLMapper is a syntactic sugar on top of Datastax Cassandra driver.
The SimpleCQLMapper facilitates writing your code around CQL queries (and not around tables, like ORM mapping would) and it does not hide CQL from you. Another significant feature: it hides complexities of querying rotated tables, if you use them.
## Basic Usage
### Datamodel
Consider the following simple data model:
```
CREATE TABLE IF NOT EXIST CONTENT_BY_SHA {
    sha256  blob,
    content text,
    customer text,
    PRIMARY KEY (sha256);
};
```
### DAO class
DAO class has 3 sections:
* defining your queries, where each query is represented by interface you define.
* initialization section, where queries are prepared during startup
* data access methods, where you make use of the quesries
#### Define the query interfaces
Define your conditional-update query:
```
public interface UpdateContent extends SimpleCqlMapper<UpdateContent>
{
    SimpleCqlFactory<UpdateContent> Factory = SimpleCqlFactory.factory(UpdateContent.class, "UPDATE CONTENT_BY_SHA SET content=?, customer=? WHERE sha256=? IF NOT EXIST");
    UpdateContent sha256(byte[] sha256);
    UpdateContent content(String content);
    UpdateContent customer(String customer);
}
```
Note how UpdateContent class is being used in multiple places inside the interface declaration. This is to support fluent API. Also notice that the names and number of setters correspond to the bind parameters placeholders. If you rather use INSERT, it can be defined as follows:

Define your insert query:
```
public interface InsertContent extends SimpleCqlMapper<InsertContent>
{
    SimpleCqlFactory<InsertContent> Factory = SimpleCqlFactory.factory(InsertContent.class, "INSERT INTO CONTENT_BY_SHA (content,customer) VALUES (:content, :customer) WHERE sha256=?");
    InsertContent sha256(byte[] sha256);
    InsertContent content(String content);
    InsertContent customer(String customer);
}
```
Note how the content bind placeholder is identified using a colon.

Define select query:
```
public interface SelectContent extends SimpleCqlMapper<SelectContent>
{
    SimpleCqlFactory<SelectContent> Factory = SimpleCqlFactory.factory(SelectContent.class, "SELECT content,customer FROM CONTENT_BY_SHA WHERE sha256=?");
    SelectContent sha256(byte[] sha256);
    String content();
    String customer();
}
```
Note how the getter names correspond to the select column names.

#### Prepare once upon initialization

```
void onConnected(CassandraClusterConnector connector)
{
    UpdateContent.Factory.prepare(connector);
    InsertContent.Factory.prepare(connector);
    SelectContent.Factory.prepare(connector);
}
```
CassandraClusterConnector is a wrapper around Datastax driver's Session object.

See more on initialization and integration with Guice and PP Platform below.

#### Data access methods
##### Updates
Since the update has IF NOT EXIST clause, we want to know whether the update was applied. In this case we can transform the resulting future into boolean via ResultSet::wasApplied.
```
public CompletableFuture<Boolean> updateContent(byte[] sha256, String content, String customer)
{
    return UpdateContent.Factory.get()
        .sha256(sha256)
        .content(content)
        .customer(customer)
        .executeAsync()
        .thenApply(ResultSet::wasApplied);
}
```
##### Selects
By default the resulting future will be SelectContent typed but we rather care only about content in this call, so we map the result to a String.
```
public CompletableFuture<Optional<String>> selectContent(byte[] sha256)
{
    return SelectContent.Factory.get()
        .sha256(sha256)
        .executeAsyncAndMapOne()
        .thenApply(optionalRow -> optionalRow.map(SelectContent::content));
}
```
What if we want to return multiple fields but don't want to expose SelectContent interface to the caller? In this case we should define the query as follows:

```
public interface ContentWithCustomer
{
    String content();
    String customer();
}

public interface SelectContent extends SimpleCqlMapper<ContentWithCustomer>
{
    SimpleCqlFactory<SelectContent> Factory = SimpleCqlFactory.factory(SelectContent.class, "SELECT content,customer FROM CONTENT_BY_SHA WHERE sha256=?");
    SelectContent sha256(byte[] sha256);
}
```
Note how the SimpleCqlMapper is now parameterized by ContentWithCustomer. And the query method now looks like:
```
public CompletableFuture<Optional<ContentWithCustomer>> selectContentWithCustomer(byte[] sha256)
{
    return SelectContent.Factory.get().sha256(sha256).executeAsyncAndMapOne();
}
```
##### Selects that return collections
Let's use a table with some clustering columns. Imagine that we store content by hash-customer pair, where sha256 is still the partition key and customer becomes a clustering column.
```
CREATE TABLE IF NOT EXIST CONTENT_BY_SHA {
    sha256  blob,
    content text,
    customer text,
    PRIMARY KEY (sha256, customer);
};
```
Now the select query can be defined identically as we saw above.

But our query method would look different, we now use executeAsyncAndMap instead of executeAsyncAndMapOne.
```
public CompletableFuture<Collection<ContentWithCustomer>> selectContentWithCustomer(byte[] sha256)
{
    return SelectContent.Factory.get().sha256(sha256).executeAsyncAndMap();
}
```
##### Selects that return single value
Suppose you want to return a single aggregate value, or a static column from your query method. Use the Optional::map method to re-map the result.
```
public interface SelectCount extends SimpleCqlMapper<SelectCount>
{
    SimpleCqlFactory<SelectCount> Factory = SimpleCqlFactory.factory(SelectCount.class, "SELECT count(*) as contentCount FROM CONTENT_BY_SHA LIMIT 100000");
    int contentCount();
}
  
public CompletableFuture<Optional<Integer>> countOfContent()
{
    return SelectContent.Factory.get().sha256(sha256).executeAsyncAndMapOne().thenApply(optionalResult -> optionalResult.map(SelectCount::theCount));
}
```
#### Integration with Guice and Proofpoint platform
In the application DAO-specific module, bind DAO class, named cassandra config and connector.
```
public class EventBackupModule extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.requireExplicitBindings();
        binder.disableCircularProxies();
        
        // bind named cassandra config
        ConfigurationModule.bindConfig(binder)
                .annotatedWith(Names.named("eventcenter"))
                .prefixedWith("eventcenter")
                .to(CassandraProperties.class);
    
        // bind the DAO object(s)
        binder.bind(EventBackupDAO.class).in(Scopes.SINGLETON);
        //... other DAO classes
        
        healthBinder(binder).export(ConfigValuesDao.class).withNameSuffix("cassandra");
    }

    @Provides
    @Singleton
    @Named("configcenter")
    CassandraClusterConnector provideConfigCenterCassandraDao(@Named("eventcenter") CassandraProperties config)
    {
        return new CassandraClusterConnector(config);
    }
}
```
The DAO class is injected with the connector
```
    @Inject
    public EventBackupDAO(@Named("eventcenter") CassandraClusterConnector connector)
    {
        this.connector = connector;
        connector.addConnectListener(this::onConnected);
    }

    @AcceptRequests
    public void init()
    {
        connector.initialize();
    }

    void onConnected(CassandraClusterConnector connector)
    {
        int nTables = config.getNumRotationPeriods();
        InsertEventBackup.Factory.rotations(nTables).autoTruncate()
                .expirationMs(config.getExpirationPeriod().toMillis())
                .rotationMs(config.getRotationPeriod().toMillis()).prepare(connector);
        ...
    }
    
    public CompletableFuture<Boolean> storeEventBackup(String ec_id, String topic, short partition, byte[] event_body, TimeUUID id)
    {
        return InsertEventBackup.Factory.get().ec_id(ec_id).topic(topic).partition(partition).event_body(event_body).id(id)
                .executeAsync()
                .thenApply(ResultSet::wasApplied);
    }
```
Using the DAO in the application:
```
    @Inject
    public ECProducerCassandraBackup(EventBackupDAO dao)
    {
        this.dao = dao;
    }

    public void storeBackup(String clusterID, String topic, short partition, byte[] payload)
    {
        this.partition = (short) partition;

        TimeUUID cassandraID = TimeUUID.now();
        storeEventFutures.add(dao.storeEventBackup(clusterID, topic, (short) partition, payload, cassandraID));
    }
```
## Table rotation
Now imagine that our content constantly expires. But because content column is huge, we don't want to use compaction (which causes write amplification). Instead, we want to use a set of round-buffer tables and truncate when table goes out of scope.
SimpleCQLMapper library can be useful in abstracting out the table rotation complexities. But first, let's identify the problem.

### Rolling data - TWCS or bust? No.
FACT: Cassandra is not designed for time-series data, queues, and queue-like datasets (see https://www.datastax.com/dev/blog/cassandra-anti-patterns-queues-and-queue-like-datasets). nCassandra is a log-structured merge tree (LSM) and data is never modified in-place. Column delete or expiration is written as soft-delete and executed later via compaction process.

TWCS is perfect if your data has fixed expiration window (columns and/or TTL never updated) and you don't need Cassandra counters and your content is not huge. Otherwise, the only other solution is table rotation.

Cassandra assumes no in-place data modification, postponing the update resolution to reading time. Due to that, the kinds of problems that occur:

* Holes in SSTables due to defunct data (explicitly deleted, via default TTL or via USING TTL) occupies space and contributes to data fragmentation. Reads must hop over multiple SSTables.
* SSTables data fragmentation because columns are updated over time. Reads must hop over multiple SSTables.
* Write amplification - due to compaction, same data is written and rewritten multiple times.


| Use case | Can use TWCS | Can use LCS	| Must use something else | Comments |
| --- | :---: | :---: | :---: | --- |
| Data is immutable (never updated after 1st write) | yes | | | all data expires at once. After time window compaction further compactions don't run. Uses STCS underneath. So may not be suitable for storing huge blobs.
| Data expires | yes | | | ditto
| Data has the same TTL (TTL never updated) | yes | | | ditto 
| Data is pseudo static. Much more reads than writes | why would you?	| yes | | LCS favors reads
| Data is mutable but mutations localized over time | depends | yes	|  | 	LCS favors reads
| Data is mutable and mutations spread over time |	maybe	 |  | 	yes	 | use table rotation strategy for expiration
| Counters are used  |	can not	  |  |	yes	 |use table rotation strategy for expiration
| Data (blobs) is huge |	should not	 | 	 | yes |	try to avoid compaction (to avoid write amplification)
### Using rotation tables and TRUNCATE
Define tables as follows:
```
CREATE TABLE_0 { ... }
CREATE TABLE_1 { ... }
CREATE TABLE_2 { ... }
CREATE TABLE_3 { ... }
```
Calculate the table-ID using time-based index, for example:
```
int tableID = (getCassandraCurrentTimeMillis() / rotationInMillis ) % 4
```
Always write into the current table.

Select from the current table or previous table depending on your data expiration time. Choosing table rotation time twice the expiration time will reduce the amount of 2-table queries.

You can use automatic truncation or schedule a thread to manually truncate older tables on schedule via:
```
session.execute("TRUNCATE TABLE_"+getFutureTableRotationID());
```
### Define the query
Define your insert query:
```
public interface InsertContent extends SimpleCqlMapper<InsertContent>
{
    SimpleCqlFactory<InsertContent> Factory = SimpleCqlFactory.factory(InsertContent.class, "UPDATE TABLE_$(TID) SET content=? WHERE sha256=?");
    InsertContent sha256(byte[] sha256);
    InsertContent content(String content);
}
```
Define select query:
```
public interface SelectContent extends SimpleCqlMapper<SelectContent>
{
    SimpleCqlFactory<SelectContent> Factory = SimpleCqlFactory.factory(SelectContent.class, "SELECT content FROM TABLE_$(TID) WHERE sha256=?");
    SelectContent sha256(byte[] sha256);
    String content();
}
```
### Prepare once upon initialization
```
void onConnected(CassandraClusterConnector connector)
{
    int nTables = config.getNumRotationPeriods();
    long expirationPeriod = config.getExpirationPeriod();
    long rotationPeriod = expirationPeriod * 2;
    InsertContent.Factory
        .rotations(nTables)
        .expirationMs(expirationPeriod)
        .rotationMs(rotationPeriod)
        .prepare(connector);
    SelectContent.Factory
        .rotations(nTables)
        .expirationMs(expirationPeriod)
        .rotationMs(rotationPeriod)
        .prepare(connector);
}
```
### Updates
Update is simple, it always goes to the current table.
```
public CompletableFuture<Boolean> insertContent(byte[] sha256, String content)
{
    return InsertContent.Factory.get()
        .sha256(sha256)
        .content(content)
        .executeAsync()
        .thenApply(ResultSet::wasApplied);
}
```
### Selects
Selects are more tricky because we need to optimize how we query Cassandra to minimize load and speed up the reads. There are several criteria that SimpleCqlMapper employs, query in parallel or in-sequence and how many tables to query. All this logic is transparent to the calling code. 
```
public CompletableFuture<Optional<SelectContent>> selectContent(byte[] sha256)
{
    SelectContent.Factory.get()
        .sha256(sha256)
        .executeAsyncAndMapOne();
}
```
### Using specific table to insert or query
Use `SelectContent.Factory.tid(tid).get()` or `InsertContent.Factory.tid(tid).get()` to query or insert into a specific table.

### Query multiple tables
Use `SelectContent.Factory.faster(nTables).get()` to query multiple tables in parallel - for collections, or use `SelectContent.Factory.orderly(nTables).get()` to query in sequence and return once a result found.

### List of query options
If you don't specify any query options then SimpleCqlMapper picks the most optimal option. But programmer can set specific option:

| Query options | Description |
| --- | --- |
Factory.tid(tid).get()	| query specific table with ID tid. It is programmer's responsibility to calculate the current TID.
Factory.faster(numTables).get()	| simultaneously query a number of rotation tables and merge all results. For one-item queries a random result will be picked, which is why this option is not recommended in this case.
Factory.orderly(numTables).get() |	sequentially query a number rotation tables and return first found. This option is not recommended for collection-returning queries since they can be called in parallel.
Factory.faster().get()	| query current table or query both tables (current and previous, when during table transition) and return all.
Factory.orderly().get()	| try current rotation and only if nothing found, query the previous. This option is not recommended for collection-returning queries since they can be called in parallel.
Factory.both().get()	| simultaneously try both current and previous rotation tables and return all (collection) or last found (one item).
Factory.any().get()	| sequentially try both current and previous rotation tables and return all (collection) or first found (one item).
Factory.all().get()	| query all tables in parallel (collection) or sequentially (one item).
Factory.last().get() |	sequentially try both current and previous rotation tables and return all (collection) or last found (one item).
default, Factory.get() |	most optimal strategy will be picked.
### Automatic truncation
The SimpleCqlMapper will automatically truncate next current table half-period in advance. To enable that, use autoTruncate option, like so:
```
void onConnected(CassandraClusterConnector connector)
    {
        int nTables = config.getNumRotationPeriods();
        long expirationPeriod = config.getExpirationPeriod();
        long rotationPeriod = expirationPeriod * 2;
        InsertContent.Factory
            .rotations(nTables)
            .autoTruncate()
            .expirationMs(expirationPeriod)
            .rotationMs(rotationPeriod)
            .prepare(connector);
```
### Managing Cassandra timestamp skew
Truncation depends on strict synchronization of client side vs Cassandra side timestamps. The SimpleCqlMapper will automatically calculate clock skew between local system and remote Cassandra - during initialization. During queries some time padding is used to make sure the previous period table is still queried within plus-minus padding period.

### References
* http://thelastpickle.com/blog/2016/12/08/TWCS-part1.html
* https://docs.datastax.com/en/cassandra/3.0/cassandra/dml/dmlHowDataMaintain.html
* https://docs.datastax.com/en/cassandra/3.0/cassandra/operations/opsConfigureCompaction.html
