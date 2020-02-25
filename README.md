# Cassandra set up

First create the correct keyspace:

```cql
create keyspace logstreamcassandra with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};
```

Use the keyspace:

```cql
use logstreamcassandra;
```


Then create the first master table:

```cql
create table masterlogdata(timestamp bigint, visitor text, ip text, message text, statusCode int, loglevel text, PRIMARY KEY(statusCode, loglevel, timestamp));
```
