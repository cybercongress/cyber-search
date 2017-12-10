# Run elassandra for dev

### Cassandra tool

* [Download cassandra](http://cassandra.apache.org/download/ )
* [Add cassandra bin to $PATH](https://stackoverflow.com/questions/29944484/how-to-run-cassandra-cqlsh-from-anywhere)


### First run

# Run elassandra and than initialize keyspaces and tables

```bash
cqlsh  -f ./bootstrap.cql
```

### Useful commands

See cluster status
```bash
nodetool status
```

Dive into elassandra docker container shell
```bash
docker exec -it cyber_markets_elassandra_dev bash
```





