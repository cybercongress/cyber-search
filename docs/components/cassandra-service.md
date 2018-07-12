# Cassandra Service

## Usage of cassandra-service Module

Сassandra service module could be used in two ways:

1. With specifying `CHAIN_FAMILY` environment variable
2. Without specifying `CHAIN_FAMILY` environment variable

### Specifying `CHAIN_FAMILY`

1. Only to interact with specific keyspace (bitcoin, ethereum). F.e. in dumps, contract-summary
2. To run migrations on keyspace
3. Logic in abstract `CassandraRepositoriesConfiguration`. See examples of implementations

### Without `CHAIN_FAMILY`

1. No migrations
2. Initializing repositories for all available keyspaces and put them in spring context.
3. Logic in `SearchRepositoriesConfiguration`