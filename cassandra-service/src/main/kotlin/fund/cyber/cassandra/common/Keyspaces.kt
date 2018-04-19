package fund.cyber.cassandra.common

import org.springframework.data.cassandra.core.cql.keyspace.CreateKeyspaceSpecification
import org.springframework.data.cassandra.core.cql.keyspace.DataCenterReplication

private const val RELIABLE_REPLICATION_FACTOR = 3L

fun defaultKeyspaceSpecification(keyspaces: String): CreateKeyspaceSpecification {
    return CreateKeyspaceSpecification.createKeyspace(keyspaces)
            .withNetworkReplication(
                    DataCenterReplication.of("WITHOUT_REPLICATION", 1),
                    DataCenterReplication.of("RELIABLE", RELIABLE_REPLICATION_FACTOR)
            )
            .ifNotExists()
}
