package fund.cyber.cassandra.repository

import com.datastax.driver.core.Cluster
import fund.cyber.cassandra.CassandraKeyspaceRepository
import fund.cyber.cassandra.model.keyspace
import fund.cyber.node.common.Chain


class EthereumKeyspaceRepository(
        cassandra: Cluster, chain: Chain
) : CassandraKeyspaceRepository(cassandra, chain.keyspace)