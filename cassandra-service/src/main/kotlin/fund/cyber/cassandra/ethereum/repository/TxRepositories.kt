package fund.cyber.cassandra.ethereum.repository

import fund.cyber.cassandra.common.RoutingReactiveCassandraRepository
import fund.cyber.cassandra.ethereum.model.CqlEthereumTx

interface EthereumTxRepository : RoutingReactiveCassandraRepository<CqlEthereumTx, String>
