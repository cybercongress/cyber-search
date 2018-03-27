package fund.cyber.cassandra.ethereum.repository

import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository

interface EthereumTxRepository : ReactiveCassandraRepository<CqlEthereumTx, String>
