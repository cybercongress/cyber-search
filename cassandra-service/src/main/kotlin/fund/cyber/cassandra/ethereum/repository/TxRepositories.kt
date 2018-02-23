package fund.cyber.cassandra.ethereum.repository

import fund.cyber.cassandra.ethereum.model.CqlEthereumTx
import org.springframework.data.repository.reactive.ReactiveCrudRepository

interface EthereumTxRepository : ReactiveCrudRepository<CqlEthereumTx, String>