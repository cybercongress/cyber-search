package fund.cyber.cassandra.ethereum.repository

import fund.cyber.cassandra.ethereum.model.CqlEthereumTransaction
import org.springframework.data.repository.reactive.ReactiveCrudRepository

interface EthereumTxRepository : ReactiveCrudRepository<CqlEthereumTransaction, String>