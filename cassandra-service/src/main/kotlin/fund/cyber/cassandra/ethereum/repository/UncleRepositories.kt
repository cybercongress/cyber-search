package fund.cyber.cassandra.ethereum.repository

import fund.cyber.cassandra.ethereum.model.CqlEthereumUncle
import org.springframework.data.repository.reactive.ReactiveCrudRepository

interface EthereumUncleRepository : ReactiveCrudRepository<CqlEthereumUncle, String>
