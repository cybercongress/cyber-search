package fund.cyber.cassandra.ethereum.repository

import fund.cyber.cassandra.ethereum.model.CqlEthereumBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumBlockTxPreview
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.repository.reactive.ReactiveCrudRepository

interface EthereumBlockRepository : ReactiveCrudRepository<CqlEthereumBlock, Long>

interface EthereumBlockTxRepository : ReactiveCrudRepository<CqlEthereumBlockTxPreview, MapId>