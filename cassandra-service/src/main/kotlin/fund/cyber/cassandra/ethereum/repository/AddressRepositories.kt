package fund.cyber.cassandra.ethereum.repository

import fund.cyber.cassandra.ethereum.model.CqlEthereumAddress
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressMinedBlock
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressTxPreview
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressUncle
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.repository.reactive.ReactiveCrudRepository

interface EthereumAddressRepository : ReactiveCrudRepository<CqlEthereumAddress, String>

interface EthereumAddressTxRepository : ReactiveCrudRepository<CqlEthereumAddressTxPreview, MapId>

interface EthereumAddressMinedBlockRepository : ReactiveCrudRepository<CqlEthereumAddressMinedBlock, MapId>

interface EthereumAddressUncleRepository : ReactiveCrudRepository<CqlEthereumAddressUncle, MapId>