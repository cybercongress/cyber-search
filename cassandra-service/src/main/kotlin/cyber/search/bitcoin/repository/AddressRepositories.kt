package cyber.search.bitcoin.repository

import cyber.search.bitcoin.model.BitcoinAddress
import cyber.search.bitcoin.model.BitcoinAddressTransaction
import org.springframework.data.cassandra.core.cql.QueryOptions
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux



interface BitcoinAddressRepository : ReactiveCrudRepository<BitcoinAddress, String>



interface BitcoinAddressTxRepository : ReactiveCrudRepository<BitcoinAddressTransaction, MapId> {

    fun findAllByAddress(address: String, options: QueryOptions = QueryOptions.empty()): Flux<BitcoinAddressTransaction>
}
