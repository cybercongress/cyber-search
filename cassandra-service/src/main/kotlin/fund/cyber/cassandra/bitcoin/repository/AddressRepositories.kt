package fund.cyber.cassandra.bitcoin.repository

import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressSummary
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressTx
import org.springframework.data.cassandra.core.cql.QueryOptions
import org.springframework.data.cassandra.core.mapping.MapId
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux



interface BitcoinAddressRepository : ReactiveCrudRepository<CqlBitcoinAddressSummary, String>



interface BitcoinAddressTxRepository : ReactiveCrudRepository<CqlBitcoinAddressTx, MapId> {

    fun findAllByAddress(address: String, options: QueryOptions = QueryOptions.empty()): Flux<CqlBitcoinAddressTx>
}
