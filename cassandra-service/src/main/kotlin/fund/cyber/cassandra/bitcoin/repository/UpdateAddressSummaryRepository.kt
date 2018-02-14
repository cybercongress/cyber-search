package fund.cyber.cassandra.bitcoin.repository

import com.datastax.driver.core.ConsistencyLevel
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinAddressSummary
import org.springframework.data.cassandra.repository.Consistency
import org.springframework.data.cassandra.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono


/**
 * To archive atomic updates of address summaries we should use CAS, two-phase commit, serial reads and quorum writes.
 */
interface BitcoinUpdateAddressSummaryRepository : ReactiveCrudRepository<CqlBitcoinAddressSummary, String> {


    @Consistency(value = ConsistencyLevel.QUORUM)
    fun save(entity: CqlBitcoinAddressSummary): Mono<CqlBitcoinAddressSummary>

    @Consistency(value = ConsistencyLevel.SERIAL)
    override fun findById(id: String): Mono<CqlBitcoinAddressSummary>

    @Consistency(value = ConsistencyLevel.SERIAL)
    override fun findAllById(ids: Iterable<String>): Flux<CqlBitcoinAddressSummary>

    @Consistency(value = ConsistencyLevel.QUORUM)
    @Query("""
        UPDATE address_summary
        SET confirmed_balance = ':#{#summary.confirmed_balance}',
            confirmed_tx_number = :#{#summary.confirmed_tx_number},
            confirmed_total_received = :#{#summary.confirmed_total_received},
            kafka_delta_offset = :#{#summary.kafka_delta_offset},
            kafka_delta_offset_committed = false,
        WHERE id = ':#{#summary.id}'
        IF confirmed_balance = ':oldConfirmedBalance' AND
           kafka_delta_offset < :#{#summary.kafka_delta_offset} AND
           kafka_delta_offset_committed = true
        """)
    fun update(@Param("summary") summary: CqlBitcoinAddressSummary,
               @Param("oldConfirmedBalance") oldBalance: String
    ): Mono<CqlBitcoinAddressSummary>


    @Consistency(value = ConsistencyLevel.QUORUM)
    @Query("""
        UPDATE address_summary
        SET kafka_delta_offset_committed = true
        WHERE id = ':#{#summary.id}'
        IF kafka_delta_offset = :#{#summary.kafka_delta_offset} AND
           kafka_delta_offset_committed = false
        """)
    fun commitUpdate(@Param("summary") summary: CqlBitcoinAddressSummary): Mono<CqlBitcoinAddressSummary>
}