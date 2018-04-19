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

    @Consistency(value = ConsistencyLevel.LOCAL_QUORUM)
    override fun findById(id: String): Mono<CqlBitcoinAddressSummary>

    @Consistency(value = ConsistencyLevel.LOCAL_QUORUM)
    fun findAllByIdIn(ids: Iterable<String>): Flux<CqlBitcoinAddressSummary>

    /**
     * Return {@code true} if update was successful.
     */
    @Consistency(value = ConsistencyLevel.LOCAL_QUORUM)
    @Query("""
        UPDATE address_summary
        SET confirmed_balance = :#{#summary.confirmedBalance},
            confirmed_tx_number = :#{#summary.confirmedTxNumber},
            confirmed_total_received = :#{#summary.confirmedTotalReceived},
            last_activity_date = :#{#summary.lastActivityDate},
            kafka_delta_offset = :#{#summary.kafkaDeltaOffset},
            kafka_delta_topic = :#{#summary.kafkaDeltaTopic},
            kafka_delta_partition = :#{#summary.kafkaDeltaPartition},
            version = :#{#summary.version},
            kafka_delta_offset_committed = false
        WHERE id = :#{#summary.id}
        IF version = :oldVersion
        """)
    fun update(@Param("summary") summary: CqlBitcoinAddressSummary,
               @Param("oldVersion") oldVersion: Long): Mono<Boolean>

    /**
     * Return {@code true} if there is no record for key and insert was successful.
     */
    @Consistency(value = ConsistencyLevel.LOCAL_QUORUM)
    @Query("""
        INSERT INTO address_summary (id, confirmed_balance, confirmed_tx_number,
          confirmed_total_received, first_activity_date, last_activity_date, version, kafka_delta_offset,
          kafka_delta_topic, kafka_delta_partition, kafka_delta_offset_committed)
        VALUES (
            :#{#summary.id}, :#{#summary.confirmedBalance}, :#{#summary.confirmedTxNumber},
            :#{#summary.confirmedTotalReceived}, :#{#summary.firstActivityDate}, :#{#summary.lastActivityDate},
            :#{#summary.version}, :#{#summary.kafkaDeltaOffset},
            :#{#summary.kafkaDeltaTopic}, :#{#summary.kafkaDeltaPartition}, false
            )
        IF NOT EXISTS
        """)
    fun insertIfNotRecord(@Param("summary") summary: CqlBitcoinAddressSummary): Mono<Boolean>

    @Consistency(value = ConsistencyLevel.LOCAL_QUORUM)
    @Query("""
        UPDATE address_summary
        SET kafka_delta_offset_committed = true,
            version = :newVersion
        WHERE id = :address
        """)
    fun commitUpdate(@Param("address") address: String, @Param("newVersion") newVersion: Long): Mono<Boolean>
}
