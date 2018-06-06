package fund.cyber.cassandra.bitcoin.repository

import com.datastax.driver.core.ConsistencyLevel
import fund.cyber.cassandra.bitcoin.model.CqlBitcoinContractSummary
import fund.cyber.cassandra.common.RoutingReactiveCassandraRepository
import org.springframework.data.cassandra.repository.Consistency
import org.springframework.data.cassandra.repository.Query
import org.springframework.data.repository.query.Param
import reactor.core.publisher.Mono


/**
 * To archive atomic updates of contract summaries we should use CAS, two-phase commit, serial reads and quorum writes.
 */
interface BitcoinUpdateContractSummaryRepository
    : RoutingReactiveCassandraRepository<CqlBitcoinContractSummary, String> {

    @Consistency(value = ConsistencyLevel.LOCAL_QUORUM)
    fun findByHash(hash: String): Mono<CqlBitcoinContractSummary>

    /**
     * Return {@code true} if update was successful.
     */
    @Consistency(value = ConsistencyLevel.LOCAL_QUORUM)
    @Query("""
        UPDATE contract_summary
        SET confirmed_balance = :#{#summary.confirmedBalance},
            confirmed_tx_number = :#{#summary.confirmedTxNumber},
            confirmed_total_received = :#{#summary.confirmedTotalReceived},
            last_activity_date = :#{#summary.lastActivityDate},
            kafka_delta_offset = :#{#summary.kafkaDeltaOffset},
            kafka_delta_topic = :#{#summary.kafkaDeltaTopic},
            kafka_delta_partition = :#{#summary.kafkaDeltaPartition},
            version = :#{#summary.version},
            kafka_delta_offset_committed = false
        WHERE hash = :#{#summary.hash}
        IF version = :oldVersion
        """)
    fun update(@Param("summary") summary: CqlBitcoinContractSummary,
               @Param("oldVersion") oldVersion: Long): Mono<Boolean>

    /**
     * Return {@code true} if there is no record for key and insert was successful.
     */
    @Consistency(value = ConsistencyLevel.LOCAL_QUORUM)
    @Query("""
        INSERT INTO contract_summary (hash, confirmed_balance, confirmed_tx_number,
          confirmed_total_received, first_activity_date, last_activity_date, version, kafka_delta_offset,
          kafka_delta_topic, kafka_delta_partition, kafka_delta_offset_committed)
        VALUES (
            :#{#summary.hash}, :#{#summary.confirmedBalance}, :#{#summary.confirmedTxNumber},
            :#{#summary.confirmedTotalReceived}, :#{#summary.firstActivityDate}, :#{#summary.lastActivityDate},
            :#{#summary.version}, :#{#summary.kafkaDeltaOffset},
            :#{#summary.kafkaDeltaTopic}, :#{#summary.kafkaDeltaPartition}, false
            )
        IF NOT EXISTS
        """)
    fun insertIfNotRecord(@Param("summary") summary: CqlBitcoinContractSummary): Mono<Boolean>

    @Consistency(value = ConsistencyLevel.LOCAL_QUORUM)
    @Query("""
        UPDATE contract_summary
        SET kafka_delta_offset_committed = true,
            version = :newVersion
        WHERE hash = :contract
        """)
    fun commitUpdate(@Param("contract") contract: String, @Param("newVersion") newVersion: Long): Mono<Boolean>
}
