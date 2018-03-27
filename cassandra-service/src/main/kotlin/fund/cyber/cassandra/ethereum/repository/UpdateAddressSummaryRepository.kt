package fund.cyber.cassandra.ethereum.repository

import com.datastax.driver.core.ConsistencyLevel
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressSummary
import org.springframework.data.cassandra.repository.Consistency
import org.springframework.data.cassandra.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono


//todo write small tests for query consistency with object fields.
/**
 * To archive atomic updates of address summaries we should use CAS, two-phase commit, serial reads and quorum writes.
 */
interface EthereumUpdateAddressSummaryRepository : ReactiveCrudRepository<CqlEthereumAddressSummary, String> {

    @Consistency(value = ConsistencyLevel.QUORUM)
    override fun findById(id: String): Mono<CqlEthereumAddressSummary>

    @Consistency(value = ConsistencyLevel.QUORUM)
    fun findAllByIdIn(ids: Iterable<String>): Flux<CqlEthereumAddressSummary>

    /**
     * Return {@code true} if update was successful.
     */
    @Consistency(value = ConsistencyLevel.QUORUM)
    @Query("""
        UPDATE address_summary
        SET confirmed_balance = :#{#summary.confirmedBalance},
            contract_address = :#{#summary.contractAddress},
            confirmed_total_received = :#{#summary.confirmedTotalReceived},
            tx_number = :#{#summary.txNumber},
            uncle_number = :#{#summary.minedUncleNumber},
            mined_block_number = :#{#summary.minedBlockNumber},
            kafka_delta_offset = :#{#summary.kafkaDeltaOffset},
            kafka_delta_topic = :#{#summary.kafkaDeltaTopic},
            kafka_delta_partition = :#{#summary.kafkaDeltaPartition},
            version = :#{#summary.version},
            kafka_delta_offset_committed = false
        WHERE id = :#{#summary.id}
        IF version = :oldVersion
        """)
    fun update(@Param("summary") summary: CqlEthereumAddressSummary,
               @Param("oldVersion") oldVersion: Long): Mono<Boolean>

    /**
     * Return {@code true} if there is no record for key and insert was successful.
     */
    @Consistency(value = ConsistencyLevel.QUORUM)
    @Query("""
        INSERT INTO address_summary (id, confirmed_balance, contract_address,
          confirmed_total_received, tx_number, uncle_number, mined_block_number,
          version, kafka_delta_offset, kafka_delta_topic,
          kafka_delta_partition, kafka_delta_offset_committed)
        VALUES (:#{#summary.id}, :#{#summary.confirmedBalance}, :#{#summary.contractAddress},
            :#{#summary.confirmedTotalReceived}, :#{#summary.txNumber}, :#{#summary.minedUncleNumber},
            :#{#summary.minedBlockNumber}, :#{#summary.version}, :#{#summary.kafkaDeltaOffset},
            :#{#summary.kafkaDeltaTopic}, :#{#summary.kafkaDeltaPartition},
            false)
        IF NOT EXISTS
        """)
    fun insertIfNotRecord(@Param("summary") summary: CqlEthereumAddressSummary): Mono<Boolean>

    @Consistency(value = ConsistencyLevel.QUORUM)
    @Query("""
        UPDATE address_summary
        SET kafka_delta_offset_committed = true,
            version = :newVersion
        WHERE id = :address
        """)
    fun commitUpdate(@Param("address") address: String, @Param("newVersion") newVersion: Long): Mono<Boolean>
}
