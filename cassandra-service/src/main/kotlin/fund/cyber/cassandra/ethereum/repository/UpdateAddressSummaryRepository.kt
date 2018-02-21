package fund.cyber.cassandra.ethereum.repository

import com.datastax.driver.core.ConsistencyLevel
import fund.cyber.cassandra.ethereum.model.CqlEthereumAddressSummary
import org.springframework.data.cassandra.repository.Consistency
import org.springframework.data.cassandra.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono


/**
 * To archive atomic updates of address summaries we should use CAS, two-phase commit, serial reads and quorum writes.
 */
interface EthereumUpdateAddressSummaryRepository : ReactiveCrudRepository<CqlEthereumAddressSummary, String> {

    @Consistency(value = ConsistencyLevel.SERIAL)
    override fun findById(id: String): Mono<CqlEthereumAddressSummary>

    @Consistency(value = ConsistencyLevel.SERIAL)
    fun findAllByIdIn(ids: Iterable<String>): Flux<CqlEthereumAddressSummary>

    /**
     * Return {@code true} if update was successful.
     */
    @Consistency(value = ConsistencyLevel.QUORUM)
    @Query("""
        UPDATE address_summary
        SET confirmed_balance = :#{#summary.confirmed_balance},
            contract_address = :#{#summary.contract_address},
            confirmed_total_received = :#{#summary.confirmed_total_received},
            tx_number = :#{#summary.tx_number},
            uncle_number = :#{#summary.uncle_number},
            mined_block_number = :#{#summary.mined_block_number},
            kafka_delta_offset = :#{#summary.kafka_delta_offset},
            kafka_delta_topic = :#{#summary.kafka_delta_topic},
            kafka_delta_partition = :#{#summary.kafka_delta_partition},
            version = :#{#summary.version},
            kafka_delta_offset_committed = false
        WHERE id = :#{#summary.id}
        IF version = :oldVersion
        """)
    fun update(@Param("summary") summary: CqlEthereumAddressSummary, @Param("oldVersion") oldVersion: Long): Mono<Boolean>

    /**
     * Return {@code true} if there is no record for key and insert was successful.
     */
    @Consistency(value = ConsistencyLevel.QUORUM)
    @Query("""
        INSERT INTO address_summary (id, confirmed_balance, contract_address,
          confirmed_total_received, tx_number, uncle_number, mined_block_number,
          version, kafka_delta_offset, kafka_delta_topic,
          kafka_delta_partition, kafka_delta_offset_committed)
        VALUES (:#{#summary.id}, :#{#summary.confirmed_balance}, :#{#summary.contract_address}, :#{#summary.confirmed_total_received},
            :#{#summary.tx_number},:#{#summary.uncle_number},:#{#summary.mined_block_number},
            :#{#summary.version}, :#{#summary.kafka_delta_offset}, :#{#summary.kafka_delta_topic}, :#{#summary.kafka_delta_partition},
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