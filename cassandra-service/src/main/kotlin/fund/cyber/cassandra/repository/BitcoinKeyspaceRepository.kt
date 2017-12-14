package fund.cyber.cassandra.repository

import com.datastax.driver.core.Cluster
import com.datastax.driver.mapping.Result
import com.datastax.driver.mapping.annotations.Accessor
import com.datastax.driver.mapping.annotations.Query
import com.google.common.util.concurrent.ListenableFuture
import fund.cyber.cassandra.CassandraKeyspaceRepository
import fund.cyber.cassandra.model.keyspace
import fund.cyber.node.common.Chain
import fund.cyber.node.model.*


@Accessor
interface BitcoinKeyspaceRepositoryAccessor {

    @Query("SELECT * FROM tx_preview_by_address where address=? limit 20")
    fun addressTransactions(address: String): ListenableFuture<Result<BitcoinAddressTransaction>>
}


class BitcoinKeyspaceRepository(
        cassandra: Cluster, chain: Chain
) : CassandraKeyspaceRepository(cassandra, chain.keyspace) {


    val blockStore = mappingManager.mapper(BitcoinBlock::class.java)!!
    val blockTxStore = mappingManager.mapper(BitcoinBlockTransaction::class.java)!!
    val txStore = mappingManager.mapper(BitcoinTransaction::class.java)!!
    val addressStore = mappingManager.mapper(BitcoinAddress::class.java)!!
    val addressTxtore = mappingManager.mapper(BitcoinAddressTransaction::class.java)!!
    val bitcoinKeyspaceRepositoryAccessor = mappingManager.createAccessor(BitcoinKeyspaceRepositoryAccessor::class.java)!!
}