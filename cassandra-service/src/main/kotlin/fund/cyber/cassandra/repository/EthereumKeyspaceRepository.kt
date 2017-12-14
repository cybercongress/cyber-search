package fund.cyber.cassandra.repository

import com.datastax.driver.core.Cluster
import com.datastax.driver.mapping.Result
import com.datastax.driver.mapping.annotations.Accessor
import com.datastax.driver.mapping.annotations.Query
import com.google.common.util.concurrent.ListenableFuture
import fund.cyber.cassandra.CassandraKeyspaceRepository
import fund.cyber.cassandra.model.keyspace
import fund.cyber.node.common.Chain
import fund.cyber.node.model.EthereumAddress
import fund.cyber.node.model.EthereumAddressTxPreview


@Accessor
interface EthereumKeyspaceRepositoryAccessor {

    @Query("SELECT * FROM tx_preview_by_address where address=? limit 20")
    fun addressTransactions(address: String): ListenableFuture<Result<EthereumAddressTxPreview>>
}

class EthereumKeyspaceRepository(
        cassandra: Cluster, chain: Chain
) : CassandraKeyspaceRepository(cassandra, chain.keyspace) {

    val addressStore = mappingManager.mapper(EthereumAddress::class.java)!!
    val ethereumKeyspaceRepositoryAccessor = mappingManager.createAccessor(EthereumKeyspaceRepositoryAccessor::class.java)!!
}