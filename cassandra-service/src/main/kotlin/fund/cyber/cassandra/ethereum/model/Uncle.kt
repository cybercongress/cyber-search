@file:Suppress("MemberVisibilityCanBePrivate")

package fund.cyber.cassandra.ethereum.model

import fund.cyber.search.model.ethereum.EthereumUncle
import org.springframework.data.cassandra.core.mapping.Column
import org.springframework.data.cassandra.core.mapping.PrimaryKey
import org.springframework.data.cassandra.core.mapping.Table
import java.time.Instant


@Table("uncle")
data class CqlEthereumUncle(
        @PrimaryKey val hash: String,
        val position: Int,
        val number: Long,
        val timestamp: Instant,
        @Column("block_number") val blockNumber: Long,
        @Column("block_time") val blockTime: Instant,
        @Column("block_hash") val blockHash: String,
        val miner: String,
        @Column("uncle_reward") val uncleReward: String
) : CqlEthereumItem {

    constructor(uncle: EthereumUncle) : this(
            hash = uncle.hash, position = uncle.position, number = uncle.number, timestamp = uncle.timestamp,
            blockNumber = uncle.blockNumber, blockTime = uncle.blockTime, blockHash = uncle.blockHash,
            miner = uncle.miner, uncleReward = uncle.uncleReward.toString()

    )
}

