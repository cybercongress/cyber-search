package fund.cyber.search.model.ethereum

import fund.cyber.search.model.chains.ChainEntity
import java.math.BigDecimal
import java.time.Instant


data class EthereumUncle(
        val hash: String,
        val position: Int,
        val number: Long,
        val timestamp: Instant,
        val blockNumber: Long,
        val blockTime: Instant,
        val blockHash: String,
        val miner: String,
        val uncleReward: BigDecimal
) : ChainEntity
