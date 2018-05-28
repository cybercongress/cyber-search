package fund.cyber.search.model.ethereum

import java.math.BigDecimal


data class EthereumSupply(
    val blockNumber: Long,
    val uncleNumber: Long,
    val totalSupply: BigDecimal,
    val genesisSupply: BigDecimal,
    val miningBlocksSupply: BigDecimal,
    val miningUnclesSupply: BigDecimal,
    val includingUnclesSupply: BigDecimal
)
