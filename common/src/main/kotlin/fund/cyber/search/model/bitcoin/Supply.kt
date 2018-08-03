package fund.cyber.search.model.bitcoin

import java.math.BigDecimal

data class BitcoinSupply(
    val blockNumber: Long,
    val totalSupply: BigDecimal
)
