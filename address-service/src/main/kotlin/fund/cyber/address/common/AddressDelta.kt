package fund.cyber.address.common

import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity
import java.math.BigDecimal

data class AddressDelta(
        val address: String,
        val delta: BigDecimal,
        val blockNumber: Long,
        val source: ChainEntity
)


val Chain.addressDeltaTopic: String get() = name + "_ADDRESS_DELTA"