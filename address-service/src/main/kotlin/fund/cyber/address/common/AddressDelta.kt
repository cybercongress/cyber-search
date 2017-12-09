package fund.cyber.address.common

import fund.cyber.node.common.Chain
import java.math.BigDecimal

data class AddressDelta(
        val address: String,
        val delta: BigDecimal
)


val Chain.addressDeltaTopic: String get() = name + "_ADDRESS_DELTA"