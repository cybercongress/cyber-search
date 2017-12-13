package fund.cyber.address.common

import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity
import java.math.BigDecimal

open class AddressDelta(
        val source: ChainEntity,
        val address: String,
        val blockNumber: Long,
        val balanceDelta: BigDecimal,
        val txNumberDelta: Int
) {

    open fun reverseDelta(): AddressDelta = AddressDelta(
            source = source, address = address, blockNumber = blockNumber,
            balanceDelta = -balanceDelta, txNumberDelta = -txNumberDelta
    )
}


val Chain.addressDeltaTopic: String get() = name + "_ADDRESS_DELTA"