package fund.cyber.address.ethereum

import fund.cyber.address.common.AddressDelta
import fund.cyber.node.common.ChainEntity
import java.math.BigDecimal


class EthereumAddressDelta(
        source: ChainEntity,
        address: String,
        blockNumber: Long,
        balanceDelta: BigDecimal,
        txNumberDelta: Int = 0,
        val uncleNumberDelta: Int = 0,
        val minedBlockNumberDelta: Int = 0
) : AddressDelta(source, address, blockNumber, balanceDelta, txNumberDelta) {


    override fun reverseDelta(): EthereumAddressDelta = EthereumAddressDelta(
            source = source, address = address, blockNumber = blockNumber,
            balanceDelta = -balanceDelta, txNumberDelta = -txNumberDelta, uncleNumberDelta = -uncleNumberDelta,
            minedBlockNumberDelta = -minedBlockNumberDelta
    )
}