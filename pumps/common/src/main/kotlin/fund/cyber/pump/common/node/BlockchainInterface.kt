package fund.cyber.pump.common.node

import fund.cyber.search.model.chains.ChainEntity
import fund.cyber.search.model.chains.ChainEntityType


interface BlockBundle {
    val hash: String
    val parentHash: String
    val number: Long
    val blockSize: Int

    fun entitiesByType(chainEntityType: ChainEntityType): List<ChainEntity>
}

interface BlockchainInterface<out T : BlockBundle> {
    fun lastNetworkBlock(): Long
    fun blockBundleByNumber(number: Long): T
}
