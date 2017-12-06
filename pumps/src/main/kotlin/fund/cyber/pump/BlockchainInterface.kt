package fund.cyber.pump

import fund.cyber.node.common.Chain


interface Blockchain {
    val chain: Chain
}

interface BlockchainInterface<out T : BlockBundle> : Blockchain {
    fun lastNetworkBlock(): Long
    fun blockBundleByNumber(number: Long): T
}

interface BlockBundle {
    val chain: Chain
    val hash: String
    val parentHash: String
    val number: Long
}