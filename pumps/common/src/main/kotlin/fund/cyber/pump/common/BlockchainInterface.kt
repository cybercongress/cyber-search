package fund.cyber.pump.common


const val UNKNOWN_PARENT_HASH = "UNKNOWN_PARENT_HASH"

interface BlockBundle {
    val hash: String
    val parentHash: String
    val number: Long
    val blockSize: Int
}

interface BlockchainInterface<out T : BlockBundle> {
    fun lastNetworkBlock(): Long
    fun blockBundleByNumber(number: Long): T
}