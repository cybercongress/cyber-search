package fund.cyber.pump.common.node

import fund.cyber.search.model.chains.ChainEntity
import fund.cyber.search.model.chains.ChainEntityType

/**
 * Blockchain block with all dependent entities.
 */
interface BlockBundle {
    /**
     * Hash of the block
     */
    val hash: String
    /**
     * Hash of the parent block
     */
    val parentHash: String
    /**
     * Number of the block in blockchain
     */
    val number: Long
    /**
     * Size of the block in bytes
     */
    val blockSize: Int

    /**
     * Get dependent entity values list by entity type.
     *
     * @param chainEntityType type of entity (for example: [ChainEntityType.TX])
     * @return list of entity values (for example: transactions)
     */
    fun entitiesByType(chainEntityType: ChainEntityType): List<ChainEntity>
}

/**
 * Interface representing blockchain
 *
 * @param T block bundle of this blockchain
 */
interface BlockchainInterface<out T : BlockBundle> {
    /**
     * Get last number of the block in blockchain network.
     *
     * @return block number
     */
    fun lastNetworkBlock(): Long

    /**
     * Get [BlockBundle] by block number.
     *
     * @param number block number
     * @return block bundle
     */
    fun blockBundleByNumber(number: Long): T
}
