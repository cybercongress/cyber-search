package fund.cyber.search.model.chains

import fund.cyber.search.model.bitcoin.BitcoinBlock
import fund.cyber.search.model.bitcoin.BitcoinTx
import fund.cyber.search.model.ethereum.EthereumBlock
import fund.cyber.search.model.ethereum.EthereumTx
import fund.cyber.search.model.ethereum.EthereumUncle

interface ChainEntity

interface BlockEntity : ChainEntity {
    val number: Long
}

enum class ChainEntityType {
    BLOCK, TX, UNCLE
}

/**
 * Enum of chain families used in application.
 */
enum class ChainFamily(
    /**
     * Default URL of blockchain node to connect for data.
     */
    val defaultNodeUrl: String,
    /**
     * Map of blockchain entities types to their class representation.
     */
    val entityTypes: Map<ChainEntityType, Class<*>>
) {
    BITCOIN(
        defaultNodeUrl = "http://cyber:cyber@127.0.0.1:8332",
        entityTypes = mapOf(
            ChainEntityType.BLOCK to BitcoinBlock::class.java,
            ChainEntityType.TX to BitcoinTx::class.java
        )
    ),
    ETHEREUM(
        defaultNodeUrl = "http://127.0.0.1:8545",
        entityTypes = mapOf(
            ChainEntityType.BLOCK to EthereumBlock::class.java,
            ChainEntityType.TX to EthereumTx::class.java,
            ChainEntityType.UNCLE to EthereumUncle::class.java
        )
    )
}

class ChainInfo(
    val family: ChainFamily,
    val name: String = family.toString(),
    val nodeUrl: String = family.defaultNodeUrl
) {

    val entityTypes get() = family.entityTypes.keys
    val familyNameLowerCase get() = family.toString().toLowerCase()
    val nameLowerCase get() = name.toLowerCase()

    fun entityClassByType(type: ChainEntityType) = family.entityTypes[type]
}
