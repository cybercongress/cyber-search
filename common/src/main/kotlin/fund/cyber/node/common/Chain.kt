package fund.cyber.node.common

enum class Chain {
    BITCOIN,
    BITCOIN_CASH,
    ETHEREUM,
    ETHEREUM_CLASSIC;

    fun lowercaseName() = this.toString().toLowerCase()
}

enum class ChainEntity {
    BLOCK,
    TRANSACTION,
    ADDRESS
}