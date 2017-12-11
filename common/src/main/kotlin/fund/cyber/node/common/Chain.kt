package fund.cyber.node.common

enum class Chain {
    BITCOIN,
    BITCOIN_CASH,
    ETHEREUM,
    ETHEREUM_CLASSIC
}

enum class ChainEntity {

    //common
    BLOCK,
    TRANSACTION,
    ADDRESS,

    //ethereum
    UNCLE
}