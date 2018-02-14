package fund.cyber.search.model.chains


enum class BitcoinFamilyChain : Chain {
    BITCOIN,
    BITCOIN_CASH;
}


enum class BitcoinFamilyChainEntity : ChainEntity {
    BLOCK,
    TRANSACTION,
    ADDRESS
}