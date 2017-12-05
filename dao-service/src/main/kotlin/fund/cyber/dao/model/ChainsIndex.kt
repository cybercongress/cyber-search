package fund.cyber.dao.model

import fund.cyber.node.common.Chain
import fund.cyber.node.common.Chain.*
import fund.cyber.node.common.ChainEntity
import fund.cyber.node.common.ChainEntity.*


object ChainsIndex {

    val INDEX_TO_CHAIN_ENTITY: Map<String, Pair<Chain, ChainEntity>> = hashMapOf(

            "bitcoin_block" to Pair(BITCOIN, BLOCK),
            "bitcoin_tx" to Pair(BITCOIN, TRANSACTION),
            "bitcoin_address" to Pair(BITCOIN, BLOCK),

            "bitcoin_cash_block" to Pair(BITCOIN_CASH, BLOCK),
            "bitcoin_cash_tx" to Pair(BITCOIN_CASH, TRANSACTION),
            "bitcoin_cash_address" to Pair(BITCOIN_CASH, BLOCK),

            "ethereum_block" to Pair(ETHEREUM, BLOCK),
            "ethereum_tx" to Pair(ETHEREUM, TRANSACTION),
            "ethereum_address" to Pair(ETHEREUM, BLOCK),

            "ethereum_classic_block" to Pair(ETHEREUM_CLASSIC, BLOCK),
            "ethereum_classic_tx" to Pair(ETHEREUM_CLASSIC, TRANSACTION),
            "ethereum_classic_address" to Pair(ETHEREUM_CLASSIC, BLOCK)
    )
}
