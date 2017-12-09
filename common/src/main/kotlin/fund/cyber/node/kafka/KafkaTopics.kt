package fund.cyber.node.kafka

import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity


fun chainEntityKafkaTopic(chain: Chain, entity: ChainEntity): String = chain.name + "_" + entity.name

const val BITCOIN_TX_TOPIC = "bitcoin_tx"
const val BITCOIN_CASH_TX_TOPIC = "bitcoin_cash_tx"