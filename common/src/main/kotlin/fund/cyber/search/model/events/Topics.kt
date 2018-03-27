package fund.cyber.search.model.events

import fund.cyber.search.model.chains.Chain


val Chain.txPumpTopic: String get() = name + "_TX_PUMP"
val Chain.blockPumpTopic: String get() = name + "_BLOCK_PUMP"
val Chain.unclePumpTopic: String get() = name + "_UNCLE_PUMP"
