package fund.cyber.search.model.events

import fund.cyber.search.model.chains.ChainInfo

val ChainInfo.txPumpTopic: String get() = name + "_TX_PUMP"
val ChainInfo.blockPumpTopic: String get() = name + "_BLOCK_PUMP"
val ChainInfo.unclePumpTopic: String get() = name + "_UNCLE_PUMP"
val ChainInfo.supplyTopic: String get() = name + "_SUPPLY"
