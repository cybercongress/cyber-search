package cyber.search.model.events

import cyber.search.model.chains.Chain


val Chain.txPumpTopic: String get() = name + "_TX_PUMP"
val Chain.blockPumpTopic: String get() = name + "_BLOCK_PUMP"