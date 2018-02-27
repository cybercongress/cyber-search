package fund.cyber.pump.common.genesis

import fund.cyber.pump.common.node.BlockBundle

interface GenesisDataProvider<T: BlockBundle> {
    fun provide(blockBundle: T): T
}
