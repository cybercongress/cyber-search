package fund.cyber.pump.common.genesis

import fund.cyber.pump.common.BlockBundle

interface GenesisDataProvider<T: BlockBundle> {
    fun provide(blockBundle: T): T
}
