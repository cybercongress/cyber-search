package fund.cyber.pump.common.genesis

import fund.cyber.pump.common.BlockBundle
import fund.cyber.search.model.chains.Chain

interface GenesisBundleProvider<out T: BlockBundle> {
    fun provide(chain: Chain): T
}
