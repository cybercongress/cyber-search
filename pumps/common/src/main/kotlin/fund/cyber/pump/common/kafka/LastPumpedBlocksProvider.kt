package fund.cyber.pump.common.kafka

import fund.cyber.pump.common.node.BlockBundle
import fund.cyber.search.model.events.PumpEvent

interface LastPumpedBundlesProvider<out Bundle : BlockBundle> {
    fun getLastBlockBundles(): List<Pair<PumpEvent, Bundle>>
}