package fund.cyber.pump.common

import cyber.search.model.events.PumpEvent

interface LastPumpedBundlesProvider<out Bundle : BlockBundle> {
    fun getLastBlockBundles(): List<Pair<PumpEvent, Bundle>>
}