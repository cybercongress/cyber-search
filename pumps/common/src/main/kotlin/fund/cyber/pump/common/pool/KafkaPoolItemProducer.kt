package fund.cyber.pump.common.pool

import fund.cyber.search.model.PoolItem
import fund.cyber.search.model.events.PumpEvent

interface KafkaPoolItemProducer<in T : PoolItem> {

    fun storeItems(itemsEvents: List<Pair<PumpEvent, T>>)
}
