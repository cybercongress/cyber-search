package fund.cyber.pump.common.pool

import fund.cyber.search.model.PoolItem

interface PoolInterface<out T: PoolItem> {
    fun onNewItem(action: (T)->Unit, onError: (Throwable) -> Unit)
}
