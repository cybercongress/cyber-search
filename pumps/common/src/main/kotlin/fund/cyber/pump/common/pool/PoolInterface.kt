package fund.cyber.pump.common.pool

import fund.cyber.search.model.PoolItem

// todo: this interface should have one method that returns observable/flowable
interface PoolInterface<out T: PoolItem> {
    fun onNewItem(action: (T)->Unit, onError: (Throwable) -> Unit)
}
