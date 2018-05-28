package fund.cyber.pump.common.pool

import fund.cyber.search.model.PoolItem
import io.reactivex.Flowable

interface PoolInterface<T: PoolItem> {
    fun subscribePool(): Flowable<T>
}
