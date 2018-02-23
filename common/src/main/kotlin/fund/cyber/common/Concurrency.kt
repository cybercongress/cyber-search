package fund.cyber.common

import java.util.concurrent.CompletableFuture

fun <T> List<CompletableFuture<T>>.await(): List<T> {
    val futures = this.toTypedArray()
    return CompletableFuture.allOf(*futures)
            .thenApply { futures.map { future -> future.get() } }.get()
}