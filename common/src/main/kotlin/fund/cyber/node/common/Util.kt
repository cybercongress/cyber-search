package fund.cyber.node.common

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import java.util.*


fun Deque<String>.intValue(): Int? = first?.toIntOrNull()
fun Deque<String>.longValue(): Long? = first?.toLongOrNull()
fun Deque<String>.stringValue(): String? = first


fun <T> List<ListenableFuture<T>>.awaitAll(): List<T> = Futures.allAsList(this).get()