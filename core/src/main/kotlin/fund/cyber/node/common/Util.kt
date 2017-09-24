package fund.cyber.node.common

import java.util.*

fun Deque<String>.intValue(): Int? = first?.toIntOrNull()
fun Deque<String>.longValue(): Long? = first?.toLongOrNull()
fun Deque<String>.stringValue(): String? = first


inline fun <reified T : Any> env(name: String, default: T): T =
        when (T::class) {
            String::class -> (System.getenv(name) ?: default) as T
            Int::class, Int::class.javaPrimitiveType -> (System.getenv(name)?.toIntOrNull() ?: default) as T
            Boolean::class, Boolean::class.javaPrimitiveType -> (System.getenv(name).toBoolean()) as T
            else -> default
        }