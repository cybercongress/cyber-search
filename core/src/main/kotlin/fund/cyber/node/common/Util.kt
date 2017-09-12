package fund.cyber.node.common

import java.util.*

fun Deque<String>.intValue(): Int? = first?.toIntOrNull()
fun Deque<String>.stringValue(): String? = first
