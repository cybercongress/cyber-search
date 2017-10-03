@file:Suppress("NOTHING_TO_INLINE")

package fund.cyber.node.common

import java.math.BigDecimal
import java.math.BigInteger


inline operator fun BigInteger.plus(increment: Int): BigInteger = this.add(BigInteger.valueOf(increment.toLong()))

/**
 * Returns the sum of all values produced by [selector] function applied to each element in the collection.
 */
inline fun <T> Iterable<T>.sumByBigDecimal(selector: (T) -> String): BigDecimal {
    var sum: BigDecimal = BigDecimal.ZERO
    for (element in this) {
        sum += BigDecimal(selector(element))
    }
    return sum
}

inline fun String.hexToLong() : Long = java.lang.Long.decode(this)

