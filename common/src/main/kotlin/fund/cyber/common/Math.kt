@file:Suppress("NOTHING_TO_INLINE")

package fund.cyber.common

import java.math.BigDecimal
import java.math.BigInteger


inline operator fun BigInteger.plus(increment: Int): BigInteger = this.add(BigInteger.valueOf(increment.toLong()))

/**
 * Returns the sum of all values produced by [selector] function applied to each element in the collection.
 */
inline fun <T> Iterable<T>.sumByBigDecimalString(selector: (T) -> String): BigDecimal {
    var sum: BigDecimal = BigDecimal.ZERO
    for (element in this) {
        sum += BigDecimal(selector(element))
    }
    return sum
}

/**
 * Returns the sum of all values produced by [selector] function applied to each element in the collection.
 */
inline fun <T> Iterable<T>.sumByDecimal(selector: (T) -> BigDecimal): BigDecimal {
    var sum: BigDecimal = BigDecimal.ZERO
    for (element in this) {
        sum += selector(element)
    }
    return sum
}


/**
 * Returns the sum of all values in the collection.
 */
inline fun Iterable<BigDecimal>.sum(): BigDecimal {
    var sum: BigDecimal = BigDecimal.ZERO
    for (element in this) {
        sum += element
    }
    return sum
}

inline fun String.hexToLong(): Long = java.lang.Long.decode(this)

val decimal8 = BigDecimal(8)
val decimal32 = BigDecimal(32)

