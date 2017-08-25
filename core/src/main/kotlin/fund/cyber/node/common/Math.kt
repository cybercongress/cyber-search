@file:Suppress("NOTHING_TO_INLINE")

package fund.cyber.node.common

import java.math.BigInteger


inline operator fun BigInteger.plus(increment: Int): BigInteger = this.add(BigInteger.valueOf(increment.toLong()))
