package fund.cyber.node.common

import fund.cyber.common.StackCache
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Test for [StackCache]
 *
 * @author Ioda Mikhail
 */
class StackCacheTest {

    @Test
    fun `right sequence`() {
        val stack = StackCache<Int>(3)

        (-2..5).forEach { stack.push(it) }

        (5 downTo 3).forEach { assertEquals(it, stack.pop()) }

        assertEquals(null, stack.pop())
        assertEquals(null, stack.pop())

        stack.push(6)

        assertEquals(6, stack.pop())
        assertEquals(null, stack.pop())
    }

    @Test
    fun `extreme size 1`() {
        val stack = StackCache<Int>(1)

        (1..4).forEach { stack.push(it) }

        assertEquals(4, stack.pop())
        assertEquals(null, stack.pop())
        assertEquals(null, stack.pop())
        assertEquals(null, stack.pop())
        assertEquals(null, stack.pop())

        stack.push(5)
        assertEquals(5, stack.pop())
        assertEquals(null, stack.pop())
    }

    @Test
    fun peekTest() {
        val stack = StackCache<Int>(3)

        (2..5).forEach { stack.push(it) }

        assertEquals(5, stack.peek())
        assertEquals(5, stack.peek())

        stack.pop()

        assertEquals(4, stack.peek())
        assertEquals(4, stack.peek())

        stack.pop()

        assertEquals(3, stack.peek())
        assertEquals(3, stack.peek())

        stack.pop()

        assertEquals(null, stack.pop())
        assertEquals(null, stack.pop())

        stack.push(6)

        assertEquals(6, stack.peek())
        assertEquals(6, stack.pop())
    }
}
