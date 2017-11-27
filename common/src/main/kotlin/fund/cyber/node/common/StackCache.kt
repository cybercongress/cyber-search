package fund.cyber.node.common

/**
 * Stack with limited size, which loses old elements
 *
 * @author Ioda Mikhail
 */
class StackCache<T>(
        private val maxSize: Int
) {
    private val _elements = arrayOfNulls<Any>(maxSize) as Array<T>
    private var first = 0
    private var last = 0

    fun push(element: T) {
        _elements[first++] = element
        if (first == maxSize) first = 0
        if (first == last) ++last
    }

    fun pop(): T? {
        return if (first == last) {
            null
        } else {
            if (first > 0)
                first = maxSize
            _elements[--first]
        }
    }
}