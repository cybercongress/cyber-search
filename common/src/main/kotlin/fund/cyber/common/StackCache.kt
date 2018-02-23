package fund.cyber.common

/**
 * Stack with limited size, which loses old elements
 *
 * @author Ioda Mikhail
 */
class StackCache<T>(
        maxSize: Int
) {
    private val max = maxSize + 1
    @Suppress("UNCHECKED_CAST")
    private val _elements = arrayOfNulls<Any>(max) as Array<T>
    private var first = 0
    private var last = 0

    fun push(element: T) {
        _elements[first++] = element
        if (first == max) first = 0
        if (first == last) last = (last + 1) % max
    }

    fun pop(): T? {
        return if (first == last) {
            null
        } else {
            if (first == 0)
                first = max
            _elements[--first]
        }
    }
}