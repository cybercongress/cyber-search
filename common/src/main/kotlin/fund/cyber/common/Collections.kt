package fund.cyber.common


fun <K, V> MutableMap<K, V>.with(vararg pairs: Pair<K, V>): MutableMap<K, V> {
    putAll(pairs)
    return this
}
