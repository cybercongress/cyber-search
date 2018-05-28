package fund.cyber.common

const val HASH_PREFIX = "0x"

fun String.toSearchHashFormat(): String {
    if (this.startsWith(HASH_PREFIX, true)) {
        return this.substring(HASH_PREFIX.length)
    }
    return this
}

fun String.isEmptyHexValue() = this == "0x"
