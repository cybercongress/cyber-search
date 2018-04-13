package fund.cyber.api.common

const val HASH_PREFIX = "0x"

fun String.formatHash(): String {

    var formattedHash: String = this
    if (startsWith(HASH_PREFIX, true).not()) {
        formattedHash = HASH_PREFIX + formattedHash
    }

    return formattedHash.toLowerCase()
}
