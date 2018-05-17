package fund.cyber.search.model.chains

interface Chain {
    val name: String
    val lowerCaseName: String get() = name.toLowerCase()
}
