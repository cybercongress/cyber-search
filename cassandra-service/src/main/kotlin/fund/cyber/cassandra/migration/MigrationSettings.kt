package fund.cyber.cassandra.migration

val defaultSettings = MigrationSettings("default", "")

open class MigrationSettings(
        val migrationDirectory: String,
        val applicationId: String
)