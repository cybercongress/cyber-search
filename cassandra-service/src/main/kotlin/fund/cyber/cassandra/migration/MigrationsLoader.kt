package fund.cyber.cassandra.migration

import fund.cyber.cassandra.migration.configuration.MigrationRepositoryConfiguration
import java.io.File

interface MigrationsLoader {
    fun load(settings: MigrationSettings): List<Migration>
}

private const val CQL_EXTENSION = "cql"
private const val JSON_EXTENSION = "json"

class DefaultMigrationsLoader(
        private val migrationsRootDirectory: String = "migrations"
) : MigrationsLoader {

    override fun load(settings: MigrationSettings): List<Migration> {

        val url = MigrationRepositoryConfiguration::class.java.getResource("/$migrationsRootDirectory/${settings.migrationDirectory}")

        return if (url == null) emptyList() else File(url.path)
                .walk().map { createMigration(it, settings) }.toCollection(mutableListOf())
    }


    private fun createMigration(it: File, migrationSettings: MigrationSettings): Migration =
            when (it.extension) {
                CQL_EXTENSION -> CqlFileBasedMigration(it.nameWithoutExtension,
                        migrationSettings.applicationId, getMigrationPath(migrationSettings, it))
                JSON_EXTENSION -> ElasticHttpMigration(it.nameWithoutExtension,
                        migrationSettings.applicationId, getMigrationPath(migrationSettings, it))
                else -> EmptyMigration()
            }

    private fun getMigrationPath(migrationSettings: MigrationSettings, it: File) =
            "/$migrationsRootDirectory/${migrationSettings.migrationDirectory}/${it.name}"
}