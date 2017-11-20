package fund.cyber.dao.system

import com.datastax.driver.core.Session


class MigrationException(message: String, exception: Throwable) : RuntimeException(message, exception)


interface Migration {

    val version: Int
    val applicationId: String
    fun execute(session: Session)
}

class CqlFileBasedMigration(
        override val applicationId: String,
        override val version: Int,
        private val filePath: String
) : Migration {


    override fun execute(session: Session) {

        CqlFileBasedMigration::class.java.getResourceAsStream(filePath)
                .bufferedReader().use { it.readText() }
                .split(";")
                .map(String::trim)
                .filter { statement -> statement.isNotEmpty() }
                .map { statement -> statement + ";" }
                .forEach { statement -> session.execute(statement) }
    }
}