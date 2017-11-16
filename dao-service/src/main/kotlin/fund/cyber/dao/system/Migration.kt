package fund.cyber.dao.system

import com.datastax.driver.core.Session
import java.io.InputStream


class MigrationException(message: String, exception: Throwable) : RuntimeException(message, exception)


interface Migration {

    val version: Int
    val applicationId: String
    val description: String
    fun execute(session: Session)
}

class CqlFileBasedMigration(
        private val cqlFile: InputStream,
        override val version: Int,
        override val applicationId: String,
        override val description: String) : Migration {


    override fun execute(session: Session) {

        cqlFile.bufferedReader().use { it.readText() }
                .split(";")
                .map(String::trim)
                .map { statement -> statement + ";" }
                .forEach { statement -> session.execute(statement) }
    }
}