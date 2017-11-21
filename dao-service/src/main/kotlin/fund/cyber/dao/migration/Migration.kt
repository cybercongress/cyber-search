package fund.cyber.dao.migration

import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.Statement

interface Migration {
    val version: Int
    val applicationId: String
}

interface CassandraMigration : Migration {
    fun getStatements(): List<Statement>
}


class CqlFileBasedMigration(
        override val version: Int,
        override val applicationId: String,
        private val filePath: String
) : CassandraMigration {

    override fun getStatements(): List<Statement> {

        return CqlFileBasedMigration::class.java.getResourceAsStream(filePath)
                .bufferedReader().use { it.readText() }
                .split(";").map(String::trim)
                .filter { statement -> statement.isNotEmpty() }
                .map { statement -> statement + ";" }
                .map { statement -> SimpleStatement(statement) }
    }
}


