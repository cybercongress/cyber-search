package fund.cyber.cassandra

import org.apache.cassandra.io.util.FileUtils
import org.cassandraunit.spring.CassandraUnitDependencyInjectionIntegrationTestExecutionListener
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.cassandraunit.utils.EmbeddedCassandraServerHelper.DEFAULT_TMP_DIR
import org.springframework.test.context.TestContext
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Paths

/**
 * This is temp solution.
 * After https://github.com/jsevellec/cassandra-unit/issues/273 resolved, should be closed.
 */
class CassandraTestExecutionListener : CassandraUnitDependencyInjectionIntegrationTestExecutionListener() {

    private var initialized = false

    override fun prepareTestInstance(testContext: TestContext) {
        if (!initialized) {
            initialized = true
            rmdir(DEFAULT_TMP_DIR + "1")
            copy("/cassandra-rackdc_cs.properties", DEFAULT_TMP_DIR + "1")
            val file = File(DEFAULT_TMP_DIR + "1/cassandra-rackdc_cs.properties")
            System.setProperty("cassandra-rackdc.properties", "file:" + file.absolutePath)
            super.beforeTestClass(testContext)
        }
    }

    override fun beforeTestClass(testContext: TestContext) {
        if (!initialized) {
            initialized = true
            rmdir(DEFAULT_TMP_DIR + "1")
            copy("/cassandra-rackdc_cs.properties", DEFAULT_TMP_DIR + "1")
            val file = File(DEFAULT_TMP_DIR + "1/cassandra-rackdc_cs.properties")
            System.setProperty("cassandra-rackdc.properties", "file:" + file.absolutePath)
            super.beforeTestClass(testContext)
        }
    }

    @Throws(IOException::class)
    private fun copy(resource: String, directory: String) {
        mkdir(directory)
        val fileName = resource.substring(resource.lastIndexOf("/") + 1)
        val from = EmbeddedCassandraServerHelper::class.java.getResourceAsStream(resource)
        Files.copy(from, Paths.get(directory + System.getProperty("file.separator") + fileName))
    }

    private fun mkdir(dir: String) {
        FileUtils.createDirectory(dir)
    }

    private fun rmdir(dir: String) {
        val dirFile = File(dir)
        if (dirFile.exists()) {
            FileUtils.deleteRecursive(dirFile)
        }
    }
}