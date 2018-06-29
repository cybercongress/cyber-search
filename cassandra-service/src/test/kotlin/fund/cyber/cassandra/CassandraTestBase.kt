package fund.cyber.cassandra

import com.nhaarman.mockito_kotlin.any
import com.nhaarman.mockito_kotlin.doReturn
import com.nhaarman.mockito_kotlin.mock
import org.apache.http.HttpResponse
import org.apache.http.HttpStatus
import org.apache.http.StatusLine
import org.apache.http.client.HttpClient
import org.apache.http.message.BasicHttpResponse
import org.cassandraunit.spring.EmbeddedCassandra
import org.junit.jupiter.api.Tag
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestExecutionListeners
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import org.springframework.test.context.support.AnnotationConfigContextLoader
import java.lang.annotation.Inherited


@Configuration
@ComponentScan
@ContextConfiguration(loader = AnnotationConfigContextLoader::class)
class ElassandraContext {


    /**
     * Used to skip elastic initial migrations
     */
    @Bean
    @Primary
    fun mockedHttpClient() = mock<HttpClient> {
        on { execute(any()) } doReturn httpResponseOk()
    }

    private fun httpResponseOk(): HttpResponse {
        val statusLineOk = object : StatusLine {
            override fun getStatusCode() = HttpStatus.SC_OK
            override fun getProtocolVersion() = null
            override fun getReasonPhrase() = null
        }
        return BasicHttpResponse(statusLineOk)
    }
}


@SpringJUnitConfig
@Tag("elassandra-integration")
@EmbeddedCassandra(configuration = "cassandra-cs.yaml")
@TestExecutionListeners(
    listeners = [CassandraTestExecutionListener::class],
    mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS
)
@TestPropertySource(properties = ["CASSANDRA_PORT:9142"])
@Inherited
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS)
annotation class CassandraTestBase


@CassandraTestBase
@ContextConfiguration(classes = [ElassandraContext::class])
abstract class BaseCassandraServiceTest
