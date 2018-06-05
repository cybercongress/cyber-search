package fund.cyber.cassandra

import org.cassandraunit.spring.CassandraUnitDependencyInjectionIntegrationTestExecutionListener
import org.cassandraunit.spring.EmbeddedCassandra
import org.junit.jupiter.api.Tag
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestExecutionListeners
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig
import org.springframework.test.context.support.AnnotationConfigContextLoader
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener
import org.springframework.test.context.support.DirtiesContextBeforeModesTestExecutionListener
import org.springframework.test.context.support.DirtiesContextTestExecutionListener


@Configuration
@ComponentScan
@ContextConfiguration(loader = AnnotationConfigContextLoader::class)
class ElassandraContext




@SpringJUnitConfig
@Tag("elassandra-integration")
@ContextConfiguration(classes = [ElassandraContext::class])
@EmbeddedCassandra(configuration = "embedded-cassandra.yaml")
@TestExecutionListeners(listeners = [
    CassandraUnitDependencyInjectionIntegrationTestExecutionListener::class,
    DependencyInjectionTestExecutionListener::class,
    DirtiesContextBeforeModesTestExecutionListener::class,
    DirtiesContextTestExecutionListener::class
])
@TestPropertySource(properties = ["CASSANDRA_PORT:9142"])
abstract class CassandraTestBase
