package fund.cyber.cassandra

import fund.cyber.cassandra.bitcoin.repository.BitcoinBlockRepository
import fund.cyber.cassandra.common.searchRepositoryBeanName
import fund.cyber.cassandra.ethereum.repository.EthereumBlockRepository
import org.cassandraunit.spring.CassandraDataSet
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.support.GenericApplicationContext
import org.springframework.test.annotation.DirtiesContext


@DirtiesContext
@CassandraDataSet(value = ["create-chains-keyspaces.cql"])
class AllChainsContextStartUpTest : CassandraTestBase() {

    @Autowired
    private lateinit var applicationContext: GenericApplicationContext

    @Test
    @DisplayName("Should successfully create context for Bitcoin repositories")
    fun shouldCreateContextForBitcoinChain() {

        val btcRepositoryBeanName = BitcoinBlockRepository::class.java.searchRepositoryBeanName("bitcoin_x")
        val bthRepositoryBeanName = BitcoinBlockRepository::class.java.searchRepositoryBeanName("bitcoin_cash")

        val ethRepositoryBeanName = EthereumBlockRepository::class.java.searchRepositoryBeanName("ethereum")
        val etcRepositoryBeanName = EthereumBlockRepository::class.java.searchRepositoryBeanName("ethereum_classic")

        Assertions.assertNotNull(applicationContext.getBean(btcRepositoryBeanName))
        Assertions.assertNotNull(applicationContext.getBean(bthRepositoryBeanName))

        Assertions.assertNotNull(applicationContext.getBean(ethRepositoryBeanName))
        Assertions.assertNotNull(applicationContext.getBean(etcRepositoryBeanName))
    }
} 