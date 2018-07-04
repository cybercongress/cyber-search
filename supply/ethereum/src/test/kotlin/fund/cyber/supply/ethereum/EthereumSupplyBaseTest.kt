package fund.cyber.supply.ethereum

import fund.cyber.common.kafka.BaseKafkaIntegrationTestWithStartedKafka
import fund.cyber.supply.CommonConfiguration
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource

@DirtiesContext
@TestPropertySource(properties = ["CHAIN_FAMILY:ETHEREUM", "GENESIS_SUPPLY:72009990.50"])
@ContextConfiguration(classes = [ApplicationConfiguration::class, CommonConfiguration::class])
abstract class EthereumSupplyBaseTest : BaseKafkaIntegrationTestWithStartedKafka()
