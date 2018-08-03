package fund.cyber.supply.bitcoin

import fund.cyber.common.kafka.BaseKafkaIntegrationTestWithStartedKafka
import fund.cyber.supply.CommonConfiguration
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource

@DirtiesContext
@TestPropertySource(properties = ["CHAIN_FAMILY:BITCOIN", "GENESIS_SUPPLY:0"])
@ContextConfiguration(classes = [ApplicationConfiguration::class, CommonConfiguration::class])
abstract class BitcoinSupplyBaseTest : BaseKafkaIntegrationTestWithStartedKafka()