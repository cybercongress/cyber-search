package fund.cyber.supply.ethereum

import fund.cyber.common.kafka.BaseKafkaIntegrationTestWithStartedKafka
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.TestPropertySource


@TestPropertySource(properties = ["CHAIN_FAMILY:ETHEREUM", "GENESIS_SUPPLY:72009990.50"])
@ContextConfiguration(classes = [ApplicationConfiguration::class])
abstract class EthereumSupplyBaseTest : BaseKafkaIntegrationTestWithStartedKafka()
