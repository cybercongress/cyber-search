package fund.cyber.pump

import fund.cyber.pump.bitcoin.client.BitcoinBlockBundle
import fund.cyber.pump.common.ChainPump
import fund.cyber.pump.common.FlowableBlockchainInterface
import fund.cyber.pump.common.KafkaBlockBundleProducer
import fund.cyber.pump.common.LastPumpedBundlesProvider
import fund.cyber.search.configuration.CHAIN
import fund.cyber.search.configuration.env
import fund.cyber.search.model.chains.BitcoinFamilyChain
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.SpringApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import javax.annotation.PostConstruct

@ComponentScan(basePackageClasses = [BitcoinPumpApplication::class])
@Configuration
class BitcoinPumpApplication {

    @Autowired
    lateinit var kafkaProducer: KafkaBlockBundleProducer<BitcoinBlockBundle>

    @Autowired
    lateinit var blockchainInterface: FlowableBlockchainInterface<BitcoinBlockBundle>

    @Autowired
    lateinit var stateProvider: LastPumpedBundlesProvider<BitcoinBlockBundle>

    @PostConstruct
    fun startPump() {
        ChainPump(blockchainInterface, kafkaProducer, stateProvider).startPump()
    }

    @Bean
    fun chain(): BitcoinFamilyChain {
        val chainAsString = env(CHAIN, "BITCOIN")
        return BitcoinFamilyChain.valueOf(chainAsString)
    }
}


fun main(args: Array<String>) {

    val application = SpringApplication(BitcoinPumpApplication::class.java)
    application.setRegisterShutdownHook(false)
    application.run(*args)
}