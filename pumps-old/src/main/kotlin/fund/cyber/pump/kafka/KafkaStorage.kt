package fund.cyber.pump.kafka

import fund.cyber.node.common.Chain
import fund.cyber.node.kafka.JsonSerializer
import fund.cyber.pump.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory
import java.util.*

private val log = LoggerFactory.getLogger(KafkaStorage::class.java)!!

class KafkaStorage(
        private val kafkaBrokers: List<String> = PumpsConfiguration.kafkaBrokers,
        private val chainsToPumpAsString: String = PumpsConfiguration.chainsToPump.map(Chain::name).joinToString("_")
) : StorageInterface {

    private val kafkaProperties = Properties().apply {
        put("bootstrap.servers", kafkaBrokers)
        put("group.id", "pumps" + chainsToPumpAsString)
        put("transactional.id", "pumps_" + chainsToPumpAsString)
        put("transaction.timeout.ms", 30 * 1000)
    }

    private val jsonSerializer = JsonSerializer<Any>()
    private val producer by lazy {
        log.info("Initializing kafka storage producer")
        KafkaProducer<Any, Any>(kafkaProperties, jsonSerializer, jsonSerializer).apply {
            initTransactions()
            log.info("Initializing kafka storage producer completed")
        }
    }

    private val actionFactories = mutableMapOf<Chain, KafkaStorageActionTemplateFactory<BlockBundle>>()

    override fun initialize(blockchainInterface: BlockchainInterface<*>) {
        //todo create topics manually??
    }

    @Suppress("UNCHECKED_CAST")
    override fun setStorageActionSourceFactoryFor(chain: Chain, actionSourceFactory: StorageActionSourceFactory) {
        if (actionSourceFactory is KafkaStorageActionTemplateFactory<*>) {
            actionFactories.put(chain, actionSourceFactory as KafkaStorageActionTemplateFactory<BlockBundle>)
        }
    }

    override fun constructAction(blockBundle: BlockBundle): StorageAction {

        val actionTemplate = actionFactories[blockBundle.chain]?.constructActionTemplate(blockBundle)

        if (actionTemplate != null) {
            return KafkaStorageAction(producer, actionTemplate)
        }
        return EmptyStorageAction
    }
}