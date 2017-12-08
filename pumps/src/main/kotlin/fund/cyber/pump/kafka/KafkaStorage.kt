package fund.cyber.pump.kafka

import fund.cyber.node.common.Chain
import fund.cyber.node.kafka.JsonSerializer
import fund.cyber.pump.*
import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*


class KafkaStorage(
        private val kafkaBrokers: List<String> = PumpsConfiguration.kafkaBrokers
) : StorageInterface {

    private val kafkaProperties = Properties().apply {
        put("bootstrap.servers", kafkaBrokers)
        put("group.id", "pumps")
    }

    private val jsonSerializer = JsonSerializer<Any>()
    private val producer by lazy { KafkaProducer<Any, Any>(kafkaProperties, jsonSerializer, jsonSerializer) }

    private val actionFactories = mutableMapOf<Chain, KafkaStorageActionTemplateFactory<BlockBundle>>()

    override fun initialize(blockchainInterface: BlockchainInterface<*>) {
        //todo create topics manually??
    }

    @Suppress("UNCHECKED_CAST")
    override fun registerStorageActionSourceFactory(chain: Chain, actionSourceFactory: StorageActionSourceFactory) {
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