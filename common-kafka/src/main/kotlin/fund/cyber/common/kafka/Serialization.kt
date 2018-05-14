package fund.cyber.common.kafka

import fund.cyber.search.jsonDeserializer
import fund.cyber.search.jsonSerializer
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.LoggerFactory
import kotlin.jvm.java
import kotlin.text.toByteArray

class JsonSerializer<T> : Serializer<T> {

    private val objectMapper = jsonSerializer

    override fun serialize(topic: String, data: T): ByteArray {
        return objectMapper.writeValueAsString(data).toByteArray()
    }

    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
    override fun close() {}
}

class JsonDeserializer<T>(private val type: Class<T>) : Deserializer<T> {

    private val objectMapper = jsonDeserializer
    private val log = LoggerFactory.getLogger(JsonDeserializer::class.java)

    override fun deserialize(topic: String, data: ByteArray): T {
        log.debug("topic $topic data size : ${data.size}")
        return objectMapper.readValue(data, type)
    }

    override fun configure(configs: MutableMap<String, *>, isKey: Boolean) {}
    override fun close() {}
}
