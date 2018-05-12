package fund.cyber.search.model.ethereum

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonDeserializer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ObjectNode
import fund.cyber.search.model.ethereum.OperationType.CALL
import fund.cyber.search.model.ethereum.OperationType.CREATE_CONTRACT
import fund.cyber.search.model.ethereum.OperationType.DESTROY_CONTRACT
import fund.cyber.search.model.ethereum.OperationType.REWARD



data class OperationTrace(
    @JsonDeserialize(using = OperationsDeserializer::class)
    val operation: Operation,
    val result: OperationResult,
    val subtraces: List<OperationTrace>
)

class OperationsDeserializer : JsonDeserializer<Operation>() {

    override fun deserialize(jsonParser: JsonParser, context: DeserializationContext): Operation {
        val objectMapper = jsonParser.codec as ObjectMapper
        val root = objectMapper.readTree<ObjectNode>(jsonParser)
        val operationType = OperationType.valueOf(root["operationType"].textValue())

        return when (operationType) {
            CALL -> objectMapper.convertValue(root, CallOperation::class.java)
            CREATE_CONTRACT -> objectMapper.convertValue(root, CreateContractOperation::class.java)
            DESTROY_CONTRACT -> objectMapper.convertValue(root, DestroyContractOperation::class.java)
            REWARD -> objectMapper.convertValue(root, RewardOperation::class.java)
        }
    }
}
