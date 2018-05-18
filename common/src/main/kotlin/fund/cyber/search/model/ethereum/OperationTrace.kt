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
import java.lang.RuntimeException


data class OperationTrace(
    @JsonDeserialize(using = OperationsDeserializer::class)
    val operation: Operation,
    @JsonDeserialize(using = OperationResultDeserializer::class)
    val result: OperationResult?, //null for reward and destroy contract operations
    val subtraces: List<OperationTrace> = emptyList()
) {

    /**
     * Do not returns child contracts.
     */
    fun contractsUsedInCurrentOp(): List<String> {

        return when (operation) {
            is CallOperation -> listOf(operation.from, operation.to)
            is RewardOperation -> listOf(operation.address)
            is DestroyContractOperation -> listOf(operation.contractToDestroy, operation.refundContract)
            is CreateContractOperation -> {
                if (result is CreateContractOperationResult) listOf(operation.from, result.address)
                else listOf(operation.from)
            }
        }
    }
}


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


class OperationResultDeserializer : JsonDeserializer<OperationResult?>() {

    override fun deserialize(jsonParser: JsonParser, context: DeserializationContext): OperationResult? {

        val objectMapper = jsonParser.codec as ObjectMapper
        val root = objectMapper.readTree<ObjectNode>(jsonParser)
        if (root.isNull) return null

        return when {
            root.has("output") -> objectMapper.convertValue(root, CallOperationResult::class.java)
            root.has("code") -> objectMapper.convertValue(root, CreateContractOperationResult::class.java)
            root.has("error") -> objectMapper.convertValue(root, ErroredOperationResult::class.java)
            else -> throw RuntimeException("Unsupported Result")
        }
    }
}
