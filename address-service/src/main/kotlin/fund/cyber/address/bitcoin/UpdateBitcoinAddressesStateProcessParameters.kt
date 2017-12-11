package fund.cyber.address.bitcoin

import fund.cyber.address.common.AddressesUpdateProcessParameters
import fund.cyber.address.common.ConvertEntityToAddressDeltaProcessParameters
import fund.cyber.cassandra.repository.BitcoinKeyspaceRepository
import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity.TRANSACTION
import fund.cyber.node.kafka.entityTopic
import fund.cyber.node.model.BitcoinTransaction


fun getBitcoinAddressesUpdateProcessParameters(
        chain: Chain, keyspaceRepository: BitcoinKeyspaceRepository
): AddressesUpdateProcessParameters {

    val transactionToAddressDeltaProcessesParameters = ConvertEntityToAddressDeltaProcessParameters(
            entityType = BitcoinTransaction::class.java,
            convertEntityToAddressDeltaFunction = BitcoinTransactionToAddressDeltaFunction(),
            inputTopic = chain.entityTopic(TRANSACTION)
    )

    val applyAddressDeltaFunction = ApplyBitcoinAddressDeltaFunction(keyspaceRepository)

    return AddressesUpdateProcessParameters(
            chain = chain, applyAddressDeltaFunction = applyAddressDeltaFunction,
            convertEntityToAddressDeltaProcessesParameters = listOf(transactionToAddressDeltaProcessesParameters)
    )
}