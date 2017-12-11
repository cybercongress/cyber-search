package fund.cyber.address.ethereum

import fund.cyber.address.common.AddressesUpdateProcessParameters
import fund.cyber.address.common.ConvertEntityToAddressDeltaProcessParameters
import fund.cyber.cassandra.repository.EthereumKeyspaceRepository
import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity
import fund.cyber.node.kafka.ETHEREUM_ADDRESS_MINED_BLOCKS_TOPIC_PREFIX
import fund.cyber.node.kafka.entityTopic
import fund.cyber.node.model.EthereumAddressMinedBlock
import fund.cyber.node.model.EthereumTransaction
import fund.cyber.node.model.EthereumUncle


fun getEthereumAddressesUpdateProcessParameters(
        chain: Chain, keyspaceRepository: EthereumKeyspaceRepository
): AddressesUpdateProcessParameters {

    val transactionToAddressDeltaProcessesParameters = ConvertEntityToAddressDeltaProcessParameters(
            entityType = EthereumTransaction::class.java,
            convertEntityToAddressDeltaFunction = EthereumTransactionToAddressDeltaFunction(),
            inputTopic = chain.entityTopic(ChainEntity.TRANSACTION)
    )

    val uncleToAddressDeltaProcessesParameters = ConvertEntityToAddressDeltaProcessParameters(
            entityType = EthereumUncle::class.java,
            convertEntityToAddressDeltaFunction = EthereumUncleToAddressDeltaFunction(),
            inputTopic = chain.entityTopic(ChainEntity.UNCLE)
    )

    val minedBlockToAddressDeltaProcessesParameters = ConvertEntityToAddressDeltaProcessParameters(
            entityType = EthereumAddressMinedBlock::class.java,
            convertEntityToAddressDeltaFunction = EthereumMinedBlockToAddressDeltaFunction(),
            inputTopic = chain.name + ETHEREUM_ADDRESS_MINED_BLOCKS_TOPIC_PREFIX
    )

    val deltasEmitingProcessesParameters = listOf(
            transactionToAddressDeltaProcessesParameters,
            uncleToAddressDeltaProcessesParameters,
            minedBlockToAddressDeltaProcessesParameters
    )

    val applyAddressDeltaFunction = ApplyEthereumAddressDeltaFunction(keyspaceRepository)

    return AddressesUpdateProcessParameters(
            chain = chain, applyAddressDeltaFunction = applyAddressDeltaFunction,
            convertEntityToAddressDeltaProcessesParameters = deltasEmitingProcessesParameters
    )
}