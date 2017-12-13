package fund.cyber.address.ethereum

import fund.cyber.address.common.AddressesUpdateProcessParameters
import fund.cyber.address.common.ConvertEntityToAddressDeltaProcessParameters
import fund.cyber.address.common.addressCache
import fund.cyber.cassandra.repository.EthereumKeyspaceRepository
import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity
import fund.cyber.node.kafka.ETHEREUM_ADDRESS_MINED_BLOCKS_TOPIC_PREFIX
import fund.cyber.node.kafka.entityTopic
import fund.cyber.node.model.EthereumAddress
import fund.cyber.node.model.EthereumAddressMinedBlock
import fund.cyber.node.model.EthereumTransaction
import fund.cyber.node.model.EthereumUncle
import org.ehcache.CacheManager


fun getEthereumAddressesUpdateProcessParameters(
        chain: Chain, keyspaceRepository: EthereumKeyspaceRepository, cacheManager: CacheManager
): AddressesUpdateProcessParameters<EthereumAddressDelta> {

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

    val addressCache = cacheManager.getCache(chain.addressCache, String::class.java, EthereumAddress::class.java)
    val applyAddressDeltaFunction = ApplyEthereumAddressDeltaFunction(keyspaceRepository, addressCache)

    return AddressesUpdateProcessParameters(
            chain = chain, addressDeltaClassType = EthereumAddressDelta::class.java,
            applyAddressDeltaFunction = applyAddressDeltaFunction,
            convertEntityToAddressDeltaProcessesParameters = deltasEmitingProcessesParameters
    )
}