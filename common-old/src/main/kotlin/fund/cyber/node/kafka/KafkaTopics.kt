package fund.cyber.node.kafka

import fund.cyber.node.common.Chain
import fund.cyber.node.common.ChainEntity


fun Chain.entityTopic(entity: ChainEntity): String = name + "_" + entity.name

const val ETHEREUM_ADDRESS_MINED_BLOCKS_TOPIC_PREFIX = "_ADDRESS_MINED_BLOCK"