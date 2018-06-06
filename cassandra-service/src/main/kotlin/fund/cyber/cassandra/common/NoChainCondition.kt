package fund.cyber.cassandra.common

import fund.cyber.search.configuration.CHAIN_FAMILY
import org.springframework.context.annotation.Condition
import org.springframework.context.annotation.ConditionContext
import org.springframework.core.type.AnnotatedTypeMetadata

class NoChainCondition : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {
        return context.environment.getProperty(CHAIN_FAMILY) == null
    }
}
