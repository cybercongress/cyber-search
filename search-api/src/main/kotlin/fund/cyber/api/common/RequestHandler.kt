package fund.cyber.api.common

import fund.cyber.cassandra.common.REPOSITORY_NAME_DELIMITER
import org.springframework.context.support.GenericApplicationContext
import org.springframework.web.reactive.function.server.HandlerFunction
import org.springframework.web.reactive.function.server.RequestPredicates
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.RouterFunctions
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import reactor.core.publisher.Mono

interface ContextAwareRequestHandler {
    fun toRouterFunction(applicationContext: GenericApplicationContext): RouterFunction<ServerResponse>
}

class SingleRepositoryItemRequestHandler<R>(
    private val path: String,
    private val repositoryClass: Class<R>,
    private val handle: (ServerRequest, R) -> Mono<ServerResponse>
) : ContextAwareRequestHandler {

    override fun toRouterFunction(applicationContext: GenericApplicationContext): RouterFunction<ServerResponse> {
        return applicationContext.getBeanNamesForType(repositoryClass).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMITER)
            val repository = applicationContext.getBean(beanName, repositoryClass)

            RouterFunctions.route(
                RequestPredicates.path("/${chainName.toLowerCase()}$path"),
                HandlerFunction { request -> handle(request, repository) }
            )
        }.asSingleRouterFunction()
    }
}

class BiRepositoryItemRequestHandler<R1, R2>(
    private val path: String,
    private val repositoryClass1: Class<R1>,
    private val repositoryClass2: Class<R2>,
    private val handle: (ServerRequest, R1, R2) -> Mono<ServerResponse>
) : ContextAwareRequestHandler {

    override fun toRouterFunction(applicationContext: GenericApplicationContext): RouterFunction<ServerResponse> {
        return applicationContext.getBeanNamesForType(repositoryClass1).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMITER)
            val repository1 = applicationContext.getBean(beanName, repositoryClass1)
            val repository2 = applicationContext.getSearchRepositoryBean(repositoryClass2, chainName)

            RouterFunctions.route(
                RequestPredicates.path("/${chainName.toLowerCase()}$path"),
                HandlerFunction { request -> handle(request, repository1, repository2) }
            )
        }.asSingleRouterFunction()
    }
}
