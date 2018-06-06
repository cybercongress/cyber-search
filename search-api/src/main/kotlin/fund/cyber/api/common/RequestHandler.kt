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


class RepositoryItemRequestHandler<T>(
    private val path: String,
    private val repositoryClass: Class<T>,
    private val handle: (ServerRequest, T, String) -> Mono<ServerResponse>
) {

    fun toRouterFunction(applicationContext: GenericApplicationContext): RouterFunction<ServerResponse> {
        return applicationContext.getBeanNamesForType(repositoryClass).map { beanName ->
            val chainName = beanName.substringBefore(REPOSITORY_NAME_DELIMITER)
            val repository = applicationContext.getBean(beanName, repositoryClass)

            RouterFunctions.route(
                RequestPredicates.path("/${chainName.toLowerCase()}$path"),
                HandlerFunction { request -> handle(request, repository, chainName) }
            )
        }.asSingleRouterFunction()
    }
}