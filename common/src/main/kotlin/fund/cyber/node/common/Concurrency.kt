package fund.cyber.node.common

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.concurrent.FutureCallback
import org.apache.http.nio.client.HttpAsyncClient
import java.lang.Exception
import java.util.concurrent.CompletableFuture


fun <T> List<ListenableFuture<T>>.awaitAll(): List<T> = Futures.allAsList(this).get()

fun HttpAsyncClient.executeAsync(request: HttpUriRequest): CompletableFuture<HttpResponse> {
    val futureCallback = CompletableFutureCallback<HttpResponse>()
    this.execute(request, futureCallback)
    return futureCallback.completableFuture
}


class CompletableFutureCallback<T> : FutureCallback<T> {

    val completableFuture = CompletableFuture<T>()

    override fun completed(result: T) {
        completableFuture.complete(result)
    }

    override fun failed(ex: Exception?) {
        completableFuture.completeExceptionally(ex)
    }

    override fun cancelled() {
        completableFuture.cancel(true)
    }
}