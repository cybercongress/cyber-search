package fund.cyber.node.helpers

import kotlinx.coroutines.experimental.delay
import okhttp3.Call
import okhttp3.Callback
import okhttp3.Response
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.TimeUnit
import kotlin.coroutines.experimental.Continuation
import kotlin.coroutines.experimental.suspendCoroutine

private object Helpers {
    val LOGGER = LoggerFactory.getLogger(Helpers::class.java)!!
}

suspend fun <R> retryUntilSuccess(retryDelay: Long = 5, block: suspend () -> R): R {
    while (true) {
        try {
            return block()
        } catch (e: Exception) {
            Helpers.LOGGER.debug("Error during trying execute block", e)
            delay(retryDelay, TimeUnit.SECONDS)
        }
    }
}

inline fun <reified T : Any> env(name: String, default: T): T =
        when (T::class) {
            String::class -> (System.getenv(name) ?: default) as T
            Int::class, Int::class.javaPrimitiveType -> (System.getenv(name)?.toIntOrNull() ?: default) as T
            Boolean::class, Boolean::class.javaPrimitiveType -> (System.getenv(name).toBoolean()) as T
            else -> default
        }

suspend fun Call.await(): Response {

    return suspendCoroutine { cont: Continuation<Response> ->
        val callback = object : Callback {
            override fun onFailure(call: Call?, e: IOException) {
                cont.resumeWithException(e)
            }

            override fun onResponse(call: Call?, response: Response) {
                cont.resume(response)
            }
        }
        enqueue(callback)
    }
}


inline fun hex(ob: Int) = "0x" + Integer.toHexString(ob)
