package fund.cyber.cassandra.repository

import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import fund.cyber.node.model.CyberSearchItem
import org.ehcache.Cache
import org.ehcache.CacheManager
import org.ehcache.config.CacheConfiguration


@Suppress("UNCHECKED_CAST")
class ChainRepositoryWithCache<in T : CyberSearchItem>(
        private val chainRepository: ChainRepository<T>,
        private val cacheManager: CacheManager,
        private val cachesConfigurations: Map<Class<out T>, CacheConfiguration<String, out T>>
) : ChainRepository<T> by chainRepository {


    override fun save(entity: T) {
        chainRepository.save(entity)
        val cache = getEntityCache(entity::class.java) as Cache<String, T>?
        cache?.put("a", entity)
    }


    override fun saveAsync(entity: T): ListenableFuture<Void> {

        return chainRepository.saveAsync(entity).runWhenReady {
            val cache = getEntityCache(entity::class.java) as Cache<String, T>?
            cache?.put("a", entity)
        }
    }


    override fun <R : T> get(vararg arguments: Any, entityType: Class<R>): R {

        //using cache
        val cache = getEntityCache(entityType)
        if (cache != null) {
            val result = cache.get("a")
            if (result != null) return result
        }

        //query
        val result = chainRepository.get(arguments = *arrayOf(arguments), entityType = entityType)
        cache?.put("a", result)

        return result
    }


    override fun <R : T> getAsync(vararg arguments: Any, entityType: Class<R>): ListenableFuture<R> {

        val cache = getEntityCache(entityType)
        if (cache != null) {
            val result = cache.get("a")
            if (result != null) return Futures.immediateFuture(result)
        }

        return chainRepository.getAsync(arguments = *arrayOf(arguments), entityType = entityType)
                .runWhenReady { entity: R -> cache?.put("a", entity) }
    }


    private fun <R : T> getEntityCache(type: Class<R>): Cache<String, R>? {
        val cache = cacheManager.getCache(type.name, String::class.java, type)
        if (cache == null) {
            val cacheConfiguration = cachesConfigurations[type] as CacheConfiguration<String, R>?
            if (cacheConfiguration != null) {
                return cacheManager.createCache(type.name, cacheConfiguration)
            }
        }
        return cache
    }

    private fun <F> ListenableFuture<F>.runWhenReady(runnable: (F) -> Unit): ListenableFuture<F> {
        return Futures.transformAsync(this) { input ->
            if (input != null) runnable(input)
            this
        }
    }
}
