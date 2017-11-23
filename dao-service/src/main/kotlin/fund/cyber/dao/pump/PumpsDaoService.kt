package fund.cyber.dao.pump

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.mapping.MappingManager
import fund.cyber.node.model.IndexingProgress

class PumpsDaoService(cassandra: Cluster) {

    private val session: Session = cassandra.connect("pump")

    private val mappingManager = MappingManager(session)
    val indexingProgressStore = mappingManager.mapper(IndexingProgress::class.java)!!
}