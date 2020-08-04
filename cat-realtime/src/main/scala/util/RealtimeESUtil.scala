package util

import java.util

import io.searchbox.client.config.HttpClientConfig.Builder
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}


object RealtimeESUtil {

    private var conn:JestClient = null
    private val esServerUri:String = "http://spark105:9200"

    private[util] def getConn() = {
        if(conn == null){
            val factory: JestClientFactory = new JestClientFactory
            factory.setHttpClientConfig(
                new Builder(esServerUri)
                        .maxTotalConnection(20)
                        .multiThreaded(true)
                        .connTimeout(10000)
                        .readTimeout(10000).
                        build())
            conn = factory.getObject
        }
        conn
    }



    def insertBulk(esIndex:String,content:List[(String,Any)])= {

        val client:JestClient = getConn()

        val bulkBuilder: Bulk.Builder = new Bulk.Builder()
                .defaultIndex(esIndex)
                .defaultType("_doc")

        for (elem <- content) {
            val indexBuilder: Index.Builder = new Index.Builder(elem._2)
            if(elem._1 != null)indexBuilder.id(elem._1)
            bulkBuilder.addAction(indexBuilder.build())
        }

        val bulk: Bulk = bulkBuilder.build()

        client.execute(bulk)

    }

}
