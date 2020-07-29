package util

import java.io.InputStream
import java.util.Properties

object PropertiesUtil {

    def main(args:Array[String]) = {
        val pro: Properties = PropertiesUtil.load("config.properties")
        println(pro.getProperty("kafka.broker.list"))
    }

    def load(path:String) = {
        val fis: InputStream = Thread.currentThread()
                .getContextClassLoader.getResourceAsStream(path)
        val properties: Properties = new Properties()
        properties.load(fis)
        properties
    }

}
