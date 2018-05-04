package util

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by yzk on 2018/5/3.
  */
object MySQLUtil {
    // 分片数（=对Oracle的连接数）
    val partition = 24

    val url = "XXXXXXXXXXXXXXXXXXX"
    val user = "XX"
    val password = "XX"
    val driver = "com.mysql.jdbc.Driver"

    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("driver", driver)

    val db = Map(
        "url" -> url,
        "user" -> user,
        "password" -> password,
        "driver" -> driver)

    /**
      * 获取某个表的全部信息
      *
      * @param sqlContext
      * @param table 表名
      * @return
      */
    def getTable(sqlContext: SQLContext, table: String): DataFrame = {
        sqlContext.read.format("jdbc").options(db + (("dbtable", table))).load()
    }
}
