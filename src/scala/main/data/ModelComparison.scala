package data

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 模式结果数量对比：限制投资比例20%和不限制
 * Created by yzk on 2016/12/10.
 */
object ModelComparison {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    for (i <- 2 to 12) {
      //      val i = 2
      println(hiveContext.sql("SELECT count(*) FROM tpin_model_yzk where type = 1 and month = " + i).first())
      println(hiveContext.sql("SELECT count(*) FROM tpin_model_yzk where type = 4 and month = " + i).first())

      println(hiveContext.sql("SELECT count(*) FROM tpin_model_yzk_v2 where type = 1 and month = " + i).first())
      println(hiveContext.sql("SELECT count(*) FROM tpin_model_yzk_v2 where type = 4 and month = " + i).first())
    }

    for (i <- 2 to 12) {
      //      val i = 2
      println(hiveContext.sql("SELECT count(*) FROM tpin_model_yzk_2 where type = 1 and month = " + i).first())
      println(hiveContext.sql("SELECT count(*) FROM tpin_model_yzk_2 where type = 4 and month = " + i).first())

      println(hiveContext.sql("SELECT count(*) FROM tpin_model_yzk_2_v2 where type = 1 and month = " + i).first())
      println(hiveContext.sql("SELECT count(*) FROM tpin_model_yzk_2_v2 where type = 4 and month = " + i).first())
    }

    sc.stop()
  }
}
