package recognition

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * 为识别结果添加税负和行业预警值属性
 * Created by yzk on 2016/12/13.
 */
object AddSfHyyjz {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val savePath = "/tpin/yzk/newExperiment/"
    val hiveTable = "tpin_vertex_yzk"

    /**
     * 获取控制、投资权重中的最大值
     * @param weight
     * @return
     */
    def max(weight: (Double, Double, Double)): Double = {
      if (weight._1 == 1.0) return weight._1
      else return weight._2
    }

    for (month <- 2 to 12) {
      // val month = 2
      // 从数据库中获取某月份的纳税人点集，以二月份为例
      val vertices_old = hiveContext.sql("select id, ychz, sf, hybh,  hyyjz from tpin_vertex_yzk where type % 2 = 1 and month = " + month).rdd.repartition(120).map(row => (row.getAs[Long]("id"), (row.getAs[Int]("ychz"), row.getAs[Double]("sf"), row.getAs[String]("hybh"), row.getAs[Double]("hyyjz"))))
      //.reduceByKey((a, b) => {
      //        (if (a._1 > b._1) a._1 else b._1, a._2, if (a._3 < b._3) a._3 else b._3)
      //      }).map(x => (x._1, x._2._1, x._2._2, x._2._3))
      println(vertices_old.filter(_._2._4 < 0.0001).count())

    }

    sc.stop()
  }
}
