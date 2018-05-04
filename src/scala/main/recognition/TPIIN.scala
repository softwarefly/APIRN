package recognition

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yzk on 2016/12/12.
 */
object TPIIN {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val savePath = "/tpin/yzk/newExperiment/"
    val hiveTable = "tpin_model_yzk_2_v2"

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
      println("month:"+month)
      // val month = 2
      // 从HDFS获取模式
      val models = sc.objectFile[(Long, ((Int, String, String, Long, Int, Int, Int), Int, (Double, Double, Double)))](savePath+hiveTable).filter(_._2._1._7 == month)
      // 简化后的模式，格式为（模式类型，起点，终点，链长，权重）
      val models_simplified = models.map(x => (x._2._1._3, x._2._2, max(x._2._3), x._2._1._1)).map(x => (x._1.split(";")(x._1.split(";").length - 1).split(","), x._2, x._3, x._4)).map(x => (x._4, x._1(0).toLong, x._1(1).toLong, x._2, x._3))

      // 田老师的TPIIN方法只考虑了关联交易模式
      // 模式1异常标签集
      val models_1 = models_simplified.filter(_._1 == 1)
      val models_1_left = models_1.map(x => (x._2, (x._4, x._5)))
      val models_1_right = models_1.map(x => (x._3, (x._4, x._5)))
      val models_1_ids = (models_1_left ++ models_1_right).reduceByKey((a, b) => {
        (if (a._1 < b._1) a._1 else b._1, if (a._2 < b._2) a._2 else b._2)
      })

      val setLabel = sc.objectFile[(Long,(String,Int,Double))](savePath + "setLabel_V2/month" + month)

      // RDD[(Long, ((String, Int, Double), (String, Int, Double)))]
      val result = (setLabel.join(models_1_ids.map(x => (x._1, ("Black", x._2._1, x._2._2)))) ++ setLabel.map(x => (x._1, (x._2, ("White", 0, 0.0))))).reduceByKey((a, b) => {
        (a._1, if (a._2._1 == "Black") a._2 else if (b._2._1 == "Black") b._2 else ("White", 0, 0.0))
      })
      result.repartition(120).saveAsObjectFile(savePath + "setLabelAddTPIIN_V2/month" + month)
    }

    sc.stop()
  }
}
