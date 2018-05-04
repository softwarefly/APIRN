package result

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yzk on 2016/12/12.
 */
object OriginRate_Step2 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val savePath = "/tpin/yzk/newExperiment/"
    val hiveTable = "tpin_model_yzk_v2"

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
      // 从HDFS获取模式
      val models = sc.objectFile[(Long, ((Int, String, String, Long, Int, Int, Int), Int, (Double, Double, Double)))](savePath+hiveTable).filter(_._2._1._7 == month)
      // 简化后的模式，格式为（模式类型，起点，终点，链长，权重）
      val models_simplified = models.map(x => (x._2._1._3, x._2._2, max(x._2._3), x._2._1._1)).map(x => (x._1.split(";")(x._1.split(";").length - 1).split(","), x._2, x._3, x._4)).map(x => (x._4, x._1(0).toLong, x._1(1).toLong, x._2, x._3))

      // 模式1异常标签集
      val models_1 = models_simplified.filter(_._1 == 1)
      val models_1_left = models_1.map(x => (x._2, (x._4, x._5)))
      val models_1_right = models_1.map(x => (x._3, (x._4, x._5)))
      val models_1_ids = (models_1_left ++ models_1_right).reduceByKey((a, b) => {
        (if (a._1 < b._1) a._1 else b._1, if (a._2 < b._2) a._2 else b._2)
      })

      // 模式4异常标签集
      val models_4 = models_simplified.filter(_._1 == 4)
      val models_4_left = models_4.map(x => (x._2, (x._4, x._5)))
      val models_4_right = models_4.map(x => (x._3, (x._4, x._5)))
      val models_4_ids = (models_4_left ++ models_4_right).reduceByKey((a, b) => {
        (if (a._1 < b._1) a._1 else b._1, if (a._2 < b._2) a._2 else b._2)
      })

      // 异常标签集
      val setC_Before = (models_1_ids ++ models_4_ids).reduceByKey((a, b) => {
        (if (a._1 < b._1) a._1 else b._1, if (a._2 < b._2) a._2 else b._2)
      })

      // 从HDFS获取初始识别结果集
      val recognitionResult = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + month)

      val setC = setC_Before.join(recognitionResult)

      val setB = recognitionResult.filter(_._2._1._1 == "Black")

      val B = setB.count().toDouble
      val C = setC.count().toDouble
      val C_and_B = setC.join(setB).count().toDouble

      val recall = C_and_B / B
      val precision = C_and_B / C

      println((B.toLong, C.toLong, C_and_B.toLong, recall, precision))
    }

    sc.stop()
  }
}
