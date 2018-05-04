package result

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * 查全率查准率统计
 * Created by yzk on 2016/12/12.
 */
object OriginRate_new {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val savePath = "/tpin/yzk/newExperiment/"

    def comuteRecallAndPrecision(recognitionResult: RDD[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))]): ((Int, Int, Int, Int, Int, Int), (Double, Double, Double, Double)) = {
      val blackLabel = recognitionResult.filter(x => x._2._1._1 == "Black").count().toDouble
      val blackLabelWhite = recognitionResult.filter(x => x._2._1._1 == "White").count().toDouble
      val blackTpiin = recognitionResult.filter(x => x._2._2._1 == "Black").count().toDouble
      val blackApirn = recognitionResult.filter(x => x._2._3._1 == "Black").count().toDouble

      val blackLabelAndTpiin = recognitionResult.filter(x => x._2._1._1 == "Black" && x._2._2._1 == "Black").count().toDouble
      val blackLabelAndApirn = recognitionResult.filter(x => x._2._1._1 == "Black" && x._2._3._1 == "Black").count().toDouble

      val recallTpiin = (blackLabelAndTpiin / blackLabel)
      val recallApirn = (blackLabelAndApirn / blackLabel)

      val precisionTpiin = (blackLabelAndTpiin / blackTpiin)
      val precisionApirn = (blackLabelAndApirn / blackApirn)

      //      println((blackLabel.toInt, blackTpiin.toInt, blackApirn.toInt, blackLabelAndTpiin.toInt, blackLabelAndApirn.toInt),(recallTpiin, recallApirn, precisionTpiin, precisionApirn))

      ((blackLabel.toInt, blackLabelWhite.toInt, blackTpiin.toInt, blackApirn.toInt, blackLabelAndTpiin.toInt, blackLabelAndApirn.toInt), (recallTpiin, recallApirn, precisionTpiin, precisionApirn))
    }

    for (month <- 2 to 12) {
      // val month = 2
      // 从HDFS获取初始识别结果集
      val recognitionResult = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + month)

      println(comuteRecallAndPrecision(recognitionResult))
    }

    sc.stop()
  }
}
