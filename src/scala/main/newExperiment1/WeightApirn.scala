package newExperiment1

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yzk on 2016/12/16.
 */
object WeightApirn {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val savePath = "/tpin/yzk/newExperiment/"

    for (weight <- 0 to 100) {

      // Apirn
      var B_Count = 0L
      var C_Count = 0L
      var C_and_B_Count = 0L

      // 标签集合数量总计
      var W_Count = 0L
      // MEAN ERROR数量总计
      var FP_Count = 0L

      // 正常标签集合数量总计
      var L_Count = 0L
      // MEAN ERROR数量总计
      var ME_Count = 0L

      for (month <- 2 to 12) {
        // val month = 2
        // 从HDFS获取初始识别结果集
        val recognitionResult = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + month)

        val setB = recognitionResult.filter(_._2._1._1 == "Black")
        val setC = recognitionResult.filter(_._2._3._1 == "Black").filter(_._2._3._3 <= weight.toDouble / 100.0)

        val B = setB.count()
        val C = setC.count()
        val C_and_B = setC.join(setB).count()

        B_Count = B_Count + B
        C_Count = C_Count + C
        C_and_B_Count = C_and_B_Count + C_and_B

        // 误判的结果
        val setW = recognitionResult.filter(_._2._1._1 == "White")
        val setFP = recognitionResult.filter(x => x._2._1._1 == "White" && x._2._3._1 == "Black").filter(_._2._3._3 <= weight.toDouble / 100.0)

        val W = setW.count()
        val FP = setFP.count()

        W_Count = W_Count + W
        FP_Count = FP_Count + FP

        // 平均误差的结果
        val setME = recognitionResult.filter(x => x._2._1._1 != x._2._3._1).filter(_._2._3._3 <= weight.toDouble / 100.0)

        val L = recognitionResult.count()
        val ME = setME.count()

        L_Count = L_Count + L
        ME_Count = ME_Count + ME
      }

      val recall = C_and_B_Count.toDouble / B_Count.toDouble
      val precision = C_and_B_Count.toDouble / C_Count.toDouble
      val fp_rate = FP_Count.toDouble / W_Count.toDouble
      val mean_error = ME_Count.toDouble / L_Count.toDouble

      println((weight.toDouble / 100.0, B_Count, C_Count, C_and_B_Count, recall, precision, 2 * recall * precision / (recall + precision), W_Count, FP_Count, fp_rate, L_Count, ME_Count, mean_error))
    }

    sc.stop()
  }

}
