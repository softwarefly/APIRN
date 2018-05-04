package newExperiment1

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yzk on 2016/12/16.
 */
object CombineLabel {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val savePath = "/tpin/yzk/newExperiment/"

    // 从HDFS获取初始识别结果集
    val result2 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 2)
    val result3 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 3)
    val result4 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 4)
    val result5 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 5)
    val result6 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 6)
    val result7 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 7)
    val result8 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 8)
    val result9 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 9)
    val result10 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 10)
    val result11 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 11)
    val result12 = sc.objectFile[(Long, ((String, Int, Double), (String, Int, Double), (String, Int, Double)))](savePath + "NewLabelSet2_V2/month" + 12)

    val result = result2 ++ result3 ++ result4 ++ result5 ++ result6 ++ result7 ++ result8 ++ result9 ++ result10 ++ result11 ++ result12

    result.saveAsObjectFile(savePath + "NewLabelSet2_V2/AllYear")

  }
}
