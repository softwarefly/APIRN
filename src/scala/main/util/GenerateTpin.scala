package util

import java.text.SimpleDateFormat
import java.util.Date

import entity.Pattern
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by david on 6/28/16.
  **/
object GenerateTpin {

    def generate(sc: SparkContext, hiveContext: HiveContext): Unit = {

        val format = new SimpleDateFormat("hh:mm:ss.SSS")
        // 构建TPIN
        // ***需要足够大的内存persist中间RDD以保证zipWithIndex函数不会被重复调用（来保证结果正确性）
        // ***特别注意每次构建后的编号会发生变化
        // ***建议生成后保存到HDFS再进行后续操作

//        val before_construct = new Date()
//        val tpin = InputOutputTools.getFromMySQL(hiveContext).persist()
//
//        println("after construct:  \n" + tpin.vertices.count)
//        println(tpin.edges.count)
//        val after_add_cid = new Date()
//        val tpinWithCommunityId = CommunityDiscovery.Use_connectedComponents(tpin).persist()
//        println("after add cid:\n " + tpinWithCommunityId.vertices.count)
//        println(tpinWithCommunityId.edges.count)
//        //  保存TPIN到HDFS
//        InputOutputTools.saveAsObjectFile(tpinWithCommunityId, sc)

        // 从HDFS获取TPIN
        val tpin1 = InputOutputTools.getFromHDFS_Object(sc).persist()
        //    val after_persist=new Date();
        println("read from object file after construct:  \n" + tpin1.vertices.count)
        println(tpin1.edges.count)
        // 构建带社团编号的TPINo

        // 构建关联交易模式（单向环和双向环）
        val pattern1 = PatternMatching.Match_Pattern_1(sc, tpin1).persist()

        val add_strict_IL = AddNewAttr.addStrictIL(tpin1, 0.2).persist()
        val pattern2 = PatternMatching.Match_Pattern_2(sc, add_strict_IL, 0.0).persist()
        val pattern3 = PatternMatching.Match_Pattern_3(sc, add_strict_IL).persist()

        val add_loose_IL = AddNewAttr.Add_Loose_IL(sc, tpin1).persist()
        val pattern4 = PatternMatching.Match_Pattern_4(sc, add_loose_IL).persist()
        val patterns = pattern1.union(pattern2).union(pattern3).union(pattern4)
        val after_findPattern = new Date()
        // 保存带社团编号的TPIN到Hive
        InputOutputTools.saveAsHiveTable(hiveContext, add_strict_IL)
        //annotation of david:输出社团到hive中
        CommunityDiscovery.ConcludeCommunityAndOutput(add_strict_IL, patterns, hiveContext)
        // 保存关联交易模式（单向环和双向环）到Hive
        Pattern.saveAsHiveTable(hiveContext, patterns)
        val after_output = new Date()
        //    System.out.println("\r" + format.format(before_construct));
        //    System.out.println("\r" + format.format(after_add_cid));
        //    System.out.println("\r" + format.format(after_persist));
        //    System.out.println("\r" + format.format(after_findPattern));
        //    System.out.println("\r" + format.format(after_output));
        //    System.out.println("初始建图用时："+(after_add_cid.getTime()-before_construct.getTime())/(1000D)+"秒");
        //    System.out.println("划分社团用时："+(after_persist.getTime()-after_add_cid.getTime())/(1000D)+"秒");
        //    System.out.println("匹配模式用时："+(after_findPattern.getTime()-after_persist.getTime())/(1000D)+"秒");
        //    System.out.println("输出结果用时："+(after_output.getTime()-after_findPattern.getTime())/(1000D)+"秒");
        System.out.println("点个数：" + tpin1.vertices.count)
        System.out.println("边个数：" + tpin1.edges.count)
        //    System.out.println("模式个数："+patterns.count)
        //    hiveContext.sql("SELECT * FROM tpin.tpin_pattern_wwd").rdd.take(10)
        sc.stop()
    }

    def generateNew(sc: SparkContext, hiveContext: HiveContext, weight_il: Double = 0.2): Unit = {

        val format = new SimpleDateFormat("hh:mm:ss.SSS")
        // 构建TPIN
        // ***需要足够大的内存persist中间RDD以保证zipWithIndex函数不会被重复调用（来保证结果正确性）
        // ***特别注意每次构建后的编号会发生变化

        val before_construct = new Date()
        val tpin = InputOutputTools.getFromMySQL(hiveContext).persist()

        println("after construct:  \n" + tpin.vertices.count)
        println(tpin.edges.count)

        val after_add_cid = new Date()
        val tpinWithCommunityId = CommunityDiscovery.Use_connectedComponents(tpin).persist()

        println("after add cid:\n " + tpinWithCommunityId.vertices.count)
        println(tpinWithCommunityId.edges.count)

        InputOutputTools.saveAsObjectFile(tpinWithCommunityId,sc)
        println("having output to the hdsf object")

        val tpin1 = InputOutputTools.getFromHDFS_Object(sc).persist()
        // 从HDFS获取TPIN
        val tpinHaveIL = AddNewAttr.addStrictIL(tpin1,weight_il).persist()

        InputOutputTools.saveAsHiveTable(hiveContext, tpinHaveIL)

        val after_output = new Date();
        //    System.out.println("\r" + format.format(before_construct));
        //    System.out.println("\r" + format.format(after_add_cid));
        //    System.out.println("\r" + format.format(after_persist));
        //    System.out.println("\r" + format.format(after_findPattern));
        //    System.out.println("\r" + format.format(after_output));
            System.out.println("初始建图用时："+(after_add_cid.getTime()-before_construct.getTime())/(1000D)+"秒");
            System.out.println("划分社团识别互锁用时："+(after_output.getTime()-after_add_cid.getTime())/(1000D)+"秒");
//            System.out.println("匹配模式用时："+(after_findPattern.getTime()-after_persist.getTime())/(1000D)+"秒");
        //    System.out.println("输出结果用时："+(after_output.getTime()-after_findPattern.getTime())/(1000D)+"秒");
        System.out.println("点个数：" + tpinHaveIL.vertices.count)
        System.out.println("边个数：" + tpinHaveIL.edges.count)
    }

}
