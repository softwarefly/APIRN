package util

import java.util.Arrays

import entity.{EdgeAttr, Pattern, VertexAttr}
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.LinkedList

/**
  * Created by david on 6/29/16.
  */
object PatternMatching {

  //annotation of david:构建IL开头，第二条边不经过交叉前件区域，所有投资边投资比例超过0.2的路径集合
  def getPathsForPattern4(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr]):RDD[Seq[VertexId]] = {
    // 发送路径
    def sendPaths(edge: EdgeContext[Seq[Seq[VertexId]], Boolean, Seq[Seq[VertexId]]]): Unit = {
      // 过滤掉仅含交易权重的边
      if (edge.attr) {
        // 顶点路径集合中的每条路径追加终点编号
        val paths = edge.srcAttr.map(_ ++ Seq(edge.dstId))
        // 向终点发送顶点路径集合
        edge.sendToDst(paths)
      }
    }
    // 去除环
    def removeCircle(paths: Seq[Seq[VertexId]], length: Int): Seq[Seq[VertexId]] = {
      // 过滤掉路径中包含相同顶点的路径
      paths.filter(path => path.size == (length+1) && path.indexOf(path(length)) == length)
    }
    // 路径长度
    var length = 2
    // 初始顶点路径集合中仅有一条包含自身顶点编号的路径
    val Intial_Graph = tpinWithCommunityId.mapVertices((id, vd) => Seq[Seq[VertexId]](Seq[VertexId](id)))
    //      .mapEdges((edge)=>edge.attr.isAntecedent())

    //annotation of david:路径的第一条边一定是IL
    val IL_Paths = Intial_Graph.subgraph(epred = e => e.attr.is_IL).aggregateMessages[Seq[Seq[VertexId]]](
      ctx => ctx.sendToDst(ctx.srcAttr.map(_ ++ Seq(ctx.dstId))),_ ++ _)
    // 每个顶点路径集合添加到全局路径集合（paths）
    var paths = IL_Paths.flatMap(_._2).collect
    // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
    // Graph[Seq[scala.Seq[graphx.VertexId]], Boolean] 点的属性为IL边，边的属性为是否是前件路径
    var subgraph = Intial_Graph.outerJoinVertices(IL_Paths) { case (vid, oldattr, option) =>
      option match {
        case Some(list) => list.filter(_.size==2)
        case None => oldattr.filter(_.size==2)
      }
    }.mapEdges((edge)=>edge.attr.isImportantAntecedent())
    //    TestMain.printGraph(subgraph)
    val Messages = subgraph.subgraph(epred = e => e.attr).aggregateMessages[LinkedList[Long]](
      ctx => ctx.sendToSrc(LinkedList(ctx.dstId)),_ ++ _)
    //        Messages.foreach(println)
    val KnowCompany=subgraph.outerJoinVertices(Messages) { case (vid, oldattr, option) =>
      option match {
        case Some(list) =>
          (oldattr, list)
        case None => (oldattr, LinkedList[Long]())
      }
    }
    //annotation of david:点必须是IL边的端点，边必须是前件边
    val Messages_off_direction = KnowCompany.subgraph(epred = e => e.attr).aggregateMessages[LinkedList[(Long,LinkedList[Long])]](
      ctx => {
        if(ctx.srcAttr._1.size>0)
          ctx.sendToDst(LinkedList((ctx.srcId, ctx.srcAttr._2)))},_ ++ _)
    //        Messages_off_direction.foreach(println)
    val StopNodes = Messages_off_direction.flatMap{case (dstid,list)=> FindFrequenceContent(list)}
      .reduceByKey((a,b)=>a).groupBy(_._1._2)
    //        StopNodes.foreach(println)
    val KnowStopNodes = subgraph.outerJoinVertices(StopNodes){case (vid, path_ILs, option) =>
      option match {
        case Some(stopNodes) =>
          val keys=path_ILs.map(seq=>(seq(0),seq(1)))
          stopNodes.filter(entity=>keys.contains(entity._1))
        case None =>  Iterable[((Long, Long), LinkedList[Long])]()
      }
    }
    //    TestMain.printGraph(KnowStopNodes)
    val long2Paths = KnowStopNodes.aggregateMessages[Seq[Seq[VertexId]]]((ctx=>{
      if (ctx.attr) {
        for(((node1,node2),stopNodes) <- ctx.srcAttr){
          if(!stopNodes.contains(ctx.dstId)){
            ctx.sendToDst(Seq(Seq[VertexId](node1,node2,ctx.dstId)))
          }
        }
      }
    }), _ ++ _).mapValues(removeCircle(_, length)).filter(_._2.size > 0)

    paths =  paths ++long2Paths.flatMap(_._2).collect

    val vertexId = long2Paths.sortByKey().map(_._1).collect

    subgraph = subgraph.subgraph(vpred = (id, vd) => Arrays.binarySearch(vertexId, id) >= 0).joinVertices[Seq[Seq[VertexId]]](long2Paths)((id, vd, path) => path)

    while (subgraph.numVertices > 0) {
      // 路径长度+1
      length = length + 1
      // 每个顶点把自身变化后的顶点路径集合沿边发送到邻接顶点
      val vertices = subgraph.aggregateMessages[Seq[Seq[VertexId]]](sendPaths(_), _ ++ _).mapValues(removeCircle(_, length)).filter(_._2.size > 0)
      // 每个顶点路径集合添加到全局路径集合（paths）
      paths = paths ++ vertices.flatMap(_._2).collect
      // 接收到消息的顶点的顶点编号，排序以便二分查找
      val vertexId = vertices.sortByKey().map(_._1).collect
      // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
      subgraph = subgraph.subgraph(vpred = (id, vd) => Arrays.binarySearch(vertexId, id) >= 0).joinVertices[Seq[Seq[VertexId]]](vertices)((id, vd, path) => path)
    }
    // 返回全局路径集合
    sparkContext.parallelize(paths)
  }

  def FindFrequenceContent(d: LinkedList[(Long,LinkedList[Long])]): Seq[((Long,Long),LinkedList[Long])] = {
    val Frequencies = d
    val result =
      for (i <- 0 until Frequencies.length) yield
        for (j <- i + 1 until Frequencies.length) yield {
          val (vid1, list1) = Frequencies(i)
          val (vid2, list2) = Frequencies(j)
          val intersectList=list1.intersect(list2)
          if (intersectList.size >= 1)
            Option(Iterable(((vid1, vid2),intersectList)
              ,((vid2, vid1),intersectList)))
          else
            Option.empty
        }
    result.flatten.filter(!_.isEmpty).map(_.get).flatten
  }
  //annotation of david:构建IL开头，第二条边不经过交叉前件区域的路径集合
  def getPathsForPattern3(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr]):RDD[Seq[VertexId]] = {
    // 发送路径
    def sendPaths(edge: EdgeContext[Seq[Seq[VertexId]], Boolean, Seq[Seq[VertexId]]]): Unit = {
      // 过滤掉仅含交易权重的边
      if (edge.attr) {
        // 顶点路径集合中的每条路径追加终点编号
        val paths = edge.srcAttr.map(_ ++ Seq(edge.dstId))
        // 向终点发送顶点路径集合
        edge.sendToDst(paths)
      }
    }
    // 去除环
    def removeCircle(paths: Seq[Seq[VertexId]], length: Int): Seq[Seq[VertexId]] = {
      // 过滤掉路径中包含相同顶点的路径
      paths.filter(path => path.size == (length+1) && path.indexOf(path(length)) == length)
    }
    // 路径长度
    var length = 2
    // 初始顶点路径集合中仅有一条包含自身顶点编号的路径
    val Intial_Graph = tpinWithCommunityId.mapVertices((id, vd) => Seq[Seq[VertexId]](Seq[VertexId](id)))
    //      .mapEdges((edge)=>edge.attr.isAntecedent())

    //annotation of david:路径的第一条边一定是IL
    val IL_Paths = Intial_Graph.subgraph(epred = e => e.attr.is_IL).aggregateMessages[Seq[Seq[VertexId]]](
      ctx => ctx.sendToDst(ctx.srcAttr.map(_ ++ Seq(ctx.dstId))),_ ++ _)
    // 每个顶点路径集合添加到全局路径集合（paths）
    var paths = IL_Paths.flatMap(_._2).collect
    // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
    // Graph[Seq[scala.Seq[graphx.VertexId]], Boolean] 点的属性为IL边，边的属性为是否是前件路径
    var subgraph = Intial_Graph.outerJoinVertices(IL_Paths) { case (vid, oldattr, option) =>
      option match {
        case Some(list) => list.filter(_.size==2)
        case None => oldattr.filter(_.size==2)
      }
    }.mapEdges((edge)=>edge.attr.isImportantAntecedent())

    //annotation of david:由企业通知控制人
    val Messages = subgraph.subgraph(epred = e => e.attr).aggregateMessages[LinkedList[Long]](
      ctx => ctx.sendToSrc(LinkedList(ctx.dstId)),_ ++ _)
    //    Messages.foreach(println)
    //每个控制人知道了自己的所直接控制企业
    val KnowCompany=subgraph.outerJoinVertices(Messages) { case (vid, oldattr, option) =>
      option match {
        case Some(list) =>
          (oldattr, list)
        case None => (oldattr, LinkedList[Long]())
      }
    }
    //annotation of david:点必须是IL边的端点，边必须是前件边
    val Messages_off_direction = KnowCompany.subgraph(epred = e => e.attr).aggregateMessages[LinkedList[(Long,LinkedList[Long])]](
      ctx => {
        //IL边的路径大小目前为2，否则为1
        if(ctx.srcAttr._1.size>1)
          ctx.sendToDst(LinkedList((ctx.srcId, ctx.srcAttr._2)))},_ ++ _)
    //    Messages_off_direction.foreach(println)
    val StopNodes = Messages_off_direction.flatMap{case (dstid,list)=> FindFrequenceContent(list)}
      .reduceByKey((a,b)=>a).groupBy(_._1._2)
    //    StopNodes.foreach(println)
    val KnowStopNodes = subgraph.outerJoinVertices(StopNodes){case (vid, path_ILs, option) =>
      option match {
        case Some(stopNodes) =>
          val keys=path_ILs.map(seq=>(seq(0),seq(1)))
          stopNodes.filter(entity=>keys.contains(entity._1))
        case None =>  Iterable[((Long, Long), LinkedList[Long])]()
      }
    }
    //    TestMain.printGraph(KnowStopNodes)
    val long2Paths = KnowStopNodes.aggregateMessages[Seq[Seq[VertexId]]]((ctx=>{
      if (ctx.attr) {
        for(((node1,node2),stopNodes) <- ctx.srcAttr){
          if(!stopNodes.contains(ctx.dstId)){
            ctx.sendToDst(Seq(Seq[VertexId](node1,node2,ctx.dstId)))
          }
        }
      }
    }), _ ++ _).mapValues(removeCircle(_, length)).filter(_._2.size > 0)

    paths =  paths ++long2Paths.flatMap(_._2).collect

    val vertexId = long2Paths.sortByKey().map(_._1).collect

    subgraph = subgraph.subgraph(vpred = (id, vd) => Arrays.binarySearch(vertexId, id) >= 0).joinVertices[Seq[Seq[VertexId]]](long2Paths)((id, vd, path) => path)

    while (subgraph.numVertices > 0) {
      // 路径长度+1
      length = length + 1
      // 每个顶点把自身变化后的顶点路径集合沿边发送到邻接顶点
      val vertices = subgraph.aggregateMessages[Seq[Seq[VertexId]]](sendPaths(_), _ ++ _).mapValues(removeCircle(_, length)).filter(_._2.size > 0)
      // 每个顶点路径集合添加到全局路径集合（paths）
      paths = paths ++ vertices.flatMap(_._2).collect
      // 接收到消息的顶点的顶点编号，排序以便二分查找
      val vertexId = vertices.sortByKey().map(_._1).collect
      // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
      subgraph = subgraph.subgraph(vpred = (id, vd) => Arrays.binarySearch(vertexId, id) >= 0).joinVertices[Seq[Seq[VertexId]]](vertices)((id, vd, path) => path)
    }
    // 返回全局路径集合
    sparkContext.parallelize(paths)
  }

  //annotation of david:构建IL开头的路径集合
  def getPathStartFromIL(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr],weight:Double):RDD[Seq[VertexId]]= {
    // 发送路径
    def sendPaths(edge: EdgeContext[Seq[Seq[VertexId]], Boolean, Seq[Seq[VertexId]]]): Unit = {
      // 过滤掉仅含交易权重的边
      if (edge.attr) {
        // 顶点路径集合中的每条路径追加终点编号
        val paths = edge.srcAttr.map(_ ++ Seq(edge.dstId))
        // 向终点发送顶点路径集合
        edge.sendToDst(paths)
      }
    }
    // 去除环
    def removeCircle(paths: Seq[Seq[VertexId]], length: Int): Seq[Seq[VertexId]] = {
      // 过滤掉路径中包含相同顶点的路径
      paths.filter(path => path.size == (length+1) && path.indexOf(path(length)) == length)
    }
    // 路径长度
    var length = 1
    // 初始顶点路径集合中仅有一条包含自身顶点编号的路径
    val Intial_Graph = tpinWithCommunityId.mapVertices((id, vd) => Seq[Seq[VertexId]](Seq[VertexId](id)))
    //      .mapEdges((edge)=>edge.attr.isAntecedent())

    //annotation of david:路径的第一条边一定是IL
    val IL_Paths = Intial_Graph.subgraph(epred = e => e.attr.is_IL).aggregateMessages[Seq[Seq[VertexId]]](
      ctx => ctx.sendToDst(ctx.srcAttr.map(_ ++ Seq(ctx.dstId))),_ ++ _)
    // 每个顶点路径集合添加到全局路径集合（paths）
    var paths = IL_Paths.flatMap(_._2).collect
    // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
    var subgraph = Intial_Graph.outerJoinVertices(IL_Paths) { case (vid, oldattr, option) =>
      option match {
        case Some(list) => list
        case None => oldattr
      }
    }.mapEdges((edge)=>edge.attr.isAntecedent(weight))
    // 判断子图中顶点数量等于0时终止
    while (subgraph.numVertices > 0) {
      // 路径长度+1
      length = length + 1
      // 每个顶点把自身变化后的顶点路径集合沿边发送到邻接顶点
      val vertices = subgraph.aggregateMessages[Seq[Seq[VertexId]]](sendPaths(_), _ ++ _).mapValues(removeCircle(_, length)).filter(_._2.size > 0)
      // 每个顶点路径集合添加到全局路径集合（paths）
      paths = paths ++ vertices.flatMap(_._2).collect
      // 接收到消息的顶点的顶点编号，排序以便二分查找
      val vertexId = vertices.sortByKey().map(_._1).collect
      // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
      subgraph = subgraph.subgraph(vpred = (id, vd) => Arrays.binarySearch(vertexId, id) >= 0).joinVertices[Seq[Seq[VertexId]]](vertices)((id, vd, path) => path)
    }
    // 返回全局路径集合
    sparkContext.parallelize(paths)
  }

  //annotation of david:构建前件路径集合
  def constructPaths(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr]): RDD[Seq[VertexId]] = {
    // 发送路径
    def sendPaths(edge: EdgeContext[Seq[Seq[VertexId]], Boolean, Seq[Seq[VertexId]]]): Unit = {
      // 过滤掉仅含交易权重的边
      if (edge.attr) {
        // 顶点路径集合中的每条路径追加终点编号
        val paths = edge.srcAttr.map(_ ++ Seq(edge.dstId))
        // 向终点发送顶点路径集合
        edge.sendToDst(paths)
      }
    }
    // 去除环
    def removeCircle(paths: Seq[Seq[VertexId]], length: Int): Seq[Seq[VertexId]] = {
      // 过滤掉路径中包含相同顶点的路径
      paths.filter(path => path.size == (length+1) && path.indexOf(path(length)) == length)
    }
    // 路径长度
    var length = 0
    // 初始顶点路径集合中仅有一条包含自身顶点编号的路径
    var subgraph = tpinWithCommunityId.mapVertices((id, vd) => Seq[Seq[VertexId]](Seq[VertexId](id)))
      .mapEdges((edge)=>edge.attr.isAntecedent())
    // 每个顶点路径集合添加到全局路径集合（paths）
    var paths = subgraph.vertices.flatMap(_._2).collect
    // 判断子图中顶点数量等于0时终止
    while (subgraph.numVertices > 0) {
      // 路径长度+1
      length = length + 1
      // 每个顶点把自身变化后的顶点路径集合沿边发送到邻接顶点
      val vertices = subgraph.aggregateMessages[Seq[Seq[VertexId]]](sendPaths(_), _ ++ _).mapValues(removeCircle(_, length)).filter(_._2.size > 0)
      // 每个顶点路径集合添加到全局路径集合（paths）
      paths = paths ++ vertices.flatMap(_._2).collect
      // 接收到消息的顶点的顶点编号，排序以便二分查找
      val vertexId = vertices.sortByKey().map(_._1).collect
      // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
      subgraph = subgraph.subgraph(vpred = (id, vd) => Arrays.binarySearch(vertexId, id) >= 0).joinVertices[Seq[Seq[VertexId]]](vertices)((id, vd, path) => path)
    }
    // 返回全局路径集合
    sparkContext.parallelize(paths)
  }

  //annotation of david:构建前件路径集合
  def constructPaths(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr],weight:Double): RDD[Seq[VertexId]] = {
    // 发送路径
    def sendPaths(edge: EdgeContext[Seq[Seq[VertexId]], Boolean, Seq[Seq[VertexId]]]): Unit = {
      // 过滤掉仅含交易权重的边
      if (edge.attr) {
        // 顶点路径集合中的每条路径追加终点编号
        val paths = edge.srcAttr.map(_ ++ Seq(edge.dstId))
        // 向终点发送顶点路径集合
        edge.sendToDst(paths)
      }
    }
    // 去除环
    def removeCircle(paths: Seq[Seq[VertexId]], length: Int): Seq[Seq[VertexId]] = {
      // 过滤掉路径中包含相同顶点的路径
      paths.filter(path => path.size == (length+1) && path.indexOf(path(length)) == length)
    }
    // 路径长度
    var length = 0
    // 初始顶点路径集合中仅有一条包含自身顶点编号的路径
    var subgraph = tpinWithCommunityId.mapVertices((id, vd) => Seq[Seq[VertexId]](Seq[VertexId](id)))
      .mapEdges((edge)=>edge.attr.isAntecedent(weight))
    // 每个顶点路径集合添加到全局路径集合（paths）
    var paths = subgraph.vertices.flatMap(_._2).collect
    // 判断子图中顶点数量等于0时终止
    while (subgraph.numVertices > 0) {
      // 路径长度+1
      length = length + 1
      // 每个顶点把自身变化后的顶点路径集合沿边发送到邻接顶点
      val vertices = subgraph.aggregateMessages[Seq[Seq[VertexId]]](sendPaths(_), _ ++ _).mapValues(removeCircle(_, length)).filter(_._2.size > 0)
      // 每个顶点路径集合添加到全局路径集合（paths）
      paths = paths ++ vertices.flatMap(_._2).collect
      // 接收到消息的顶点的顶点编号，排序以便二分查找
      val vertexId = vertices.sortByKey().map(_._1).collect
      // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
      subgraph = subgraph.subgraph(vpred = (id, vd) => Arrays.binarySearch(vertexId, id) >= 0).joinVertices[Seq[Seq[VertexId]]](vertices)((id, vd, path) => path)
    }
    // 返回全局路径集合
//    val a=sparkContext.parallelize(paths.map{case path=>(path.last,path)}).groupByKey().map{case (vid,ite)=> (vid,ite.toSeq)}
    sparkContext.parallelize(paths)
//    val a=paths.map{case path=>(path.last,path)}
  }

  //annotation of david:构建20%前件路径集合
  def constructPaths_GT0_2(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr]): RDD[Seq[VertexId]] = {
    // 发送路径
    def sendPaths(edge: EdgeContext[Seq[Seq[VertexId]], Boolean, Seq[Seq[VertexId]]]): Unit = {
      // 过滤掉仅含交易权重的边
      if (edge.attr) {
        // 顶点路径集合中的每条路径追加终点编号
        val paths = edge.srcAttr.map(_ ++ Seq(edge.dstId))
        // 向终点发送顶点路径集合
        edge.sendToDst(paths)
      }
    }
    // 去除环
    def removeCircle(paths: Seq[Seq[VertexId]], length: Int): Seq[Seq[VertexId]] = {
      // 过滤掉路径中包含相同顶点的路径
      paths.filter(path => path.size == (length+1) && path.indexOf(path(length)) == length)
    }
    // 路径长度
    var length = 0
    // 初始顶点路径集合中仅有一条包含自身顶点编号的路径
    var subgraph = tpinWithCommunityId.mapVertices((id, vd) => Seq[Seq[VertexId]](Seq[VertexId](id)))
      .mapEdges((edge)=>edge.attr.isImportantAntecedent())
    // 每个顶点路径集合添加到全局路径集合（paths）
    var paths = subgraph.vertices.flatMap(_._2).collect
    // 判断子图中顶点数量等于0时终止
    while (subgraph.numVertices > 0) {
      // 路径长度+1
      length = length + 1
      // 每个顶点把自身变化后的顶点路径集合沿边发送到邻接顶点
      val vertices = subgraph.aggregateMessages[Seq[Seq[VertexId]]](sendPaths(_), _ ++ _).mapValues(removeCircle(_, length)).filter(_._2.size > 0)
      // 每个顶点路径集合添加到全局路径集合（paths）
      paths = paths ++ vertices.flatMap(_._2).collect
      // 接收到消息的顶点的顶点编号，排序以便二分查找
      val vertexId = vertices.sortByKey().map(_._1).collect
      // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
      subgraph = subgraph.subgraph(vpred = (id, vd) => Arrays.binarySearch(vertexId, id) >= 0).joinVertices[Seq[Seq[VertexId]]](vertices)((id, vd, path) => path)
    }
    // 返回全局路径集合
    sparkContext.parallelize(paths)
  }

  //annotation of david:构建关联交易模式（单向环和双向环）
  def Match_Pattern_1(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr]): RDD[Pattern] = {

    val paths = PatternMatching.constructPaths(sparkContext, tpinWithCommunityId).persist()
//    println("模式1的前件路径个数："+paths.count())
    // 从带社团编号的TPIN中提取交易边
    //annotation of david:属性为社团id
    val tradeEdges = tpinWithCommunityId.edges.filter(_.attr.isTrade()).map(edge => ((edge.srcId, edge.dstId), edge.attr.community_id))
    // 构建路径终点和路径组成的二元组
    val pathsWithLastVertex = paths.map(path => (path.last, path))
    // 交易边的起点、终点和上一步的二元组join
    //annotation of david:一个交易边的两条前件路径，（包含他本身的）和社团id
    val pathTuples = tradeEdges.map(edge => (edge._1._1, (edge._1._2, edge._2))).join(pathsWithLastVertex)
      .map(edge => (edge._2._1._1, (edge._2._2, edge._2._1._2))).join(pathsWithLastVertex).map(edge => ((edge._2._1._1, edge._2._2), edge._2._1._2))
    // 匹配模式，规则为“1、以交易边起点为路径终点的路径的起点 等于 以交易边终点为路径终点的路径的起点（去除不成环的情况） 2、两条路径中顶点的交集的元素个数为1（去除交叉的情况）”
    val pattern1=pathTuples.filter(tuple => tuple._1._1(0) == tuple._1._2(0) && tuple._1._1.intersect(tuple._1._2).size == 1)
    val toReturn=pattern1.map{case((path1,path2),cid)=>
      val edges=
//         val a =(Seq[Seq[Long]]()++path1.sliding(2)
//       val b = path1.sliding(2).toSeq
        (Seq[Seq[Long]]()++path1.sliding(2)).filter(_.size==2).map(seq=>(seq(0),seq(1))) ++
        (Seq[Seq[Long]]()++path2.sliding(2)).filter(_.size==2).map(seq=>(seq(0),seq(1))) ++
        Seq((path1.last,path2.last))
      val vertices=(path1.tail++path2)
      val result=Pattern(1,edges.toSeq,vertices)
      result.communityId=cid
      result
    }
    toReturn
  }

  //annotation of david:构建互锁关联交易模式（单向环和双向环），未考虑第三方企业的选择
  def Match_Pattern_2(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr],weight:Double): RDD[Pattern] = {


    //annotation of david:不允许单向环的出现
    val paths = PatternMatching.getPathStartFromIL(sparkContext, tpinWithCommunityId,weight).filter(_.size>2).persist()
    println("模式2的前件路径个数："+paths.count())
    //    paths.foreach(println)
    // 从带社团编号的TPIN中提取交易边
    //annotation of david:属性为社团id
    val tradeEdges = tpinWithCommunityId.edges.filter(_.attr.isTrade()).map(edge => ((edge.srcId, edge.dstId), edge.attr.community_id))
    // 构建路径终点和路径组成的二元组
    val pathsWithLastVertex = paths.map(path => (path.last, path))
    // 交易边的起点、终点和上一步的二元组join
    //annotation of david:一个交易边的两条前件路径，（包含他本身的）和社团id
    val pathTuples = tradeEdges.map(edge => (edge._1._1, (edge._1._2, edge._2))).join(pathsWithLastVertex)
      .map(edge => (edge._2._1._1, (edge._2._2, edge._2._1._2))).join(pathsWithLastVertex).map(edge => ((edge._2._1._1, edge._2._2), edge._2._1._2))
    // 匹配模式，规则为“1、以交易边起点为路径终点的路径的起点 等于 以交易边终点为路径终点的路径的起点（去除不成环的情况） 2、两条路径中顶点的交集的元素个数为1（去除交叉的情况）”
    val pattern1=pathTuples.filter(tuple =>
      tuple._1._1(0) == tuple._1._2(1)
        &&tuple._1._1(1) == tuple._1._2(0)
        && tuple._1._1.intersect(tuple._1._2).size == 2)

    val toReturn=pattern1.map{case((path1,path2),cid)=>
      val edges= (Seq[Seq[Long]]()++path1.sliding(2)).filter(_.size==2).map(seq=>(seq(0),seq(1))) ++
          (Seq[Seq[Long]]()++path2.sliding(2)).filter(_.size==2).map(seq=>(seq(0),seq(1))) ++
          Seq((path1.last,path2.last))
      val vertices=(path1.tail.tail++path2)
      val result=Pattern(2,edges.toSeq,vertices)
      result.communityId=cid
      result
    }
    toReturn
  }

  //annotation of david:构建隐藏互锁交易模式（单向环和双向环）
  def Match_Pattern_3(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr]): RDD[Pattern] = {

    val paths = PatternMatching.getPathsForPattern3(sparkContext, tpinWithCommunityId).persist()
//    println("模式3的前件路径个数："+paths.count())
    //        paths.foreach(println)
    // 从带社团编号的TPIN中提取交易边
    //annotation of david:属性为社团id
    val tradeEdges = tpinWithCommunityId.edges.filter(_.attr.isTrade()).map(edge => ((edge.srcId, edge.dstId), edge.attr.community_id))
    // 构建路径终点和路径组成的二元组
    val pathsWithLastVertex = paths.map(path => (path.last, path))
    // 交易边的起点、终点和上一步的二元组join
    //annotation of david:一个交易边的两条前件路径，（包含他本身的）和社团id
    val pathTuples = tradeEdges.map(edge => (edge._1._1, (edge._1._2, edge._2))).join(pathsWithLastVertex)
      .map(edge => (edge._2._1._1, (edge._2._2, edge._2._1._2))).join(pathsWithLastVertex).map(edge => ((edge._2._1._1, edge._2._2), edge._2._1._2))
    // 匹配模式，规则为“1、以交易边起点为路径终点的路径的起点 等于 以交易边终点为路径终点的路径的起点（去除不成环的情况） 2、两条路径中顶点的交集的元素个数为1（去除交叉的情况）”
    val pattern1=pathTuples.filter(tuple =>
      tuple._1._1(0) == tuple._1._2(1)
        &&tuple._1._1(1) == tuple._1._2(0)
        && tuple._1._1.intersect(tuple._1._2).size == 2)

    val toReturn=pattern1.map{case((path1,path2),cid)=>
      val edges=
        (Seq[Seq[Long]]()++path1.sliding(2)).filter(_.size==2).map(seq=>(seq(0),seq(1))) ++
          (Seq[Seq[Long]]()++path2.sliding(2)).filter(_.size==2).map(seq=>(seq(0),seq(1))) ++
          Seq((path1.last,path2.last))
      val vertices=(path1.tail.tail++path2)
      val result=Pattern(3,edges.toSeq,vertices)
      result.communityId=cid
      result
    }.filter(!_.edges.isEmpty)
    toReturn
  }

  //annotation of david: 放松版的互锁边，加强版的前件路径，互锁交易模式
  def Match_Pattern_4(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr]): RDD[Pattern] ={

    val paths = PatternMatching.getPathsForPattern4(sparkContext, tpinWithCommunityId).persist()
    //    println("模式4的前件路径个数："+paths.count())
    //            paths.foreach(println)
    // 从带社团编号的TPIN中提取交易边
    //annotation of david:属性为社团id
    val tradeEdges = tpinWithCommunityId.edges.filter(_.attr.isTrade()).map(edge => ((edge.srcId, edge.dstId), edge.attr.community_id))
    // 构建路径终点和路径组成的二元组
    val pathsWithLastVertex = paths.map(path => (path.last, path))
    // 交易边的起点、终点和上一步的二元组join
    //annotation of david:一个交易边的两条前件路径，（包含他本身的）和社团id
    val pathTuples = tradeEdges.map(edge => (edge._1._1, (edge._1._2, edge._2))).join(pathsWithLastVertex)
      .map(edge => (edge._2._1._1, (edge._2._2, edge._2._1._2))).join(pathsWithLastVertex).map(edge => ((edge._2._1._1, edge._2._2), edge._2._1._2))
    // 匹配模式，规则为“1、以交易边起点为路径终点的路径的起点 等于 以交易边终点为路径终点的路径的起点（去除不成环的情况） 2、两条路径中顶点的交集的元素个数为1（去除交叉的情况）”
    val pattern1=pathTuples.filter(tuple =>
      tuple._1._1(0) == tuple._1._2(1)
        &&tuple._1._1(1) == tuple._1._2(0)
        && tuple._1._1.intersect(tuple._1._2).size == 2)

    val toReturn=pattern1.map{case((path1,path2),cid)=>
      val edges=
        (Seq[Seq[Long]]()++path1.sliding(2)).filter(_.size==2).map(seq=>(seq(0),seq(1))) ++
        (Seq[Seq[Long]]()++path2.sliding(2)).filter(_.size==2).map(seq=>(seq(0),seq(1))) ++
        Seq((path1.last,path2.last))
      val vertices=(path1.tail.tail++path2)
      val result=Pattern(4,edges.toSeq,vertices)
      result.communityId=cid
      result
    }.filter(!_.edges.isEmpty)
    toReturn
  }

  //annotation of david:构建基于董事会互锁的模式，只判断双向环
  def Match_Director_Interlock(sparkContext: SparkContext, tpinWithCommunityId: Graph[VertexAttr, EdgeAttr],weight: Double): RDD[Pattern] = {

    //annotation of david:只判断双向环
    val paths = PatternMatching.constructPaths(sparkContext, tpinWithCommunityId,weight).filter(_.size>1).persist()

    println("模式1的前件路径个数："+paths.count())
    // 从带社团编号的TPIN中提取交易边
    //annotation of david:属性为社团id
    val tradeEdges = tpinWithCommunityId.edges.filter(_.attr.isTrade()).map(edge => ((edge.srcId, edge.dstId), edge.attr.community_id))
    // 构建路径终点和路径组成的二元组
    val pathsWithLastVertex = paths.map(path => (path.last, path))
    // 交易边的起点、终点和上一步的二元组join
    //annotation of david:一个交易边的两条前件路径，（包含他本身的）和社团id
    val pathTuples = tradeEdges.map(edge => (edge._1._1, (edge._1._2, edge._2))).join(pathsWithLastVertex)
      .map(edge => (edge._2._1._1, (edge._2._2, edge._2._1._2))).join(pathsWithLastVertex).map(edge => ((edge._2._1._1, edge._2._2), edge._2._1._2))
    // 匹配模式，规则为“1、以交易边起点为路径终点的路径的起点 等于 以交易边终点为路径终点的路径的起点（去除不成环的情况） 2、两条路径中顶点的交集的元素个数为1（去除交叉的情况）”
    val pattern1=pathTuples.filter(tuple => tuple._1._1(0) == tuple._1._2(0) && tuple._1._1.intersect(tuple._1._2).size == 1)
    val toReturn=pattern1.map{case((path1,path2),cid)=>
      val edges=
      //         val a =(Seq[Seq[Long]]()++path1.sliding(2)
      //       val b = path1.sliding(2).toSeq
        (Seq[Seq[Long]]()++path1.sliding(2)).filter(_.size==2).map(seq=>(seq(0),seq(1))) ++
          (Seq[Seq[Long]]()++path2.sliding(2)).filter(_.size==2).map(seq=>(seq(0),seq(1))) ++
          Seq((path1.last,path2.last))
      val vertices=(path1.tail++path2)
      val result=Pattern(1,edges.toSeq,vertices)
      result.communityId=cid
      result
    }
    toReturn
  }

  //annotation of david:构建IL开头的路径集合
  def getPathStartFromIL(graph: Graph[VertexAttr, EdgeAttr],weight:Double)= {

    //annotation of david:路径的第一条边一定是IL
    val IL_Paths = graph
        .subgraph(epred = e => e.attr.is_IL)
        .mapVertices((id, vd) => Seq(Seq(id)))
        .aggregateMessages[Seq[Seq[VertexId]]](ctx =>
      ctx.sendToDst(ctx.srcAttr.map(_ ++ Seq(ctx.dstId))),_ ++ _
    )
    // 使用接收到消息的顶点及其边构建子图，二分查找判断顶点编号是否在上一步的vertexId中提高效率
    val initialGraph = graph.outerJoinVertices(IL_Paths) { case (vid, oldattr, option) =>
      option match {
        case Some(list) => list
        case None => Seq[Seq[VertexId]]()
      }
    }
    AddNewAttr.getPath(initialGraph,weight,initLength = 2)
  }
  //annotation of david:构建互锁关联交易模式（单向环和双向环），未考虑第三方企业的选择
  def matchPattern_2(graph: Graph[VertexAttr, EdgeAttr],weight:Double): RDD[Pattern] = {
    // 从带社团编号的TPIN中提取交易边
    //annotation of david:属性为社团id
    val tradeEdges = graph.edges.filter(_.attr.isTrade()).map(edge => ((edge.srcId, edge.dstId), edge.attr.community_id)).cache()
    // 构建路径终点和路径组成的二元组
    val pathsWithLastVertex = getPathStartFromIL(graph,weight).flatMap{case (vid,paths)=>paths.map((vid,_))}.cache()
    // 交易边的起点、终点和上一步的二元组join
    //annotation of david:一个交易边的两条前件路径，（包含他本身的）和社团id
    val pathTuples = tradeEdges
        .keyBy(_._1._1).join(pathsWithLastVertex)
        .keyBy(_._2._1._1._2).join(pathsWithLastVertex)
        .map{case(dstid,( (srcid,(((srcid2,dstid2),cid),srcpath)) ,dstpath)) =>
          ((srcpath,dstpath),cid)
        }
    // 匹配模式，规则为“1、以交易边起点为路径终点的路径的起点 等于 以交易边终点为路径终点的路径的起点（去除不成环的情况） 2、两条路径中顶点的交集的元素个数为1（去除交叉的情况）”
    val pattern = pathTuples
        .filter { case ((srcpath, dstpath), cid) =>
          srcpath(0) == dstpath(1) && srcpath(1) == dstpath(0) && srcpath.intersect(dstpath).size == 2
        }
        .map { case ((srcpath, dstpath), cid) =>
          val edges=
            (
                (Seq[Seq[Long]]()++ srcpath.sliding(2) ++ dstpath.sliding(2)).filter(_.size==2) ++
                    Seq(Seq(srcpath.last,dstpath.last))
                ).map(list=> (list(0),list(1)))
          val vertices=(srcpath.tail.tail++dstpath)
          val result=Pattern(2,edges,vertices)
          result.communityId=cid
          result
        }
    pattern
  }

}
