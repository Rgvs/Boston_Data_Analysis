import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

object Direct {
  type Weight = (Array[Float], Array[Float], Array[Float])
  def func(tuple: Weight, tuple1: Weight): Weight = {

    var tuple2: Weight = (new Array[Float](12), new Array[Float](24), new Array[Float](7))
    for (i <- tuple._1.indices) {
      //println(i, tuple2._1. , tuple1._1.length, tuple._1.length)
      tuple2._1(i) = math.min(tuple._1(i), tuple1._1(i))
    }
    for (i <- tuple._2.indices) {
      tuple2._2(i) = math.min(tuple._2(i), tuple1._2(i))
    }
    for (i <- tuple._3.indices) {
      tuple2._3(i) = math.min(tuple._3(i), tuple1._3(i))
    }
    tuple2
  }
  def normEach(tuple: (Array[Float], Array[Float], Array[Float])): Weight = {
    var tuple1: Weight = (new Array[Float](12), new Array[Float](24), new Array[Float](7))
    var c:Int = 0
    var s:Float = 0
    for (i <- tuple._1.indices) {
      if(tuple._1(i) != Float.MaxValue) {
        tuple1._1(i) = tuple._1(i)
        c = c + 1
        s += tuple._1(i)
      }
    }

    for (i <- tuple._1.indices) {
      if(tuple._1(i) == Float.MaxValue) {
        tuple1._1(i) = s/c
      }
    }
    c = 0
    s = 0
    for (i <- tuple._2.indices) {
      if(tuple._2(i) != Float.MaxValue) {
        tuple1._2(i) = tuple._2(i)
        c = c + 1
        s += tuple._2(i)
      }
    }

    for (i <- tuple._2.indices) {
      if(tuple._2(i) == Float.MaxValue) {
        tuple1._2(i) = s/c
      }
    }
    c = 0
    s = 0
    for (i <- tuple._3.indices) {
      if(tuple._3(i) != Float.MaxValue) {
        tuple1._3(i) = tuple._3(i)
        c = c + 1
        s += tuple._3(i)
      }
    }

    for (i <- tuple._3.indices) {
      if(tuple._3(i) == Float.MaxValue) {
        tuple1._3(i) = s/c
      }
    }
    tuple1
  }

  def norm(tuple: (Weight, Weight, Weight)):(Weight, Weight, Weight) = {

    val tuple1 = (normEach(tuple._1), normEach(tuple._2), normEach(tuple._3))

    tuple1
  }
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Uber")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val vertices = sc.objectFile[(Long, (String, (Boolean, Boolean, Boolean)))]("data/vertices")
    //val random = sc.broadcast(scala.util.Random)
    val edges = sc.objectFile[Edge[(Array[Float], Array[Float], Array[Float])]]("data/edges")
    val edges_subway = sc.objectFile[Edge[(Array[Float], Array[Float], Array[Float])]]("data/edges_subway")




    val hospitals = vertices.filter(vertex => vertex._2._2._2).map(vertex => vertex._1).collect().toSeq

    val ed_filtered = edges.filter(edge => hospitals.contains(edge.srcId))
      .map(edge=> (edge.dstId, edge.attr))
      .reduceByKey((x,y) => func(x,y))

    val ed_filtered_sub = edges_subway.filter(edge => hospitals.contains(edge.srcId))
      .map(edge=> (edge.dstId, edge.attr))
      .reduceByKey((x,y) => func(x,y))

    val police = vertices.filter(vertex => vertex._2._2._1).map(vertex => vertex._1).collect().toSeq

    val ed_filtered1 = edges.filter(edge => police.contains(edge.srcId))
      .map(edge=> (edge.dstId, edge.attr))
      .reduceByKey((x,y) => func(x,y))

    val ed_filtered1_sub = edges_subway.filter(edge => police.contains(edge.srcId))
      .map(edge=> (edge.dstId, edge.attr))
      .reduceByKey((x,y) => func(x,y))

    val fire = vertices.filter(vertex => vertex._2._2._3).map(vertex => vertex._1).collect().toSeq

    val ed_filtered2 = edges.filter(edge => fire.contains(edge.srcId))
      .map(edge=> (edge.dstId, edge.attr))
      .reduceByKey((x,y) => func(x,y))

    val ed_filtered2_sub = edges_subway.filter(edge => fire.contains(edge.srcId))
      .map(edge=> (edge.dstId, edge.attr))
      .reduceByKey((x,y) => func(x,y))

    val ed_total = ed_filtered1.fullOuterJoin(ed_filtered).fullOuterJoin(ed_filtered2)
      .map(line => (line._1, (line._2._1._1, line._2._1._2, line._2._2)))
      .map(line => (line._1.toLong, norm(line._2)))

    val ed_total_sub = ed_filtered1_sub.fullOuterJoin(ed_filtered_sub).fullOuterJoin(ed_filtered2_sub)
      .map(line => (line._1, (line._2._1._1, line._2._1._2, line._2._2)))
      .map(line => (line._1.toLong, norm(line._2)))

    val ed_total_combined = ed_total.fullOuterJoin(ed_total_sub)
    /*
    ed_filtered.foreach(line =>
      println(line._1,
        line._2._3.deep.mkString(","),
        line._2._2.deep.mkString(","),
        line._2._1.deep.mkString(",")))
    */


    ed_total.foreach(line =>
      println(line._1,
        line._2._1._3.deep.mkString(","),

        line._2._1._2.deep.mkString(","),

        line._2._1._1.deep.mkString(","),

        line._2._2._3.deep.mkString(","),

        line._2._2._2.deep.mkString(","),

        line._2._2._1.deep.mkString(","),

        line._2._3._3.deep.mkString(","),

        line._2._3._2.deep.mkString(","),

        line._2._3._1.deep.mkString(",")

      ))

    println(ed_total_sub.count())
    println(ed_total_combined.count())
    println(edges_subway.count())
    println(ed_filtered1_sub.count())


    ed_total_combined.foreach(line =>
      println(line._1,
        line._2._1._1._3.deep.mkString(","),
        line._2._2._1._3.deep.mkString(","),
        line._2._1._1._2.deep.mkString(","),
        line._2._2._1._2.deep.mkString(","),
        line._2._1._1._1.deep.mkString(","),
        line._2._2._1._1.deep.mkString(","),
        line._2._1._2._3.deep.mkString(","),
        line._2._2._2._3.deep.mkString(","),
        line._2._1._2._2.deep.mkString(","),
        line._2._2._2._2.deep.mkString(","),
        line._2._1._2._1.deep.mkString(","),
        line._2._2._2._1.deep.mkString(","),
        line._2._1._3._3.deep.mkString(","),
        line._2._2._3._3.deep.mkString(","),
        line._2._1._3._2.deep.mkString(","),
        line._2._2._3._2.deep.mkString(","),
        line._2._1._3._1.deep.mkString(","),
        line._2._2._3._1.deep.mkString(",")
      ))
    sc.stop()
  }
}