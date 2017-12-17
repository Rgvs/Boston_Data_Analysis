import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._


import scala.reflect.ClassTag

object SP {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */
  type Weight = (Array[Float], Array[Float], Array[Float])
  type SPMap = Map[VertexId, Weight]


  private def makeMap(x: (VertexId, Weight)*) = Map(x: _*)

  private def addWeights(tuple: Weight, tuple1: Weight): Weight = {
    var tuple2: Weight = (new Array[Float](12), new Array[Float](24), new Array[Float](7))
    for (i <- tuple._1.indices) {
      if (tuple._1(i) == Float.MaxValue || tuple2._1(i) == Float.MaxValue)
        tuple2._1(i) = Float.MaxValue
      else tuple2._1(i) = tuple._1(i) + tuple1._1(i)
    }
    for (i <- tuple._2.indices) {
      if (tuple._2(i) == Float.MaxValue || tuple2._2(i) == Float.MaxValue)
        tuple2._2(i) = Float.MaxValue
      else tuple2._2(i) = tuple._2(i) + tuple1._2(i)
    }
    for (i <- tuple._3.indices) {
      if (tuple._3(i) == Float.MaxValue || tuple2._3(i) == Float.MaxValue)
        tuple2._3(i) = Float.MaxValue
      else tuple2._3(i) = tuple._3(i) + tuple1._3(i)
    }
    tuple2
  }

  private def getMin(tuple: Weight, tuple1: Weight): Weight = {
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

  private def incrementMap(spmap: SPMap, weight: Weight): SPMap = spmap.map {
    case (v, d) => v -> addWeights(d, weight)
  }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> getMin(
        spmap1.getOrElse(k, (
          Array.fill[Float](12)(Float.MaxValue),
          Array.fill[Float](24)(Float.MaxValue),
          Array.fill[Float](7)(Float.MaxValue))),
        spmap2.getOrElse(k, (
          Array.fill[Float](12)(Float.MaxValue),
          Array.fill[Float](24)(Float.MaxValue),
          Array.fill[Float](7)(Float.MaxValue))))
    }.toMap

  /**
    * Computes shortest paths to the given set of landmark vertices.
    *
    * @tparam ED the edge attribute type (not used in the computation)
    *
    * @param graph the graph for which to compute the shortest paths
    * @param landmarks the list of landmark vertex ids. Shortest paths will be computed to each
    * landmark.
    *
    * @return a graph where each vertex attribute is a map containing the shortest-path distance to
    * each reachable landmark vertex.
    */
  def run[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: Seq[VertexId]): Graph[SPMap, ED] = {
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> (
        Array.fill[Float](12)(0),
        Array.fill[Float](24)(0),
        Array.fill[Float](7)(0))) else makeMap()
    }

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, ED]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr, edge.attr.asInstanceOf[Weight])
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("Uber")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    /*
    val edgeArr: Array[Edge[Int]] = Array(Edge(2, 1, 7),
              Edge(2, 4, 2),
              Edge(3, 2, 4),
              Edge(3, 6, 3),
              Edge(4, 1, 1),
              Edge(5, 2, 2),
              Edge(5, 3, 3),
              Edge(5, 6, 8),
              Edge(5, 7, 2),
              Edge(7, 6, 4),
              Edge(7, 4, 1))

    val edges = sc.parallelize(edgeArr)
    val graph = Graph.fromEdges(edges, true)
    val landmarks = graph.vertices.map(vertex => vertex._1).take(4).toSeq
    val results = run(graph, landmarks)

    results.vertices.collect().foreach(line =>
    println(line))
    */
    val vertices = sc.objectFile[(Long, (String, (Boolean, Boolean, Boolean)))]("data/vertices")
    //val random = sc.broadcast(scala.util.Random)
    val edges = sc.parallelize(sc.objectFile[Edge[(Array[Float], Array[Float], Array[Float])]]("data/edges")
      .take(1000))
    //.filter(e => random.value.nextInt(50) == 1)
    val graph = Graph(vertices, edges)

    val hospitals = graph.vertices.filter(vertex => vertex._2._2._2).map(vertex => vertex._1).collect().toSeq

    println(hospitals)
    println(graph.edges.take(1)(0).attr._1.length)
    println(graph.edges.take(1)(0).attr._2.length)
    println(graph.edges.take(1)(0).attr._3.length)
    val result = run(graph, hospitals)
    println(result.vertices.take(1)(0))

    sc.stop()
  }
}