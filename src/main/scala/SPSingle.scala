import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._


import scala.reflect.ClassTag

object SPSingle {
  /** Stores a map from the vertex id of a landmark to the distance to that landmark. */

  type SPMap = (VertexId, Float)

  private def incrementMap(spmap: SPMap, weight: Float): SPMap = {
    val spmap1: SPMap = (spmap._1, spmap._2 + weight)
    spmap1
  }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap = {
    var spmap3: SPMap = spmap1
    if (spmap1._1 > spmap2._2)
      spmap3 = spmap2
    spmap3
    }

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
      if (landmarks.contains(vid)) (vid, 0F) else (vid, Float.MaxValue)
    }

    val initialMessage = (0L, Float.MaxValue)

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, ED]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstAttr, edge.attr.asInstanceOf[Float])
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
    val edges = sc.objectFile[Edge[(Array[Float], Array[Float], Array[Float])]]("data/edges")
        .map(e => Edge(e.srcId, e.dstId, e.attr._1(0)))
      //.take(1000))
    //.filter(e => random.value.nextInt(50) == 1)
    val graph = Graph(vertices, edges)

    val hospitals = graph.vertices.filter(vertex => vertex._2._2._2).map(vertex => vertex._1).collect().toSeq


    val result = run(graph, hospitals)
    println(result.vertices.saveAsTextFile("data/hospital_month_1.txt"))

    sc.stop()
  }
}