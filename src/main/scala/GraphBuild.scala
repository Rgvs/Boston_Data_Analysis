import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import java.io._

import org.apache.spark.graphx.{Edge, Graph}

import scala.util.parsing.json.JSON
import scala.io._

class SimpleCSVHeader(header: Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap

  def apply(array: Array[String], key: String): String = array(index(key))
}

object GraphBuild {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("Uber")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    // reads hourly aggreagate
    val csv = sc.textFile("./data/boston-censustracts-2016-1-All-HourlyAggregate.csv")
    // spliting by comman and trimming elements
    val data = csv.map(line => line.split(",").map(elem => elem.trim))
    // building our header with the first line,
    // it is used to get data from the element without using index values like ._1 for source id
    val header = new SimpleCSVHeader(data.take(1)(0))
    // filter the header out
    val edge_rows = data.filter(line => header(line, "dstid") != "dstid")
      .map(row => Edge(header(row, "sourceid").toLong, header(row, "dstid").toLong, row.slice(2, 7)))

    // getting only dstIds
    //val dstIds = edge_rows.map(row => header(row,"dstid"))
    // getting map from source ids to dst ids
    //val sourceToDst = edge_rows.map(row => header(row,"sourceid").toInt -> header(row,"dstid").toInt)

    //println(dstIds.collect()(2))
    //println(sourceToDst.collect()(2))

    // getting vertex data from boundaries file
    val raw_movements = Source.fromFile("./data/boston_censustracts.json").getLines.mkString

    // Json Parsing and getting features
    val movements = JSON.parseFull(raw_movements)
      .get.asInstanceOf[Map[String, Any]]("features")
      .asInstanceOf[List[Map[String, Any]]]

    // parallizing the vertex data and tranforming into a tuple of (movement_id, display_name, coordinates)
    // display name is the street name,
    // coordinates are of multi polygon
    val movements_rdd = sc.parallelize(movements)
      .map(feature => (
        feature("properties").asInstanceOf[Map[String, Any]]("MOVEMENT_ID").asInstanceOf[String].toLong,
        (feature("properties").asInstanceOf[Map[String, Any]]("DISPLAY_NAME").asInstanceOf[String],
          feature("geometry").asInstanceOf[Map[String, Any]]("coordinates")
            .asInstanceOf[List[List[List[List[Float]]]]]
        )))

    println(movements_rdd.collect()(0))

    val graph = Graph(movements_rdd, edge_rows)
    //println(graph.numEdges)
    //println(graph.numVertices)
    println(graph.edges.take(1)(0).attr.mkString(" "))
    sc.stop()
  }
}