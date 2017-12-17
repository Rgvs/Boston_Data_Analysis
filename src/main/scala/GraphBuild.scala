import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.parsing.json.JSON


object GraphBuild {
  def makeArrayHour(value: List[(Int, Float)]): Array[Float] = {
    val x = Array.fill[Float](24)(Float.MaxValue)
    for (v <- value) {
      x(v._1) = v._2
    }
    x
  }

  def makeArrayWeek(value: List[(Int, Float)]): Array[Float] = {
    val x = Array.fill[Float](7)(Float.MaxValue)
    for (v <- value) {
      x(v._1 - 1) = v._2
    }
    x
  }

  def makeArrayMonth(value: List[(Int, Float)]): Array[Float] = {
    val x = Array.fill[Float](12)(Float.MaxValue)
    for (v <- value) {
      x(v._1 - 1) = v._2
    }
    x
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
    conf.setAppName("Uber")
    conf.setMaster("local[2]")
    val sc = new SparkContext(conf)

    // reads hourly aggreagate
    // spliting by comman and trimming elements

    val csv_hour1 = sc.textFile("./data/boston-censustracts-2016-1-All-HourlyAggregate.csv")
      .map(line => line.split(",").map(elem => elem.trim))
      .filter(line => line(1) != "dstid")
    val csv_hour2 = sc.textFile("./data/boston-censustracts-2016-2-All-HourlyAggregate.csv")
      .map(line => line.split(",").map(elem => elem.trim))
      .filter(line => line(1) != "dstid")

    val data_hour = csv_hour1.union(csv_hour2)
      .map(row => ((row(0), row(1), row(2).toInt), (row(3).toFloat, 1)))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .map(row => ((row._1._1, row._1._2), (row._1._3, row._2._1/row._2._2)))
      .groupByKey()
      .mapValues(value => makeArrayHour(value.toList))

    val csv_week1 = sc.textFile("./data/boston-censustracts-2016-1-WeeklyAggregate.csv")
      .map(line => line.split(",").map(elem => elem.trim))
      .filter(line => line(1) != "dstid")
    val csv_week2 = sc.textFile("./data/boston-censustracts-2016-2-WeeklyAggregate.csv")
      .map(line => line.split(",").map(elem => elem.trim))
      .filter(line => line(1) != "dstid")

    val data_week = csv_week1.union(csv_week2)
      .map(row => ((row(0), row(1), row(2).toInt), (row(3).toFloat, 1)))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .map(row => ((row._1._1, row._1._2), (row._1._3, row._2._1/row._2._2)))
      .groupByKey()
      .mapValues(value => makeArrayWeek(value.toList))


    val csv_month1 = sc.textFile("./data/boston-censustracts-2016-1-All-MonthlyAggregate.csv")
      .map(line => line.split(",").map(elem => elem.trim))
      .filter(line => line(1) != "dstid")
    val csv_month2 = sc.textFile("./data/boston-censustracts-2016-2-All-MonthlyAggregate.csv")
      .map(line => line.split(",").map(elem => elem.trim))
      .filter(line => line(1) != "dstid")

    val data_month = csv_month1.union(csv_month2)
      .map(row => ((row(0), row(1), row(2).toInt), (row(3).toFloat, 1)))
      .reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
      .map(row => ((row._1._1, row._1._2), (row._1._3, row._2._1/row._2._2)))
      .groupByKey()
      .mapValues(value => makeArrayMonth(value.toList))

    val data_edge = data_month.join(data_hour).join(data_week)
      .map(row => Edge(row._1._1.toLong, row._1._2.toLong, (row._2._1._1, row._2._1._2, row._2._2)))


    // getting vertex data from boundaries file
    val raw_movements = Source.fromFile("./data/boston_censustracts.json").getLines.mkString

    // Json Parsing and getting features
    val movements = JSON.parseFull(raw_movements)
      .get.asInstanceOf[Map[String, Any]]("features")
      .asInstanceOf[List[Map[String, Any]]]

    // parallizing the vertex data and tranforming into a tuple of (movement_id, display_name)
    // display name is the street name,
    // coordinates are of multi polygon
    val movements_rdd = sc.parallelize(movements)
      .map(feature => (
        feature("properties").asInstanceOf[Map[String, Any]]("MOVEMENT_ID").asInstanceOf[String].toLong,
        feature("properties").asInstanceOf[Map[String, Any]]("DISPLAY_NAME").asInstanceOf[String]))

    val filename = "data/police_fire_hospital_combined.txt"

    val police_fire_data: ListBuffer[Any] = ListBuffer()

    for (line <- Source.fromFile(filename).getLines) {
      police_fire_data += JSON.parseFull(line)
    }

    val police_fire_data_rdd = sc.parallelize(police_fire_data)
      .map({case Some(line) => line.asInstanceOf[Map[String, Any]]})
      .map(line => (line("id").asInstanceOf[Double].toLong,
        (line("police").asInstanceOf[Boolean],
          line("fire_stations").asInstanceOf[Boolean],
          line("hospital").asInstanceOf[Boolean])))

    println(police_fire_data_rdd.take(1)(0))

    val nodes = movements_rdd.join(police_fire_data_rdd)
    println(nodes.collect()(0))

    val graph = Graph(nodes, data_edge)
    graph.vertices.saveAsObjectFile("data/vertices")
    graph.edges.saveAsObjectFile("data/edges")

    //val hospitals = graph.vertices.filter(vertex => vertex._2._2._3).map(vertex => vertex._1).collect().toSeq

    //val h_graph = ShortestPaths.run(graph, hospitals)
    //println(graph.vertices.map(vprop => (vprop._1, vprop._2._1)).take(1)(0))
    //println(graph.edges.take(1)(0).attr.mkString(" "))

    sc.stop()
  }
}