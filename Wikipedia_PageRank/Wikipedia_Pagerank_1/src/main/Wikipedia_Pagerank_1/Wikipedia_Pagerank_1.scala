/**
 * Created by chunyangshen on 2/21/15.
 */
/**
 * Created by fujun on 2/8/15.
 */
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import java.io.{PrintWriter, File, FileWriter, BufferedWriter}
import org.apache.spark.rdd.RDD
import scala.xml.{XML, NodeSeq}
import org.apache.spark.graphx._

object Wikipedia_Pagerank_1 {
  def main(args: Array[String]): Unit ={

        if (args.length < 4) {
          System.err.println("Usage: Wikipedia_Pagerank_1 <inputFile> <rank> " +
            "<numIterations> <outputFile>")
          System.exit(-1)
        }
    val startTime = System.currentTimeMillis
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Wikipedia_Pagerank_1")
    val sc = new SparkContext(sparkConf)
    val inputFile = args(0)
    val top = args(1).toInt
    val numIterations = args(2).toInt
    val outputFile = args(3)
/*
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[1]") setAppName("Wikipedia_Pagerank_1")
    val sc = new SparkContext(sparkConf)
    val inputFile = "/Users/chunyangshen/mini.tsv"
    val outputFile = "/Users/chunyangshen/output/"
    val numIterations = 20
    val top = 100
*/
    val input = sc.textFile(inputFile)
    val partitioner = new HashPartitioner(sc.defaultParallelism)
    val articles = dataExtract(input, partitioner)
    val rank = pageRank(articles,numIterations,partitioner)
    val results = formatResult(rank)
    sc.parallelize(results.collect.take(top)).saveAsTextFile(outputFile)
    sc.stop()
    val time = (System.currentTimeMillis - startTime) / 1000.0
    println("Completed %d iterations in %f seconds: %f seconds per iteration"
      .format(numIterations, time, time / numIterations))
  }


  def formatResult (input:RDD[(String, Double)]):RDD[String] = {
    val result=input.sortBy(_._2,false).map{
      case (title, rank) =>"%-70s %4.10f "
        .format(title,rank)
    }
    result
  }

  def dataExtract(input:RDD[String], partitioner: Partitioner)
  : RDD[(String, Array[String])]={
    val output = input.map(textline => {
      val field = textline.split("\t")
      val title = new String(field(1))
      val body = field(3).replace("\\n", "\n")
      val linksXML =
        if (body == "\\N") {
          NodeSeq.Empty
        } else {
          try {
            XML.loadString(body) \\ "link" \ "target"
          } catch {
            case e: org.xml.sax.SAXParseException =>
              System.err.println("Article \"" + title + "\" has malformed XML in body:\n" + body)
              NodeSeq.Empty
          }
        }
      val linksArray = linksXML.map(link => new String(link.text)).toArray
      (title,linksArray)
    }).partitionBy(partitioner).cache()
    output
  }

  def pageHash(title: String): VertexId = {
    title.toLowerCase.replace(" ", "").hashCode.toLong
  }


  def pageRank(
                links : RDD[(String, Array[String])],
                numIterations: Int,
                partitioner: Partitioner)
  : RDD[(String,Double)] ={
    val vertices = links.map(a => (pageHash(a._1), a._1)).cache
    val edges = links.flatMap( line => {
      val srcVer = pageHash(line._1)
      (line._2).map(x => {
        val dirVer = pageHash(x)
        Edge(srcVer.toLong, dirVer.toLong, 0)
      })
    })
    val graph = Graph(vertices, edges, "").subgraph(vpred = { (v, d) => d.nonEmpty}).cache
    val vertexrank =  graph.staticPageRank(numIterations).cache()
    val pagerank = graph.outerJoinVertices(vertexrank.vertices)
    {
      case (verid, ver, optrank ) => (ver , optrank.getOrElse(0.0))
    }
        pagerank.vertices.map (
          line=> (line._2._1, line._2._2)
        )
  }

}
