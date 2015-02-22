/**
 * Created by chunyangshen on 2/17/15.
 */
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, SerializerInstance}
import java.io.{PrintWriter, File, FileWriter, BufferedWriter}
import org.apache.spark.rdd.RDD
import scala.xml.{XML, NodeSeq}


object Wikipedia_Pagerank_2 {
  def main(args: Array[String]): Unit ={
/*
    if (args.length < 4) {
      System.err.println("Usage: Wikipedia_Pagerank_2 <inputFile> <rank> " +
        "<numIterations> <outputFile>")
      System.exit(-1)
    }

    val sparkConf = new SparkConf()
    val inputFile = args(0)
    val top = args(1).toInt
    val numIterations = args(2).toInt
    val outputFile = args(3)
*/
    val sparkConf = new SparkConf()
    val inputFile = "/Users/chunyangshen/mini.tsv"
    val top = 100
    val numIterations = 10
    val outputFile = "/Users/chunyangshen/output/"

    sparkConf.setMaster("local[1]") setAppName("Wikipedia_Pagerank_2")
    val sc = new SparkContext(sparkConf)
    //val inputFile = inputFile
    val input = sc.textFile(inputFile)
    val partitioner = new HashPartitioner(sc.defaultParallelism)
    //input.foreach(println)
    val articles = dataExtract(input, partitioner)
    //articles.collect.foreach(println)
    val n = articles.count()
    println(n)
    val defaultRank = 1.0
    val a = 0.15
    val rank = pageRank(articles,numIterations,defaultRank,a,n,partitioner)
    val result = formatResult(rank)
    sc.parallelize(result.collect).saveAsTextFile(outputFile)
  }

  def formatResult (input:RDD[(String, Double)]):RDD[String] = {
    val result=input.sortBy(_._2,false).map{
      case (title, rank) =>"%-70s %5.10f "
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
      val linksArray = linksXML.map(link => link.text.toString).toArray
      (title,linksArray)
    }).partitionBy(partitioner).cache()
    output
  }


  def pageRank(
                links : RDD[(String, Array[String])],
                numIterations: Int,
                defaultRank: Double,
                a: Double,
                n: Long,
                partitioner: Partitioner)
  : RDD[(String, Double)] ={
    var ranks = links.mapValues { edges => defaultRank }
    for (i <- 1 to numIterations) {
      val contribs=links.groupWith(ranks).flatMap{
        case (id, (destineyComb, rankComb)) =>
          val destineyIter = destineyComb.iterator
          val rankIter = rankComb.iterator

          if(destineyIter.hasNext){
            val destineyArray = destineyIter.next
            var rank = a
            if (rankIter.hasNext) {
              rank = rankIter.next
            }
            destineyArray.map(destiney => (destiney, rank/destineyArray.size))
          }
          else
            Array[(String, Double)]()
      }
      ranks = (contribs.combineByKey((x: Double) => x,
        (x: Double, y: Double) => x + y,
        (x: Double, y: Double) => x + y,
        partitioner)
        .mapValues(sum => a + (1-a)*sum))
    }
    ranks
  }

}
