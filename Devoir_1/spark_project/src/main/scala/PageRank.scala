import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document
import org.mongodb.scala.{FindObservable, MongoClient, MongoCollection, MongoDatabase, Observer}

import java.util
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.control.Breaks.{break, breakable}


object PageRank {

  val conf: SparkConf = new SparkConf().setAppName("Scala pagerank").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val iter = 20
  val dumpFactor = 0.85

  val pairs = sc.textFile("./urldata.txt").map { s =>
    val urls = s.split("\\s+") // Splits a line into an array of 2 elements according space(s)
    (urls(0), urls(1))
  }

  val links = pairs.distinct().groupByKey().cache() // RDD1 <string, string> -> RDD2<string, iterable>
  val urlRank = links.mapValues(v => 1.0)

  for (i <- 1 to iters) {
    println("Iteration : "+i)
    val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
      val size = urls.size
      urls.map(url => (url, rank / size))
    }

    urlRank = contribs.reduceByKey(_ + _).mapValues(x => (1 - dumpFactor) + dumpFactor * x)
    urlRank.take(10).foreach(println)
  }

  urlRank.sortBy(_._2, false)
  urlRank.saveAsTextFile("result")

  spark.stop()

}
