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
  val lines = sc.textFile("./urldata.txt") // read text file into Dataset[String] -> RDD1
  val pairs = lines.map { s =>
    val parts = s.split("\\s+") // Splits a line into an array of 2 elements according space(s)
    (parts(0), parts(1))
  }

  val links = pairs.distinct().groupByKey().cache() // RDD1 <string, string> -> RDD2<string, iterable>
  val urlRank = links.mapValues(v => 1.0)

  for (i <- 1 to iters) {
    val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
      val size = urls.size
      urls.map(url => (url, rank / size))
    }
    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
  }

  val output = ranks.collect()
  output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

  spark.stop()

}
