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

object Main {

  val conf: SparkConf = new SparkConf().setAppName("Scala spells").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    //Mongodb connect
    val uri: String = "mongodb+srv://root:root@cluster0.v4hqy.mongodb.net/scrapping?retryWrites=true&w=majority"
    System.setProperty("org.mongodb.async.type", "netty")
    val client: MongoClient = MongoClient(uri)
    //Mongodb database
    val db: MongoDatabase = client.getDatabase("scrapping")
    //Mongodb collection
    val coll: MongoCollection[Document] = db.getCollection("spell")

    //Future results from Mongodb
    val docs: ArrayBuffer[Document] = ArrayBuffer()

    //Mongodb request
    val observable: FindObservable[Document] = coll.find()
    observable.subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = docs.append(result)

      override def onError(e: Throwable): Unit = println("Error")

      override def onComplete(): Unit = println("Finished")
    })
    //Wait for results (10s max)
    Await.result(observable.toFuture(), Duration(10, SECONDS))

    //spark_filter(docs)
    spark_sql(docs)

  }

  def spark_sql(docs: ArrayBuffer[Document]): Unit = {
    val session: SparkSession = SparkSession.builder().getOrCreate()

    //Simplify data fro SQL requests
    val new_docs: ArrayBuffer[Document] = new ArrayBuffer()
    for (doc <- docs) {
      val new_doc: Document = new Document()

      new_doc.put("title", doc.get("title"))

      val classes: util.ArrayList[Document] = doc.get("classes").asInstanceOf[util.ArrayList[Document]]
      val it = classes.iterator()
      val cl_list: util.ArrayList[String] = new util.ArrayList[String]()
      while (it.hasNext) {
        val doc_class = it.next()
        if(doc_class.get("class").asInstanceOf[String].nonEmpty)
          cl_list.add(doc_class.get("class").asInstanceOf[String] + " " + doc_class.get("level").asInstanceOf[Int].toString)
      }
      new_doc.put("levels", cl_list)

      val cast_list = doc.get("casting").asInstanceOf[util.ArrayList[Document]]
      if (cast_list != null && cast_list.size() > 1) {
        val components = cast_list.get(1).get("value")
        new_doc.put("components", components)
      } else {
        new_doc.put("components", new util.ArrayList[String]())
      }

      new_docs.append(new_doc)
    }

    println("NON FORMATTED")
    println(docs(1).toJson)
    println("EXAMPLE OF DOC REFORMATTED")
    println(new_docs(1).toJson)


    var docs_str_json: String = ""
    new_docs.foreach(docs_str_json += _.toJson + "\n")

    import session.implicits._
    val sql_df: DataFrame = session.read.json(docs_str_json.split("\n").toSeq.toDS())

    sql_df.printSchema()
    sql_df.show(10)

    sql_df.createOrReplaceTempView("spells")

    val res = session.sql("SELECT title FROM spells " +
      "WHERE (" +
      "array_contains(levels, 'sorcerer 4') " +
      "OR array_contains(levels, 'sorcerer 3') " +
      "OR array_contains(levels, 'sorcerer 2') " +
      "OR array_contains(levels, 'sorcerer 1')" +
      "OR array_contains(levels, 'sorcerer 0')" +
      ")" +
      "AND components='V'"
    )
    res.show(40)
    println(res.count())
  }

  def spark_filter(docs: ArrayBuffer[Document]): Unit = {
    //SPARK RDD creation (documents)
    val rdd: RDD[Document] = sc.makeRDD(docs)

    def filter_fn(doc: Document): Boolean = {
      val classes: util.ArrayList[Document] = doc.get("classes").asInstanceOf[util.ArrayList[Document]]
      val it = classes.iterator()
      var class_ok = false
      breakable {
        while (it.hasNext) {
          val doc_class = it.next()
          if (doc_class.get("class").asInstanceOf[String].equalsIgnoreCase("sorcerer")) {
            if (doc_class.get("level").asInstanceOf[Int] <= 4) {
              class_ok = true
              break
            }
          }
        }
      }
      if (!class_ok)
        return false
      val cast_list = doc.get("casting").asInstanceOf[util.ArrayList[Document]]
      if (cast_list == null)
        return false
      if (cast_list.size() <= 1)
        return false
      val components = cast_list.get(1).get("value")
      components.isInstanceOf[String] && components.asInstanceOf[String].equalsIgnoreCase("v")
    }

    val res: Array[Document] = rdd.filter(filter_fn).collect()
    println(res.length)
  }

  /*  def test(nbrDecimales : Int = 1000) : Unit = {
      val slices = nbrDecimales
      val n = math.min(100000L * slices, Int.MaxValue).toInt
      val count = sc.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x*x + y*y <= 1) 1 else 0
      }.reduce(_ + _)
      println("Pi is roughly " + 4.0 * count / (n - 1))
    }*/

}
