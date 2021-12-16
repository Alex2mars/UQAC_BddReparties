import org.apache.spark.sql.{KeyValueGroupedDataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object main {

  val conf: SparkConf = new SparkConf().setAppName("Scala spells").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder().getOrCreate()

    import session.implicits._

    val df = session.read.json("src/main/scala/beast-spell.json")

    println("Taille des données :", df.count())
    df.printSchema()
    df.show(20)

    def flat_map(row: Row): ArrayBuffer[(String, String)] = {
      val res: ArrayBuffer[(String, String)] = ArrayBuffer()

      val creature_name: String = row.getString(1)
      if(creature_name.isEmpty)
        return ArrayBuffer()

      val seq: Seq[String] = row.getSeq(2).asInstanceOf[Seq[String]]
      if(seq.length < 1)
        return ArrayBuffer()

      for(spell <- seq) {
        if(spell.nonEmpty)
          res.append((spell, creature_name))
      }
      println(res)
      res
    }
    val flatmap_res = df.flatMap(flat_map)
    // Suppression des doublons
    val flatmap_res_distinct = flatmap_res.distinct().toJavaRDD.rdd
    val group_by = flatmap_res_distinct.groupByKey().mapValues(iterable => iterable.toList)
    // Ecriture du fichier
    group_by.toDF("Sort", "Liste des créatures").coalesce(1).write.json("src/main/scala/spells.json")

  }

}
