package com.sparkProject

import org.apache.spark.sql.SparkSession


// Evaluation : quelques QCM dans le test d'Olivier, et aussi un TP à faire dans la continuité
// de celui-ci

/*

1. Compiler et créer un jar
Dans un terminal, aller là où se trouve le fichier build.sbt du projet puis assembler :
> cd /cal/homes/dbensoussan/Cours/Spark/tp_spark
> sbt assembly
L’adresse où se trouve le jar est indiquée à la fin de l'exécution de la commande.

2. Pour soumettre le script, aller là où le spark-submit se trouve et compiler le jar:
> cd /cal/homes/dbensoussan/spark-2.0.0-bin-hadoop2.7/bin/
> ./spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir="/tmp"
--driver-memory 3G --executor-memory 4G --class com.sparkProject.JobML
/cal/homes/dbensoussan/Cours/Spark/tp_spark/target/scala-2.11/tp_spark-assembly-1.0.jar

*/

object Job {

  def main(args: Array[String]): Unit = {

    // SparkSession configuration
    val spark = SparkSession
      .builder
      //.master("local")
      .appName("spark session TP_parisTech")
      .getOrCreate()

    val sc = spark.sparkContext
    val df = spark
      .read // returns a DataFrameReader, giving access to methods “option” and “csv”
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("comment", "#")
      .csv("/cal/homes/dbensoussan/Cours/Spark/tp_spark/cumulative.csv")

    // df.columns returns an Array of columns names,
    // and arrays have a method “length” returning their length
    println("number of columns", df.columns.length)
    println("number of rows", df.count)

    import org.apache.spark.sql.functions._
    // In scala arrays have a method “slice” returning a slice of the array
    //val columns = df.columns.slice(10, 20)

    // La méthode select prend autant de paramètres que de noms de colonnes qu'on veut afficher
    // C'est pour ça qu'on utilise map qui va écrire col1, col2 ... à partir de l'array
    //df.select(columns.map(col): _*).show(50)

    // Nom des colonnes et types de données contenues dans le df
    // df.printSchema()

    import spark.implicits._

    // Nombre d'éléments de chaque type de disposition
    //df.groupBy($"koi_disposition").count().show()

    // Conserver uniquement les lignes "CONFIRMED ou FALSE POSITIVE"
    // Liste de colonnes à retirer
    val liste = List("koi_eccen_err1", "index", "kepid", "koi_fpflag_nt", "koi_fpflag_ss", "koi_fpflag_co",
      "koi_fpflag_ec", "koi_sparprov", "koi_trans_mod", "koi_datalink_dvr", "koi_datalink_dvs",
      "koi_tce_delivname", "koi_parm_prov", "koi_limbdark_mod", "koi_fittype", "koi_disp_prov",
      "koi_comment", "kepoi_name", "kepler_name", "koi_vet_date", "koi_pdisposition")

    val df2 = df.filter($"koi_disposition" =!= "CANDIDATE").drop(liste: _*)
    //df2.groupBy($"koi_disposition").count().show()
    //println("df2 number of columns", df2.columns.length)
    // df2.select(columns.map(col): _*).show(20)

    println("Dropping useless columns...")
    val useless = for(col <- df2.columns if df2.select(col).distinct().count() <= 1 ) yield col
    //val useless = df2.columns.filter{ case (column:String) => df2.agg(countDistinct(column)).first().getLong(0) <= 1 }
    val df3 = df2.drop(useless: _*)

    println("df3 number of columns", df3.columns.length)
    df3.describe("koi_disposition").show()

// 5.
    val df4 = df3.na.fill(0.0)

// 6.
    val df_l = df4.select("rowid", "koi_disposition")
    val df_f = df4.drop("koi_disposition")

    // Jointure des deux tables
    val df_j = df_f.join(df_l, usingColumn = "rowid")

// 7.
    // UDF
    def udf_sum = udf((col1: Double, col2: Double) => col1 + col2)

    val df5 = df4
      .withColumn("koi_ror_min", udf_sum($"koi_ror", $"koi_ror_err2"))
      .withColumn("koi_ror_max", $"koi_ror" + $"koi_ror_err1")

// 8.
    df5
      .coalesce(1) // optional : regroup all data in ONE partition, so that results are printed in ONE file
      // >>>> You should not that in general, only when the data are small enough to fit in the memory of a single machine.
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("/cal/homes/dbensoussan/Cours/Spark/tp_spark/cleanedDataFrame.csv")



  }


}

//    ----------------- word count ------------------------
//    val df_wordCount = sc.textFile("/cal/homes/dbensoussan/spark-2.0.0-bin-hadoop2.7/README.md")
//      .flatMap{case (line: String) => line.split(" ")}
//      .map{case (word: String) => (word, 1)}
//      .reduceByKey{case (i: Int, j: Int) => i + j}
//      .toDF("word", "count")
//
//    df_wordCount.orderBy($"count".desc).show()
//    ----------------- word count ------------------------