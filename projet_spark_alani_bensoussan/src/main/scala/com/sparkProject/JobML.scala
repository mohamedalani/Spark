package com.sparkProject

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

/**
  * Mohamed Al Ani et Davy Bensoussan
  */

object JobML {

  def main(args: Array[String]): Unit = {

    val project_path = "/cal/homes/dbensoussan/Cours/Spark/tp_spark/"
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
      .parquet(project_path + "cleanedDataFrame.parquet")

    val temp = df.drop("koi_disposition", "rowid")

    // Définition du vecteur de features et des labels
    val features = new VectorAssembler()
      .setInputCols(temp.columns)
      .setOutputCol("features")
    val labels = new StringIndexer()
      .setInputCol("koi_disposition")
      .setOutputCol("label")

    // Ajout sur le dataframe de base
    val indexed = labels.fit(features.transform(df)).transform(features.transform(df))

    // On définit l'ensemble d'apprentissage et les données
    // qui serviront à évaluer la performance du modèle
    val Array(train, test) = indexed.randomSplit(Array(.9, .1))

    val lr = new LogisticRegression()
      .setElasticNetParam(1.0)  // L1-norm regularization : LASSO
      .setLabelCol("label")
      .setStandardization(true)  // to scale each feature of the model
      .setFitIntercept(true)  // we want an affine regression (with false, it is a linear regression)
      .setTol(1.0e-5)  // stop criterion of the algorithm based on its convergence
      .setMaxIter(300)  // a security stop criterion to avoid infinite loops

    // On crée la grille d'hyperparamètres à évaluer
    // regParam est le paramètre de régularisation du modèle
    val paramGrid = new ParamGridBuilder()
      // On crée la grille de 10^-6 à 10^0 par puissance de .5
      .addGrid(lr.regParam, for (x <- -12 to 0) yield math.pow(10, x / 2f))
      .build()


    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new BinaryClassificationEvaluator())
      .setEstimatorParamMaps(paramGrid)
      // 70% des données sont utilisées pour l'apprentissage, 30% pour la validation
      .setTrainRatio(0.7)

    // Le modèle choisi est celui dont les paramètres donnent la meilleure performance
    // Puis effectue la prédiction sur les données de test
    val prediction = trainValidationSplit.fit(train).transform(test)

    // Affichage de la matrice de confusion
    prediction.groupBy("label", "prediction").count.show()

    println("Taux de prédictions correctes :")
    println(prediction.filter("label = prediction").count() / prediction.count().toFloat)

    // Sauvegarde du modèle
    trainValidationSplit
      .write
      .overwrite()
      .save(project_path + "log_reg_model")
  }
}