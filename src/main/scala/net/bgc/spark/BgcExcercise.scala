package net.bgc.spark

import scala.math.random

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.collection.immutable.ListMap
import org.apache.spark.storage.StorageLevel
import com.typesafe.config.ConfigFactory
import java.io.File
import java.io.PrintWriter

object BgcExcercise {
  val minimumOfFiftyVotesFilter = (votes: Int) => votes > 49

  val movieTypeFilter = (titletype: String) => titletype == "movie"

  def main(args: Array[String]) {
    val config = ConfigFactory.load("application.conf").getConfig("net.bgc.spark")

    val spark = SparkSession
      .builder
      .master(config.getString("master"))
      .appName(config.getString("app_name"))
      .getOrCreate()

    val partitions = config.getInt("num_partitions")
    val outputDir = config.getString("output_dir")

    val ratingsRdd = spark.sparkContext.textFile(config.getString("title.ratings.tsv"), partitions)
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter };

    val ratings = getRatings(ratingsRdd, minimumOfFiftyVotesFilter)

    val moviesRdd = spark.sparkContext.textFile(config.getString("title.basics.tsv"), partitions)

    val movies = getMovies(moviesRdd, movieTypeFilter)

    val topMovies = getTopMovies(movies, ratings, 20)

    saveToTextfile(outputDir + "/top-movies.txt", topMovies)

    val namesRdd = spark.sparkContext.textFile(config.getString("name.basics.tsv"), partitions)

    val principalsRdd = spark.sparkContext.textFile(config.getString("title.principals.tsv"), partitions)

    val mostCreditedPersons = getMostOftenCredited(principalsRdd, namesRdd, topMovies)

    saveToTextfile(outputDir + "/often-credited-persons.txt", mostCreditedPersons)

    val aliasRDD = spark.sparkContext.textFile(config.getString("title.akas.tsv"), partitions)

    val aliasTitleOfMovies = getAliasOfTopMovietitles(aliasRDD, topMovies)
    
    new PrintWriter(outputDir + "/title-akas.txt") { aliasTitleOfMovies foreach println; close }

    spark.stop()
  }

  def getAliasOfTopMovietitles(alias: RDD[String], movies: Array[Array[String]]): Array[String] = {
    alias.map(_.split("\t"))
      .map(p => (p(0), p(2)))
      .filter(f => movies.map(m => m(0)).contains(f._1))
      .groupByKey
      .collect()
      .map(f => (f._1 + "\t" + f._2.toList.distinct.mkString(",")))
  }

  def getMostOftenCredited(principals: RDD[String], names: RDD[String], movies: Array[Array[String]]): Array[Array[String]] = {
    val principalsRDD = principals.map {r => val p = r.split("\t");(p(0), p(2))}
    principalsRDD.cache()

    val personsIds = principalsRDD.filter(f => movies.map(m => m(0)).contains(f._1))
      .map(_._2)
      .collect()

    val cntByValue = principalsRDD.filter(f => personsIds.contains(f._2)).map(f => f._2).countByValue()

    names.map(_.split("\t"))
      .filter(f => personsIds.contains(f(0)))
      .sortBy(f => cntByValue.get(f(0)), false)
      .collect()
  }

  def getTopMovies(basics: RDD[(String, Array[String])], ratings: RDD[(String, Double)], noOfResults: Int): Array[Array[String]] = {
    basics.join(ratings).map(row => (row._2))
      .top(noOfResults)(Ordering.by[(Array[String], Double), Double](_._2))
      .map(_._1)
  }

  def getMovies(basics: RDD[String], predicate: String => Boolean): RDD[(String, Array[String])] = {
    basics.map { movie =>
      val props = movie.split("\t")
      (props(0), props)
    }
      .filter(mv => predicate(mv._2(1)))
  }

  def getRatings(ratings: RDD[String], filter: Int => Boolean): RDD[(String, Double)] = {
    ratings.map(_.split("\t"))
      .filter(rating => filter(rating(2).toInt))
      .map(rating => (rating(0), rating(1).toDouble * rating(2).toInt))
  }

  private def saveToTextfile(fileName: String, contents: Array[Array[String]]): Unit = {
    new PrintWriter(fileName) {
      contents.foreach(line => println(line.mkString("\t")))
      close
    }
  }
  
}



