import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_extract

import scala.language.postfixOps

object IMDBRatings {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MoviesRating")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val MoviesFile = "IMDBDataSet.csv"

    val MoviesDF = spark.read.format("CSV")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(MoviesFile)

    MoviesDF.printSchema()
    MoviesDF.show()

    //top movies and shows
    println("top movies and shows\n")
    val topMovies = MoviesDF
      .select("movies_name","movies_rating","movies_genre","movies_tv_rating","movies_year")
      .orderBy(desc("movies_rating"))

    topMovies.show()

    //average rating by Age restriction
    print("average rating by Age restriction\n")
    val ratingByType = MoviesDF
      .select("movies_tv_rating","movies_rating")
      .groupBy("movies_tv_rating")
      .agg(avg("movies_rating").alias("average_rating"),count("movies_rating").alias("type_count"))
      .orderBy(desc("average_rating"))

    ratingByType.show()

    //latest movies and shows sorted by rating
    println("latest movies and shows sorted by rating\n")
    val latestMovies = MoviesDF
      .select(col("movies_name"),col("movies_rating"),col("movies_genre"),col("movies_tv_rating"),regexp_extract(col("movies_year"),".*([\\d]{4}).*",1).alias("theYear"))
      .where(col("movies_rating") isNotNull)
      .orderBy(desc("theYear"), desc("movies_rating"))

    latestMovies.show()

    val currentYear = "2022"
    //number of new shows sorted by year
    println("number of new shows sorted by year\n")
    val numberShows = MoviesDF
      .select(col("movies_year"), regexp_extract(col("movies_year"),".*([\\d]{4}).*",1).alias("theYear"))
      .where(regexp_extract(col("movies_year"),".*([\\d]{4}).*",1).leq(currentYear))
      .groupBy("theYear")
      .agg(count("*").alias("number_of_shows"))
      .orderBy(desc("theYear"))

    numberShows.show()

    //getting shows By specific genre sorted by rating
    println("getting shows By specific genre sorted by rating\n")
    val genre = "animation"
    val showsByGenre = MoviesDF
      .select(col("movies_name"), col("movies_genre"), col("movies_rating"))
      .where(col("movies_genre") rlike s"(?i)$genre")
      .orderBy(desc("movies_rating"))

    showsByGenre.show()

    spark.stop()

  }


}