import org.apache.spark.sql._
import org.apache.log4j._

import scala.language.postfixOps

object IMDBRatingsSQL {

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MoviesRating")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val MoviesFile = "IMDBDataSet.csv"

    val MoviesDF = spark.read.format("CSV")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(MoviesFile)

    MoviesDF.printSchema()
    MoviesDF.show()

    //creating temporary view table named Moviestbl
    MoviesDF.createOrReplaceTempView("Moviestbl")

    //top movies and shows
    println("top movies and shows\n")
    val topMoviesResult = spark.sql("select movies_name, movies_rating " +
                "from Moviestbl order by movies_rating desc")
    topMoviesResult.show()

    //average rating by Age restriction
    print("average rating by Age restriction\n")
    val ratingByTypeResults = spark.sql("select movies_tv_rating, avg(movies_rating) as average_rating, " +
      "count(movies_rating) as type_count " +
      "from Moviestbl group by movies_tv_rating order by average_rating desc ")
    ratingByTypeResults.show()

    //latest movies and shows sorted by rating
    println("latest movies and shows sorted by rating\n")
    val latestMoviesResults = spark.sql("select movies_name, movies_rating, regexp_extract(movies_year,\".*([\\\\d]{4}).*\") as theYear " +
      "from Moviestbl where movies_rating is Not Null order by theYear desc, movies_rating desc")
    latestMoviesResults.show()

    //number of new shows sorted by year
    println("number of new shows sorted by year\n")
    val numberOfShowsResult = spark.sql("select regexp_extract(movies_year,\".*([\\\\d]{4}).*\",1) as theYear, count(*) as number_of_shows " +
      "from Moviestbl where regexp_extract(movies_year,\".*([\\\\d]{4}).*\",1) <= 2022 " +
      "group by theYear order by theYear desc")
    numberOfShowsResult.show()

    //getting shows By specific genre sorted by rating
    println("getting shows By specific genre sorted by rating\n")
    val genre = "animation"

    val showsByGenreResults = spark.sql("select movies_name, movies_genre, movies_rating " +
      s"from Moviestbl where movies_genre rlike '(?i)$genre' order by movies_rating desc")
    showsByGenreResults.show()

    spark.stop()

  }


}