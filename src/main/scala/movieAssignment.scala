import org.apache.spark.sql.{Dataset, SparkSession}

case class Movie(imdb_title_id: String, title: String, original_title: String, year: Int, date_published: String,
                 genre: String, duration: Int, country: String, language: String,
                 director: String, writer: String, production_company: String, actors: String,
                 description: String, avg_vote: String, votes: String, budget: String, usa_gross_income: String,
                 worlwide_gross_income: String, metascore: String, reviews_from_users: String, reviews_from_critics: String, _c22: String) {
}

object main extends App {
  val sparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example").config("spark.master", "local")
    .getOrCreate()

  val movieData = sparkSession.read.option("header", "true").option("inferSchema", true)
    .csv("C:\\My-Projects\\Scala-Projects\\scala-demo\\src\\movies_dataset.csv") // removing header,and applying schema
  movieData.printSchema()

  import sparkSession.implicits._

  println(movieData.limit(100))
  val movieList = movieData.limit(10000)
  val movies: Dataset[Movie] = movieList.as[Movie]

  println(movies)



}






