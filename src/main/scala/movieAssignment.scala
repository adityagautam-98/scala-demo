import org.apache.spark.sql.SparkSession
case class movieAssignment(imdbTitle: String, title: String, originalTitle: String, year: Int, datePublished: String,
                      genre: List[String], duration: Int, country: List[String], language: List[String],
                      director: List[String], writer: List[String], productionCompany: String, actors: List[String],
                      description: String, averageVote: Double, votes: Int, budget: String, usaGross: String,
                      worldWideGross: String, metaScore: Int, userReviews: Int, criticsReviews: Int) {
}

object main extends App {
  val sparkSession = SparkSession
    .builder()
    .appName("Spark SQL basic example").config("spark.master", "local")
    .getOrCreate()

  val movieData = sparkSession.read.option("header", "true")
    .csv("C:\\My-Projects\\Scala-Projects\\scala-demo\\src\\movies_dataset.csv")// removing header,and applying schema


  println(movieData.limit(100))
  val movieList = movieData.limit(10000)

}



