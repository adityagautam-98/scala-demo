import java.io.FileNotFoundException
import scala.collection.mutable.ArrayBuffer
import scala.io.Source


object movieAssignment {
  case class Movie(imdbTitle: String, title: String, originalTitle: String, year: Int, datePublished: String,
                   genre: List[String], duration: Int, country: List[String], language: List[String],
                   director: List[String], writer: List[String], productionCompany: String, actor: List[String],
                   description: String, avgVote: Double, vote: Int, budget: String, usaGross: String,
                   worldGross: String, metaScore: Int, userReview: Int, criticsReview: Int) {
  }




  def readCSVLine(source: Source): List[String] = {
    var c = if (source.hasNext) source.next else ' '
    var ret = List[String]()
    var inQuotes = false
    var current = ""
    while (source.hasNext && c != 13) {
      if (c == ',' && !inQuotes) {
        ret ::= current
        current = ""
      }
      else if (c == '\"') {
        inQuotes = !inQuotes
      }
      else if (c == '\\') {
        current += source.next
      }
      else {
        current += c
      }
      c = source.next
    }
    ret ::= current.trim
    ret.reverse.toList
  }

  def buildMovieData( movieData: =>ArrayBuffer[Movie], column: List[String]) = { // call by name to retain movieData

    movieData += Movie(column(0),
      column(1),
      column(2),
      if (column(3) == "") 0 else column(3).toInt,
      column(4),
      column(5).split(", ").toList,
      if (column(6) == "") 0 else column(6).toInt,
      column(7).split(", ").toList,
      column(8).split(", ").toList,
      column(9).split(", ").toList,
      column(10).split(", ").toList,
      column(11),
      column(12).split(", ").toList,
      column(13),
      if (column(14) == "") 0 else column(14).toDouble,
      if (column(15) == "") 0 else column(15).toInt,
      column(16),
      column(17),
      column(18),
      if (column(19) == "") 0 else column(19).toInt,
      if (column(20) == "") 0 else column(20).toInt,
      if (column(21) == "") 0 else column(21).toInt)
    movieData
  }


  def moviesByDirectorForDuration( movieData:ArrayBuffer[Movie], director: String, fromYear: Int, tillYear: Int) = {
    val selectedMovies = movieData.filter(_.director.contains(director)).filter(_.year >= fromYear).filter(_.year <= tillYear)
    println("Result for Query on Directors and Year \n")
    println("IMDB NO. | Director |     Title     |     Original Title    | Year | Date Published | Genre | Duration | Country | Language | Writer | Production | Actors | Description")
    for (i <- selectedMovies) {
      println(s"${i.imdbTitle} | ${i.director} | ${i.title} | ${i.originalTitle} | ${i.year} | ${i.year} | ${i.datePublished} | ${i.genre} | ${i.duration} | ${i.country} | ${i.language} | ${i.writer} | ${i.productionCompany} | ${i.actor} | ${i.description}")
    }

    selectedMovies
  }


  def moviesWithUserReviews( movieData:ArrayBuffer[Movie],minReview: Int) = {
    val selectedMovies = movieData.filter(_.userReview > minReview).sortBy(_.userReview).reverse// to print in descending order
    println("Result for filter on User Reviews \n")
    println("S.No. | IMDB NO. | User Reviews |     Title    | Year | Date Published | Genre | Duration | Country | Language | Writer | Actors | Description")
    var count = 0
    for (i <- selectedMovies) {
      count += 1
      println(s"${count}" + s" | ${i.imdbTitle} | ${i.userReview} | ${i.title} | ${i.year} | ${i.year} | ${i.datePublished} | ${i.genre} | ${i.duration} | ${i.country} | ${i.language} | ${i.writer} | ${i.actor} | ${i.description}")
    }
    selectedMovies

  }

  def longestDurationByCountryAndMinVotes( movieData:ArrayBuffer[Movie],country: String, minimumVotes: Int) = {
    val selectedMovies = movieData.filter(_.country.contains(country)).filter(_.vote >= minimumVotes).sortBy(_.duration).reverse
    println("Result for filter on County and minimum votes \n")
    println("S.No. | IMDB NO. | Country | Duration  |  User Reviews  |     Title    | Year | Date Published | Genre | Language | Writer | Actors | Description")
    var count = 0
    for (i <- selectedMovies) {
      count += 1
      println(s"${count}" + s" | ${i.imdbTitle} | ${i.country} | ${i.duration} | ${i.userReview} | ${i.title} | ${i.year} | ${i.year} | ${i.datePublished} | ${i.genre} | ${i.language} | ${i.writer} | ${i.actor} | ${i.description}")

    }
    selectedMovies
  }

  def main(args: Array[String]): Unit = {
    val movieData = ArrayBuffer[Movie]()
    var count = 0;
    try {
      val source = Source.fromFile("src/movies_dataset.csv")
      val lines = source.getLines().drop(1)
      while (lines.hasNext && count < 10000) { // to only load 10000 records
        val eachLine = readCSVLine(source)
        buildMovieData(movieData, eachLine)
        count += 1
      }
      moviesByDirectorForDuration(movieData,"Francesco Bertolini", 1900, 2100)
      moviesWithUserReviews(movieData,50)
      longestDurationByCountryAndMinVotes(movieData,"Australia", 10)
    }
    catch {
      case e: FileNotFoundException => println("File not found, please verify the location.")
      case e: NumberFormatException => println("Unexpected datatype found, can't convert to number.")
    }

  }

}




