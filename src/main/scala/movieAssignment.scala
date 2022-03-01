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

  var movieData = ArrayBuffer[Movie]()


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

  def buildMovieData(column: List[String]) = {
    var year = 0;
    var duration = 0;
    var averageVote = 0
    var votes = 0
    var metaScore = 0
    var userReviews = 0
    var criticsReviews = 0

    try {
      year = column(3).toInt
      duration = column(6).toInt
      averageVote = column(14).toInt
      votes = column(15).toInt
      metaScore = column(19).toInt
      userReviews = column(20).toInt
      criticsReviews = column(21).toInt

    }
    catch {
      case e: FileNotFoundException => println("The file can't be located. Please check the file path.")
      case e: NumberFormatException => println("Unexpected datatype found, can't covert to required format.")

    }
    try {
      movieData += Movie(column(0),
        column(1),
        column(2),
        year,
        column(4),
        column(5).split(", ").toList,
        duration,
        column(7).split(", ").toList,
        column(8).split(", ").toList,
        column(9).split(", ").toList,
        column(10).split(", ").toList,
        column(11),
        column(12).split(", ").toList,
        column(13),
        averageVote,
        votes,
        column(16),
        column(17),
        column(18),
        metaScore,
        userReviews,
        criticsReviews)
    }
    catch {
      case e: FileNotFoundException => println("Please check your file name and Location")
      case e: NumberFormatException => println("Caught Number Format Exception: Cannot convert to needed format")
    }
    movieData
  }


  def moviesByDirectorForDuration(director: String, fromYear: Int, tillYear: Int) = {
    val result = movieData.filter(_.director.contains(director)).filter(_.year >= fromYear).filter(_.year <= tillYear)
    println("Result for Query \n")
    println("IMDB NO. | Director |     Title     |     Original Title    | Year | Date Published | Genre | Duration | Country | Language | Writer | Production | Actors | Description")
    for (i <- result) {
      println(s"${i.imdbTitle} | ${i.director} | ${i.title} | ${i.originalTitle} | ${i.year} | ${i.year} | ${i.datePublished} | ${i.genre} | ${i.duration} | ${i.country} | ${i.language} | ${i.writer} | ${i.productionCompany} | ${i.actor} | ${i.description}")
    }
    result
  }


  def main(args: Array[String]): Unit = {
    var count = 0;
    val source = Source.fromFile("src/movies_dataset.csv")
    source.getLines().drop(1)
    while (source.hasNext && count < 10000) { // to only load 10000 records
      val eachLine = readCSVLine(source)
      buildMovieData(eachLine)
      count += 1
    }
    moviesByDirectorForDuration("Francesco Bertolini", 1900, 2100)

  }

}




