import movieAssignment.{Movie, buildMovieData, longestDurationByCountryAndMinVotes, moviesByDirectorForDuration, moviesWithUserReviews, readCSVLine}
import org.scalatest.funsuite.AnyFunSuite
import java.io.FileNotFoundException
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class moviesDataTest extends AnyFunSuite {
  val sampleData = ArrayBuffer(Movie("tt0047189", "Long John Silver", "Long John Silver", 1954, "16-12-54", List("Action", "Adventure", "Drama"), 106, List("Australia", "USA"), List("English"),
    List("Byron Haskin"), List("Martin Rackin"), "Treasure Island Pictures Pty. Ltd.", List("Robert Newton", "Connie Gilchrist", "Lloyd Berrell", "Grant Taylor", "Rod Taylor", "Harvey Adams", "Muriel Steinbeck",
      "Henry Gilbert", "John Brunskill", "Eric Reiman", "Harry Hambleton", "Syd Chambers", "George Simpson-Lyttle", "Tony Arpino", "Al Thomas"),
    "In this sequel to Treasure Island, Long John hopes to rescue his friend Jim from a rival pirate and return for more treasure.", 5.8, 620, "$1", "", "", 0, 16, 10),
    Movie("tt0010680", "The Sentimental Bloke", "The Sentimental Bloke", 1919, "04-10-19", List("Drama", "Romance"), 106, List("Australia"), List("English"), List("Raymond Longford"),
      List("C.J. Dennis", "Raymond Longford"), "Southern Cross Feature Film Company", List("Arthur Tauchert", "Lottie Lyell", "Gilbert Emery",
        "Stanley Robinson", "Harry Young", "Margaret Reid", "Charles Keegan", "William Coulter", "Helen Fergus", "C.J. Dennis"),
      "A down-at-heal ex-convict undergoes an epiphany as he experiences the love of a good woman for the first time.", 6.3, 225, "", "", "", 0, 11, 0))


  test("CSV File Read") {
    val source = Source.fromFile("src/movies_dataset.csv")
    source.getLines().drop(1)
    val reading: List[String] = readCSVLine(source)
    assert(reading.size > 0)
  }

  test("Storing data") {
    val sampleTest: List[String] = List("tt0000574",
      "The Story of the Kelly Gang", "The Story of the Kelly Gang", "1906"
      , "1906-12-26", "Biography, Crime, Drama", "70", "Australia,",
      " Charles Tait, Charles Tait, J. and N. Tait, Elizabeth Tait",
      "John Tait, Norman Campbell, Bella Cola, ", "Will Coyne, Sam Crewes,",
      " Jack Ennis, John Forde, Vera Linden, Mr. Marshall, Mr. McKenzie,",
      " Frank Mills, Ollie Wilson"
      , "True story of notorious Australian outlaw Ned Kelly (1855-80).", "6.1", "537", "$2",
      "250", "", "", "", "7", "7")
    val storing = buildMovieData(sampleData, sampleTest)
    assert(storing.size != 0)
  }

  test("File Not Found Exception") {
    assertThrows[FileNotFoundException] {
      val source = Source.fromFile("src/movie_dataset.csv")

    }
  }

  test("Number Format Exception") {
    assertThrows[NumberFormatException] {
      val sampleTest: List[String] = List("test",
        "test", "test", "test","test", "test", "test", "test",
        "test","test","test","test","test", "test", "test", "test", "test",
        "test", "test", "test", "test", "test", "test")
      val storing = buildMovieData(sampleData, sampleTest)
    }
  }

  test("Testing Movies with filter of director and given years") {

    val ExpectedData = ArrayBuffer(Movie("tt0047189", "Long John Silver", "Long John Silver", 1954, "16-12-54", List("Action", "Adventure", "Drama"), 106, List("Australia", "USA"), List("English"),
      List("Byron Haskin"), List("Martin Rackin"), "Treasure Island Pictures Pty. Ltd.", List("Robert Newton", "Connie Gilchrist", "Lloyd Berrell", "Grant Taylor", "Rod Taylor", "Harvey Adams", "Muriel Steinbeck",
        "Henry Gilbert", "John Brunskill", "Eric Reiman", "Harry Hambleton", "Syd Chambers", "George Simpson-Lyttle", "Tony Arpino", "Al Thomas"),
      "In this sequel to Treasure Island, Long John hopes to rescue his friend Jim from a rival pirate and return for more treasure.", 5.8, 620, "$1", "", "", 0, 16, 10))

    val function = moviesByDirectorForDuration(sampleData, "Byron Haskin", 1950, 1960)

    assert(function.equals(ExpectedData))

  }


  test("Movie with minimum review test") {

    val ExpectedData = ArrayBuffer(Movie("tt0047189", "Long John Silver", "Long John Silver", 1954, "16-12-54", List("Action", "Adventure", "Drama"), 106, List("Australia", "USA"), List("English"),
      List("Byron Haskin"), List("Martin Rackin"), "Treasure Island Pictures Pty. Ltd.", List("Robert Newton", "Connie Gilchrist", "Lloyd Berrell", "Grant Taylor", "Rod Taylor", "Harvey Adams", "Muriel Steinbeck",
        "Henry Gilbert", "John Brunskill", "Eric Reiman", "Harry Hambleton", "Syd Chambers", "George Simpson-Lyttle", "Tony Arpino", "Al Thomas"),
      "In this sequel to Treasure Island, Long John hopes to rescue his friend Jim from a rival pirate and return for more treasure.", 5.8, 620, "$1", "", "", 0, 16, 10))

    val function = moviesWithUserReviews(sampleData, 15)

    assert(function.equals(ExpectedData))
  }



  test("Longest duration by country and votes test") {

    val ExpectedData = ArrayBuffer(Movie("tt0047189", "Long John Silver", "Long John Silver", 1954, "16-12-54", List("Action", "Adventure", "Drama"), 106, List("Australia", "USA"), List("English"),
      List("Byron Haskin"), List("Martin Rackin"), "Treasure Island Pictures Pty. Ltd.", List("Robert Newton", "Connie Gilchrist", "Lloyd Berrell", "Grant Taylor", "Rod Taylor", "Harvey Adams", "Muriel Steinbeck",
        "Henry Gilbert", "John Brunskill", "Eric Reiman", "Harry Hambleton", "Syd Chambers", "George Simpson-Lyttle", "Tony Arpino", "Al Thomas"),
      "In this sequel to Treasure Island, Long John hopes to rescue his friend Jim from a rival pirate and return for more treasure.", 5.8, 620, "$1", "", "", 0, 16, 10))
    val function = longestDurationByCountryAndMinVotes(sampleData, "USA", 610)
    assert(function.equals(ExpectedData))

  }

}
