package quantexa.interview

import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FlatSpec

@RunWith(classOf[JUnitRunner])
class QuantexaInterviewSourcesTest extends FlatSpec {
  val spark: SparkSession = SparkSession.builder().config("spark.master", "local").enableHiveSupport().getOrCreate()

  "getSourceData" should "read and create a dataframe for the PASSANGERS table" in {
    val expectedResults: Set[Row] = Set(
      Row("1", "firstName1", "lastName1"),
      Row("2", "firstName2", "lastName2"),
      Row("3", "firstName3", "lastName3")
    )

    val expectedColumns: List[String] = List("passengerId","firstName","lastName")
    val pathToTest = "/passangers_test.csv"

    val results = QuantexaInterviewSources.getSourceData(pathToTest, spark, "passangers")

    assert( expectedResults.size == results.count() )
    assert( expectedResults == results.collect().toSet )
    assert( expectedColumns == results.columns.toList )
  }

  it should "read and create a dataframe for the FLIGHTS table" in {
    val expectedResults: Set[Row] = Set(
      Row("1", "1", "fromTest1", "toTest1", "2020-01-01"),
      Row("2", "1", "fromTest1", "toTest1", "2020-01-01"),
      Row("3", "1", "fromTest1", "toTest1", "2020-01-01")
    )

    val expectedColumns: List[String] = List("passengerId", "flightId", "from", "to", "date")
    val pathToTest = "/flightData_test.csv"

    val results = QuantexaInterviewSources.getSourceData(pathToTest, spark, "flights")

    assert( expectedResults.size == results.count() )
    assert( expectedResults == results.collect().toSet )
    assert( expectedColumns == results.columns.toList )
  }

}
