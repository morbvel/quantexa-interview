package quantexa.interview

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.junit.runner.RunWith
import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QuantexInterviewLogicTest extends FlatSpec{
  val spark: SparkSession = SparkSession.builder().config("spark.master", "local").enableHiveSupport().getOrCreate()
  import spark.implicits._

  "getMonthValueColumn" should "return the month value given the date" in {
    val inputDf = List(
      (1, "2020-01-01"),
      (2, "2020-02-01"),
      (3, "2020-12-01"),
      (4, "2020-07-01")
    ).toDF("id", "date")

    val result = inputDf.transform(QuantexaInterviewLogic.getMonthValueColumn)

    val columnToTest = "month"

    assert( "01" == getValueFromId[String](result, 1, columnToTest) )
    assert( "02" == getValueFromId[String](result, 2, columnToTest) )
    assert( "12" == getValueFromId[String](result, 3, columnToTest) )
    assert( "07" == getValueFromId[String](result, 4, columnToTest) )

  }

  "getFlightsPerMonth" should "return the actual number of flights per month" in {
    val inputDf = List(
      ("2020-01-01"),
      ("2020-01-01"),
      ("2020-07-01"),
      ("2020-07-01"),
      ("2020-07-01"),
      ("2020-10-01"),
      ("2020-12-01"),
      ("2020-12-01")
    ).toDF("date")

    val expectedResults = Set(
      Row("01", 2),
      Row("07", 3),
      Row("10", 1),
      Row("12", 2)
    )

    val results = inputDf.transform(QuantexaInterviewLogic.getFlightsPerMonth)

    assert(results.count == expectedResults.size)
    assert(results.collect().toSet == expectedResults)
  }

  "getPassengerswithMoreFlights" should "return the most frequent passangers" in {
    val passangers = List(
      ("1", "Passanger1", "LastName1"),
      ("2", "Passanger2", "LastName2"),
      ("3", "Passanger3", "LastName3"),
      ("4", "Passanger4", "LastName4")
    ).toDF("passengerId", "firstName", "lastName")

    val flights = List(
      ("1"), ("1"), ("1"), ("1"), ("2"), ("2"), ("3"), ("3"), ("3"), ("3"), ("3"), ("3"), ("1"), ("4"), ("4"), ("4")
    ).toDF("passengerId")

    val results = QuantexaInterviewLogic.getPassengerswithMoreFlights(passangers, flights)

    val idDf = List(
      (1, "1"), (2, "2"), (3, "3"), (4, "4")
    ).toDF("id", "passengerId")

    val finalResults = results.join(
      idDf, Seq("passengerId"), "left_outer"
    )

    val columnToTest = "Number of flights"

    assert( 6 == getValueFromId[Int](finalResults, 3, columnToTest) )
    assert( 5 == getValueFromId[Int](finalResults, 1, columnToTest) )
    assert( 3 == getValueFromId[Int](finalResults, 4, columnToTest) )
    assert( 2 == getValueFromId[Int](finalResults, 2, columnToTest) )
  }

  "getLongestRun" should "return the longest run for clients from the UK and so returning" in {
    val inputDf = List(
      ("1", "1", "UK", "Spain", "2020-01-01"),
      ("1", "2", "Spain", "Italy", "2020-01-02"),
      ("1", "3", "Italy", "China", "2020-01-03"),
      ("1", "4", "China", "Australia", "2020-01-04"),
      ("1", "5", "Australia", "Ireland", "2020-01-05"),
      ("1", "6", "Ireland", "Portugal", "2020-01-06"),
      ("1", "7", "Portugal", "UK", "2020-01-07"),
      ("1", "8", "UK", "USA", "2020-01-08"),
      ("1", "9", "USA", "Canada", "2020-01-09"),
      ("1", "10", "Canada", "UK", "2020-01-10"),
      ("1", "11", "UK", "Denmark", "2020-01-11"),
      ("1", "12", "Denmark", "Russia", "2020-01-12"),
      ("1", "13", "Russia", "Greece", "2020-01-13"),
      ("1", "14", "Greece", "Japan", "2020-01-14"),
      ("1", "14", "Japan", "UK", "2020-01-15"),
      ("2", "15", "UK", "Ireland", "2020-01-16"),
      ("2", "16", "Ireland", "Andorra", "2020-01-17"),
      ("2", "17", "Andorra", "UK", "2020-01-18")
    ).toDF("passengerId", "flightId", "from", "to", "date")

    val results = QuantexaInterviewLogic.getLongestRun(inputDf, spark)

    val idDataframe = List(
      (1, "1"), (2, "2")
    ).toDF("id", "passengerId")

    val finalResults = results.join(
      idDataframe, Seq("passengerId"), "left_outer"
    )

    val colToTest = "Longest run outside the UK"

    assert(5 == getValueFromId[Long](finalResults, 1, colToTest))
    assert(1 == getValueFromId[Long](finalResults, 2, colToTest))
  }

  "getPassengersInFlightsTogether" should "return passengers who have flight more than N times together" in{

    val flights: DataFrame = List(
      ("1", "1", "dummy"), ("1", "2", "dummy"), ("1", "3", "dummy"), ("1", "4", "dummy"),
      ("1", "5", "dummy"), ("1", "6", "dummy"), ("1", "7", "dummy"), ("1", "8", "dummy"), ("1", "9", "dummy"),
      ("2", "1", "dummy"), ("2", "2", "dummy"), ("2", "3", "dummy"), ("2", "4", "dummy"),
      ("3", "6", "dummy"), ("3", "7", "dummy"), ("3", "8", "dummy"), ("3", "9", "dummy")
    ).toDF("passengerId","flightId", "date")

    val results = QuantexaInterviewLogic.getPassengersInFlightsTogether(flights, 3)

    val groupedByPassenger = results
      .groupBy("passengerId").count()
      .withColumn("id", col("passengerId"))

    val colToTest = "count"
    // Number of people that each passenger has shared a flight more than 3 times
    assert( 2 == getValueFromId[Long](groupedByPassenger, 1, colToTest) )
    assert( 1 == getValueFromId[Long](groupedByPassenger, 2, colToTest) )
    assert( 1 == getValueFromId[Long](groupedByPassenger, 3, colToTest) )
  }

  "getPassangersInFlightsTogetherByDate" should "return passengers who have flight more than N times together in a date range" in {
    val flights = List(
      ("1", "1", "2020-01-04"), ("1", "2", "2020-01-06"), ("1", "3", "2020-01-08"),
      ("1", "4", "2020-01-10"), ("1", "5", "2020-01-12"), ("1", "6", "2020-01-14"),
      ("1", "7", "2020-01-16"),
      ("2", "1", "2020-01-04"), ("2", "2", "2020-01-06"), ("2", "3", "2020-01-08"),
      ("2", "4", "2020-01-10"), ("2", "5", "2020-01-12"),
      ("3", "4", "2020-01-10"), ("3", "5", "2020-01-12"), ("3", "6", "2020-01-14"),
      ("3", "7", "2020-01-16")
    ).toDF("passengerId", "flightId", "date")

    val results = QuantexaInterviewLogic
      .getPassengersInFlightsTogetherByDate(flights, 3, "2020-01-06", "2020-01-16")

    val groupedByPassenger = results
      .groupBy("passengerId").count()
      .withColumn("id", col("passengerId"))

    val numberOfFlights = "count"
    val startDate = "From"
    val endDate = "To"

    assert( 2 == getValueFromId[Long](groupedByPassenger, 1, numberOfFlights) )
    assert( 1 == getValueFromId[Long](groupedByPassenger, 2, numberOfFlights) )
    assert( 1 == getValueFromId[Long](groupedByPassenger, 3, numberOfFlights) )

    assert( "2020-01-10" == getValueFromId[String]
      (filterPassangersFlights(results, "1", "3"), 1, startDate)
    )
    assert( "2020-01-16" == getValueFromId[String]
      (filterPassangersFlights(results, "1", "3"), 1, endDate)
    )
    assert( "2020-01-06" == getValueFromId[String]
      (filterPassangersFlights(results, "1", "2"), 1, startDate)
    )
    assert( "2020-01-12" == getValueFromId[String]
      (filterPassangersFlights(results, "1", "2"), 1, endDate)
    )
    assert( "2020-01-06" == getValueFromId[String]
      (filterPassangersFlights(results, "2", "1"), 1, startDate)
    )
    assert( "2020-01-12" == getValueFromId[String]
      (filterPassangersFlights(results, "2", "1"), 1, endDate)
    )
    assert( "2020-01-10" == getValueFromId[String]
      (filterPassangersFlights(results, "3", "1"), 1, startDate)
    )
    assert( "2020-01-16" == getValueFromId[String]
      (filterPassangersFlights(results, "3", "1"), 1, endDate)
    )

  }

  private def filterPassangersFlights(inputDf: DataFrame, passenger1: String, passenger2: String): DataFrame =
    inputDf.filter(col("passengerId").equalTo(passenger1) && col("passengerId2").equalTo(passenger2))
    .withColumn("id", lit(1))

  private def getValueFromId[T](inputDf: DataFrame, id: Int, colToTest: String): T =
    inputDf.filter(col("id").equalTo(id)).head.getAs[T](colToTest)

}
