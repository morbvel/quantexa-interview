package quantexa.interview

import org.apache.spark.sql.SparkSession

object QuantexaInterviewMain extends App with Constants{

  val spark: SparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()

  val rawPassangersTable = QuantexaInterviewSources.getSourceData(PASSANGER_DATA_PATH, spark, PASSANGERS_TABLE)
  val rawFlightsTable = QuantexaInterviewSources.getSourceData(FLIGHT_DATA_PATH, spark, FLIGHTS_TABLE)

  val numberOfFlightsPerMonth = QuantexaInterviewLogic.getFlightsPerMonth(rawFlightsTable)
  numberOfFlightsPerMonth.show(false)

  val passangersWithMoreFlights = QuantexaInterviewLogic.getPassengerswithMoreFlights(rawPassangersTable, rawFlightsTable)
  passangersWithMoreFlights.show(false)

  val longestRunForPassangers = QuantexaInterviewLogic.getLongestRun(rawFlightsTable, spark)
  longestRunForPassangers.show(false)

  val passengersTogether = QuantexaInterviewLogic.getPassengersInFlightsTogether(rawFlightsTable, 3)
  passengersTogether.show(false)

  val passengersTogetherByDate = QuantexaInterviewLogic.
    getPassengersInFlightsTogetherByDate(rawFlightsTable, 3, "2020-01-01", "2020-012-31")
  passengersTogetherByDate.show(false)

}
