package quantexa.interview

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, date_format, udf, lit, lower, to_date, when, lag, max, min}
import org.apache.spark.sql.types.IntegerType

object QuantexaInterviewLogic {

  def getFlightsPerMonth(df: DataFrame): DataFrame = {
    val addedMonthValues = df.transform(getMonthValueColumn)

    addedMonthValues.groupBy("month").count
  }

  def getMonthValueColumn(flights: DataFrame): DataFrame = {
    flights.withColumn(
      "month",
      date_format(to_date(col("date"), "yyyy-MM-dd"), "MM")
    )
  }

  def getPassengerswithMoreFlights(passengers: DataFrame, flights: DataFrame): DataFrame = {
    val idWithMoreFlights = flights.groupBy("passengerId")
      .agg(count("passengerId").cast(IntegerType) as "Number of flights")

    passengers.join(
      idWithMoreFlights,
      Seq("passengerId"),
      "left_outer"
    ).orderBy(col("Number of flights").desc)
  }

  def getStartsArriveUK(flights: DataFrame): DataFrame = {
    flights.withColumn("starts_arrives_uk",
      when(
        lower(col("from")).equalTo("uk"),
        lit(1)
      ).when(
        lower(col("to")).equalTo("uk"),
        lit(2)
      ).otherwise(0)
    ).orderBy(col("date").desc)
  }

  def getLongestRun(flights: DataFrame, spark: SparkSession): DataFrame = {
    val starts_arrives_uk = flights.transform(getStartsArriveUK)

    val window = Window.partitionBy("passengerId").orderBy("date")
    spark.sparkContext.register(ComplexAccumulatorV2, "accumulator")

    val flags = starts_arrives_uk.withColumn(
      "flag", when(
        col("starts_arrives_uk").notEqual(lag(col("starts_arrives_uk"), 1).over(window)),
        lit(1)
      ).otherwise(lit(0)))

    val acc = new MyComplex(0)

    val func = udf((s: Int) => {
      if (s > 0) {
        acc.add(1)
        acc.x
      } else {
        acc.x
      }
    })

    val notUkFlag = flags
      .withColumn("not_uk", lit(0))
      .withColumn("not_uk", func(col("flag")))

    notUkFlag
      .groupBy("passengerId", "not_uk").count()
      .groupBy("passengerId").agg( max(col("count")) as "Longest run outside the UK" )

  }

  def getPassengersInFlightsTogether(flightsDf: DataFrame, minimumOfFlightsTogether: Int): DataFrame = {

    val renamedFlights = flightsDf
      .withColumnRenamed("passengerId", "passengerId2")
      .select("passengerId2", "flightId")

    val inner = flightsDf
      .join( renamedFlights, Seq("flightId"), "inner" )
      .select("passengerId", "passengerId2", "flightId", "date")
      .filter( col("passengerId").notEqual(col("passengerId2")) )

    inner
      .groupBy("passengerId", "passengerId2")
      .agg(
        count("flightId") as "Number of flights together",
        min("date") as "From",
        max("date") as "To"
      )
      .filter( col("Number of flights together") > minimumOfFlightsTogether )
      .orderBy(col("passengerId").asc)
  }

  def getPassengersInFlightsTogetherByDate(
    flights: DataFrame,
    minimumOfFlightsTogether: Int,
    startDate: String,
    endDate: String): DataFrame = {

    val flightsInDates = flights
      .filter(
        col("date").geq(startDate) && col("date").leq(endDate)
      )

    getPassengersInFlightsTogether(flightsInDates, minimumOfFlightsTogether)
  }
}
