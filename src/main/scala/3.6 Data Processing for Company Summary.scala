3.6

val compDateSumm = taxiTripDF.na.drop("any").groupBy("company", "trip_start_date").agg(count("trip_id_int").as("total_trips"),sum("trip_fare").as("total_fare"),sum("trip_miles").as("total_distance"), sum("trip_seconds").as("total_duration"),avg("trip_fare").as("avg_fare"), avg("trip_miles").as("avg_distance"),avg("trip_seconds").as("avg_duration"))

val commYearlySumm = compDateSumm.groupBy(col("company"),year(col("trip_start_date")).as("year")).agg(sum("total_trips").as("daily_trip_count"),round(sum("total_fare"),2).as("daily_total_fare"),round(sum("total_distance"),2).as("daily_total_distance"),round(sum("total_duration"),2).as("daily_total_duration"),round(avg("avg_fare"),2).as("daily_average_amount"),round(avg("avg_distance"),2).as("daily_average_distance"),round(avg("avg_duration"),2).as("daily_average_duration")).orderBy("company","year")


commYearlySumm.write.mode("overwrite").saveAsTable("edureka_972937.CompanyYearSummary")