
3.3.1
val DailyS=taxiTripDF.select((col("trip_seconds")/60).as("trip_minutes"),col("trip_start_date"),col("start_dayofweek"),col("trip_month_number"),col("trip_year"),col("weekend"),col("trip_total_amt"),col("trip_miles")).groupBy("trip_start_date","start_dayofweek","trip_month_number","trip_year","weekend").agg(count(lit(1)).alias("Total_Trip_Count"),round(sum("trip_total_amt"),2).alias("Total_Trip_Fare"),round(sum("trip_miles"),2).alias("Total_Trip_Miles"),round(sum("trip_minutes"),2).alias("Total_Trip_Duration_mins"),round(avg("trip_total_amt"),2).alias("Avg_Trip_Fare"),round(avg("trip_miles"),2).alias("Avg_Trip_Miles"),round(avg("trip_minutes"),2).alias("Avg_Trip_Duration_mins")).withColumnRenamed("trip_start_date","Date").withColumnRenamed("start_dayofweek","Day_of_Week").withColumnRenamed("trip_month_number","Month").withColumnRenamed("trip_year","Year").withColumnRenamed("weekend","Weekend_Weekday")


DailyS.write.mode("overwrite").saveAsTable("edureka_972937.DailySummaryTable")
