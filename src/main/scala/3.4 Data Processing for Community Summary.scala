3.4

val pickUp=taxiTripDF.groupBy("pickup_community","trip_start_date").agg(count(lit(1)).alias("Daily_Trip_Count"),round(sum("trip_total_amt"),2).alias("Daily_Total_Fare"),round(sum("trip_miles"),2).alias("Daily_Total_Distance"),sum("trip_seconds").alias("Daily_Total_Duration"),round(avg("trip_total_amt"),2).alias("Daily_Avg_Amount"),round(avg("trip_miles"),2).alias("Daily_Avg_Distance"),avg("trip_seconds").alias("Daily_Avg_Duration")).drop("trip_start_date")

pickUp.write.mode("overwrite").saveAsTable("edureka_972937.PickUpCommunity")


val dropOff=taxiTripDF.groupBy("dropoff_community","trip_start_date").agg(count(lit(1)).alias("Daily_Trip_Count"),round(sum("trip_total_amt"),2).alias("Daily_Total_Fare"),round(sum("trip_miles"),2).alias("Daily_Total_Distance"),sum("trip_seconds").alias("Daily_Total_Duration"),round(avg("trip_total_amt"),2).alias("Daily_Avg_Amount"),round(avg("trip_miles"),2).alias("Daily_Avg_Distance"),avg("trip_seconds").alias("Daily_Avg_Duration")).drop("trip_start_date")

dropOff.write.mode("overwrite").saveAsTable("edureka_972937.DropOffCommunity")
