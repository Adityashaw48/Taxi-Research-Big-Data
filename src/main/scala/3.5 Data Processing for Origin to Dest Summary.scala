3.5

val destPairSumm=taxiTripDF.na.drop("any").groupBy("pickup_community","dropoff_community").agg(count(lit(1)).alias("Trip_Count"),round(sum("trip_miles"),2).alias("Total_Trip_Miles"),round(avg("trip_miles"),2).alias("Avg_Trip_Miles"),round(avg("trip_seconds"),2).alias("Avg_Trip_Duration"),round(avg("trip_fare"),2).alias("Avg_Trip_fare")).orderBy("pickup_community","dropoff_community")

destPairSumm.write.mode("overwrite").saveAsTable("edureka_972937.DestPairSummary")