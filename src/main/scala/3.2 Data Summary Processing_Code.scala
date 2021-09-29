val taxiTripDF=spark.sql("SELECT * FROM edureka_972937.taxi_trip_detail_edureka_972937")

3.2.1
taxiTripDF.na.drop(Seq("TRIP_YEAR")).groupBy("TRIP_YEAR").agg(count(lit(1)).alias("Total Number Of Trips")).orderBy("TRIP_YEAR").show

3.2.2
taxiTripDF.filter($"TRIP_YEAR".isNotNull).groupBy("TRIP_YEAR").pivot("TRIP_MONTH").agg(count(lit(1))).orderBy("TRIP_YEAR").show

3.2.3
val dropOfPer=taxiTripDF.select(col("dropoff_community")).na.drop("any").groupBy("dropoff_community").agg(count(lit(1)).alias("Community_trips")).withColumn("Percentage",col("Community_trips")*100/sum("Community_trips").over()).orderBy($"Community_trips".desc).limit(10)

dropOfPer.select(col("dropoff_community"),col("Community_trips"),round(col("Percentage"),2).alias("Percentage"))show()

3.2.4
val dropDF=taxiTripDF.select(col("dropoff_community"),col("TRIP_YEAR")).na.drop("any").groupBy("dropoff_community","TRIP_YEAR").agg(count(lit(1)).alias("Community_trips")).orderBy($"Community_trips".desc)

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val ro_number= row_number().over(Window.orderBy($"Community_trips".desc))

dropDF.withColumn("ROW_NUM",ro_number).filter("ROW_NUM<11").drop("ROW_NUM").show

3.2.5
#val wkndTrip=taxiTripDF.select(col("dropoff_community"),col("weekend")).na.drop("any").groupBy("dropoff_community","weekend").agg(count(lit(1)).alias("Total_Trips")).orderBy($"Total_Trips".desc)

val WkdDF=taxiTripDF.select(col("dropoff_community"),col("weekend")).na.drop("any").groupBy("dropoff_community","weekend").agg(count(lit(1)).alias("Total_Trips")).filter("weekend==1")

val ro_number= row_number().over(Window.orderBy($"Total_Trips".desc))

WkdDF.withColumn("ROW_N",ro_number).filter("ROW_N<11").drop("ROW_N","weekend").show

val WkyDF=taxiTripDF.select(col("dropoff_community"),col("weekend")).na.drop("any").groupBy("dropoff_community","weekend").agg(count(lit(1)).alias("Total_Trips")).filter("weekend==0")

WkyDF.withColumn("ROW_N",ro_number).filter("ROW_N<11").drop("ROW_N","weekend").show


val RatDF=taxiTripDF.select(col("dropoff_community"),col("weekend")).na.drop("any").groupBy("dropoff_community","weekend").agg(count(lit(1)).alias("Total_Trips")).orderBy($"Total_Trips".desc)

RatDF.select(col("weekend"),col("Total_Trips")).groupBy("weekend").agg(sum("Total_Trips").alias("Total_trips")).withColumn("Ratio",round(col("Total_trips")/sum("Total_Trips").over(),2)).show

3.2.6

val TripDur=taxiTripDF.na.drop("any").withColumn("Trip_hour",round(col("trip_seconds")/3600,2))

val TripDur2=TripDur.select(col("Trip_Hour"),when(ceil("Trip_hour")===0,"0-1").when(floor("Trip_hour")===ceil("Trip_hour"),concat(floor("Trip_hour")-1,lit("-"),ceil("Trip_hour"))).when(floor("Trip_hour")=!=ceil("Trip_hour"),concat(floor("Trip_hour"),lit("-"),ceil("Trip_hour"))).alias("Trip_hour_Range"))

TripDur2.select("Trip_hour","Trip_hour_Range").groupBy("Trip_hour_Range").agg(count(lit(1)).alias("Trip_count")).orderBy(split(col("Trip_hour_Range"),"-").getItem(0).cast("int")).show()

3.2.7
*************HANDLE ZERO DISTANCE ***************
taxiTripDF.select("trip_miles").na.drop("any").withColumn("Trip_Distance",round(col("trip_miles"),0)).groupBy("Trip_Distance").agg(count(lit(1)).alias("Number_of_Trips")).orderBy($"Number_of_Trips".desc).show

3.2.8
taxiTripDF.select("trip_fare").na.drop("any").withColumn("Roundoff_Fare",round(col("trip_fare"),0)).groupBy("Roundoff_Fare").agg(count(lit(1)).alias("Number_of_Trips")).orderBy($"Number_of_Trips".desc).show


3.2.9
--per day
taxiTripDF.groupBy("trip_start_date").agg(round(avg("trip_fare"),2).alias("Avg_Fare")).agg(avg("Avg_Fare").alias("Avg_Per_Day")).show()

--per trip
taxiTripDF.na.drop("any").agg(round(avg("trip_fare"),2).alias("Avg_Fare")).show(10)

--weekday
taxiTripDF.filter("weekend==0").groupBy("trip_start_date").agg(avg("trip_fare").alias("Avg_Fare")).agg(round(avg("Avg_Fare"),2).alias("Avg_Per_Day")).show()

--weekend
taxiTripDF.filter("weekend==1").groupBy("trip_start_date").agg(avg("trip_fare").alias("Avg_Fare")).agg(round(avg("Avg_Fare"),2).alias("Avg_Per_Day")).show()

--per trip
taxiTripDF.na.drop("any").filter("weekend==0").agg(round(avg("trip_fare"),2).alias("Avg_Fare")).show(10)

taxiTripDF.na.drop("any").filter("weekend==1").agg(round(avg("trip_fare"),2).alias("Avg_Fare")).show(10)


--3.2.10

val TaxiDF=taxiTripDF.select("taxi_id_int","trip_start_date","trip_fare").groupBy("taxi_id_int","trip_start_date").agg(sum("trip_fare").alias("Total_Fare_Per_Day"),count(lit(1)).alias("Total_Trip_Per_Day"))

TaxiDF.write.mode("overwrite").saveAsTable("edureka_972937.TaxiData")

TaxiDF.groupBy("taxi_id_int").agg(avg("Total_Trip_Per_Day").cast("int").alias("Avg_Trip_Per_Day")).orderBy($"Avg_Trip_Per_Day".desc).show(10)

TaxiDF.groupBy("taxi_id_int").agg(round(avg("Total_Fare_Per_Day"),2).alias("Avg_Fare_Per_Day")).orderBy($"Avg_Fare_Per_Day".desc).show(10)
