import org.apache.spark.sql.functions._
import com.mongodb.spark._

val df = spark.read.parquet("hdfs://hadoop-master:9000/user/root/energie/household.parquet")

val profils = df.
  withColumn("heure", hour($"timestamp")).
  withColumn("jour", date_format($"timestamp", "u").cast("int")).
  groupBy("heure", "jour").
  agg(avg("global_active_power").as("moyenne"),
      stddev("global_active_power").as("ecart_type"),
      count("*").as("n")).
  orderBy("jour", "heure")

println("nombre de profils : " + profils.count())

MongoSpark.save(profils.write.mode("overwrite"))
profils.write.mode("overwrite").parquet("hdfs://hadoop-master:9000/user/root/energie/profils.parquet")

:quit