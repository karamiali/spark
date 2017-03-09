package karami.dev.spark.velib

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark._
import sys.process._

object Run extends App {

  val avscfile = "file:///home/karami/Drive/DEV/spark/data/velib/stations_velib.avsc";
  val csvfile  = "file:///home/karami/Drive/DEV/spark/data/velib/stations_velib.csv";
  val avrofile = "file:///home/karami/Drive/DEV/spark/data/velib/stations_velib.avro";
  val files_semaine = "file:///home/karami/Drive/DEV/spark/data/velib/semaine/*.json";
  val files_weekend = "file:///home/karami/Drive/DEV/spark/data/velib/weekend/*.json";
  val outputpath = "file:///home/karami/Drive/DEV/spark/data/output/";
  
  // spark.history.ui.port	: 18080
  //  CONFIGURATION DU JOBS MASTER
  val conf = new SparkConf()
      .setAppName("velib_application")
      .setMaster("local[1]")
      .set("spark.history.ui.port","true") 
      .set("spark.executor.memory","3g");
      
  val sc = new SparkContext(conf)
  //sc.setLogLevel("ERROR")
  
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  
  val data_semaine = sqlContext
      .read.json(files_semaine)
      .filter(col("fields.status").contains("OPEN"))
      .select("fields")
      .drop("fields.position")
      
  val data_pre_aggregated_s = data_semaine
      .groupBy("fields.number")
      .agg(sum("fields.bike_stands").alias("pre_sum_bike_stands"), sum("fields.available_bikes").alias("pre_sum_available_bikes"))
      
  val data_aggregated_s = data_pre_aggregated_s
      .select(sum("pre_sum_bike_stands")
      .alias("sum_bike_stands"), sum("pre_sum_available_bikes").alias("sum_available_bikes"))
      .collect
      
  var total_stations_s = data_aggregated_s.map(_.get(0).toString.toFloat).head
  var velib_dispo_s = data_aggregated_s.map(_.get(1).toString.toFloat).head
  
  
  val data_weekend = sqlContext.read.json(files_weekend).filter(col("fields.status").contains("OPEN")).select("fields").drop("fields.position")
  val data_pre_aggregated_w = data_weekend.groupBy("fields.number").agg(sum("fields.bike_stands").alias("pre_sum_bike_stands"), sum("fields.available_bikes").alias("pre_sum_available_bikes"))
  val data_aggregated_w = data_pre_aggregated_w.select(sum("pre_sum_bike_stands").alias("sum_bike_stands"), sum("pre_sum_available_bikes").alias("sum_available_bikes")).collect
  
  var total_stations_w = data_aggregated_w.map(_.get(0).toString.toFloat).head
  var velib_dispo_w = data_aggregated_w.map(_.get(1).toString.toFloat).head
  
  var res_semaine = (velib_dispo_s/total_stations_s)*100;
  var res_weekend = (velib_dispo_w/total_stations_w)*100;
  Console.println("\t 	TAUX DE DISPONIBILITE DES VELIBS EN SEMAINE : "+ res_semaine + " % ");
  Console.println("\t 	 TAUX DE DISPONIBILITE DES VELIBS EN WEEKEND : " + res_weekend + " % ");
  
  // 1 EXECUTEUR & ALLOCATION NULL : 27s  
  // 1 EXECUTEUR & ALLOCATION 1 Go : 25s
  // 1 EXECUTEUR & ALLOCATION 3 Go : 25s
  // 3 EXECUTEUR & ALLOCATION NULL : 24S
  // 3 EXECUTEUR & ALLOCATION 1 Go : 30s
  // 3 EXECUTEUR & ALLOCATION 3 Go : 31s
  
}