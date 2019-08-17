package com.spark.streaming

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._


object SparkStreamingInit extends App {
  val kafkaBootStrapServers = args(0)
  val kafkaProducerTopic = args(1)
  val hiveMetaStore = args(2)
  val hiveTableName = args(3)
  val textFile = args(4)
  val sparkSession = createSparkSession(hiveMetaStore)
  val kafkaconsumerData = kafkaconsumer(sparkSession, kafkaBootStrapServers, kafkaProducerTopic).cache()
  kafkaconsumerData.show(5, truncate = false)
  val loadTable = loadHiveTable(sparkSession: SparkSession, kafkaconsumerData, hiveTableName, textFile)
  //loadtable.show(5, false)

  def createSparkSession(hiveMetaStore: String): SparkSession = {
       SparkSession.builder()
      //.config("hive.metastore.uris", hiveMetaStore)
      .appName("Spark_Streaming")
      .enableHiveSupport()
      .getOrCreate()
  }

 def kafkaconsumer(sparkSession: SparkSession, kafkaBootStrapServers: String, kafkaProducerTopic: String): Dataset[(String, String)] = {
    import sparkSession.implicits._
    val df = sparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootStrapServers)
      .option("subscribe", kafkaProducerTopic)
      .load()
     df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
  }

def readText(sparkSession: SparkSession, textFile: String) = {
  val readTextFile = sparkSession.read.textFile(textFile)
  readTextFile
}

case class dept_table(Dept_Id: String, Dept_Name: String)

  def loadHiveTable(sparkSession: SparkSession, kafkaConsumer: Dataset[(String, String)], hiveTableName: String, textFile: String) =   {
    import sparkSession.implicits._

    val deptFile = readText(sparkSession, textFile)
    val deptXfrm1 = deptFile.map(x => x.split(","))
    val deptXfrm2 = deptXfrm1.map(x => dept_table(x(0),x(1)))

     val tempView = kafkaConsumer.select("value")
      val parseTempView = tempView.withColumn("_tmp",split($"value", "\\,")).select(
      $"_tmp".getItem(0).as("EmpId"),
      $"_tmp".getItem(1).as("E_First_Name"),
      $"_tmp".getItem(2).as("E_Last_Name"),
      $"_tmp".getItem(3).as("E_Dept_Id"),
      $"_tmp".getItem(4).as("SSN"),
      $"_tmp".getItem(5).as("E_STARTDT")
    ).drop("_tmp").dropDuplicates(Seq("EmpId", "E_First_Name"))

    val parseTmpViewDeptNm = parseTempView.join(deptXfrm2, parseTempView.col("E_Dept_Id") === deptXfrm2.col("Dept_Id")).select(parseTempView.col("EmpId"),parseTempView.col("E_First_Name"),parseTempView.col("E_Last_Name"),deptXfrm2.col("Dept_Name").alias("E_Dept_Id"),parseTempView.col("SSN"), date_format(parseTempView.col("E_STARTDT"), "dd/MM/yyyy").alias("E_STARTDT"))

    parseTmpViewDeptNm.createOrReplaceTempView("emp_data")
    sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    sparkSession.sql(s"insert into $hiveTableName select * from emp_data")



  }

}