package com.johnny.demo

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.SparkSession


object SparkMysqlDemo {
  def getMysqlConnection(): Connection = {
    DriverManager.getConnection("jdbc:mysql://ambari.com:3306/sephora_analyse?useUnicode=true&characterEncoding=utf8", "root", "root")
  }

  def processHiveData(): Unit = {

    val spark = SparkSession.builder().appName("SparkMysqlDemo").getOrCreate();
    val sc = spark.sparkContext;
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)


    val alldata = hiveContext.sql("select * from sephora.sephora_order")

    val connection = getMysqlConnection()
    var ps: PreparedStatement = null
    ps = connection.prepareStatement("insert into sephora_analyse.sumforsephora(sumnum) values(?)")
    ps.setString(1, alldata.count().toString)
    ps.executeUpdate()
  }

  case class Row(sum: String)

  def main(args: Array[String]): Unit = {
    //    System.setProperty("hive.metastore.uris", "thrift://bigdata.com:9083")
    //
    //    System.setProperty("hive.metastore.sasl.enabled", "true")
    //    System.setProperty("hive.security.authorization.enabled", "false")
    //    System.setProperty("hive.metastore.execute.setugi", "true")
    //
    //    System.setProperty("hive.metastore.kerberos.principal", "hive/bigdata.com@CONNEXT.COM")
    //    System.setProperty("hive.metastore.kerberos.keytab.file", this.getClass.getResource("/hive.service.keytab").getPath())

    processHiveData()
  }
}
