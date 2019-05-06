package com.johnny.demo

import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object SparkSQLDemo {
  def processCSVFile(): Unit = {

    val conf = new SparkConf().setAppName("SparkHiveDemo")
    val sc = new SparkContext(conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)


    System.out.println("start======================")

    val hadoopConf = sc.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

    val basicFolderPath = new Path("hdfs://ambari.com:8020/johnny/flume/events");

    val filePathList = mutable.Stack[Path]()
    val folderStatusArray = hdfs.listStatus(basicFolderPath);
    folderStatusArray.foreach(folderStatus => {

      if (folderStatus.isDirectory && folderStatus.getPath.getName.matches("^[0-9]{4}[0-9]{2}[0-9]{2}$")) {
        val fileIterator = hdfs.listFiles(folderStatus.getPath, false)
        while (fileIterator.hasNext) {
          val file = fileIterator.next

          System.out.println("file find: ======================" + file.getPath.toUri.getPath)


          if (file.isFile && !file.getPath.getName.endsWith(".bak")) {
            filePathList.push(file.getPath)
          }
        }
      }
    })

    import hiveContext.implicits._
//    hiveContext.sql("use sephora")


    //    val files = sc.textFile("hdfs://bigdata.com:8020/johnny/flume/events/*/events.*.log")
    //    var sephoraOrderDF =
    //      files
    //        .map(_.split(","))
    //        .map(line => SephoraOrder(line(0), line(1), line(2), line(3), line(4),
    //          line(5), line(6), line(7), line(8), line(9),
    //          line(10), line(11), line(12), line(13))).toDF()
    //    sephoraOrderDF.write.insertInto("sephora_order")



    filePathList.foreach(filePath => {
      System.out.println("file process: ======================" + filePath.toUri.getPath)


      var rdds = sc.textFile(filePath.toUri.getPath)

      var sephoraOrderDF =
        rdds
          .map(_.split(","))
          .map(line => SephoraOrder(line(0), line(1), line(2), line(3), line(4),
            line(5), line(6), line(7), line(8), line(9),
            line(10), line(11), line(12), line(13))).toDF()


      sephoraOrderDF.write.insertInto("sephora.sephora_order")

      hdfs.rename(filePath, new Path(filePath.toUri.getPath + filePath.getName + ".bak"));
    })


    sc.stop();

    System.out.println("end======================")

  }

  case class SephoraOrder(order_num: String, order_amount: String, freight: String, discount: String, member_card: String,
                          user_id: String, pay_type: String, order_source: String, order_data: String, member_group: String,
                          province: String, city: String, region: String, address: String)

  def main(args: Array[String]) {

//    System.setProperty("hive.metastore.uris", "thrift://bigdata.com:9083")
//
//    System.setProperty("hive.metastore.sasl.enabled", "true")
//    System.setProperty("hive.security.authorization.enabled", "false")
//    System.setProperty("hive.metastore.execute.setugi", "true")
//
//    System.setProperty("hive.metastore.kerberos.principal", "hive/bigdata.com@CONNEXT.COM")
//    System.setProperty("hive.metastore.kerberos.keytab.file", this.getClass.getResource("/hive.service.keytab").getPath())

    processCSVFile()
  }
}
