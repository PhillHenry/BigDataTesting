package uk.co.odinconsultants.htesting.stack


import java.sql.Statement
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting._
import uk.co.odinconsultants.htesting.local.PortUtils
import uk.co.odinconsultants.htesting.hive.HiveForTesting
import uk.co.odinconsultants.htesting.stack.StreamingIntegrationTest.COUNTER
import uk.co.odinconsultants.htesting.spark.SparkForTesting._
import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.kafka.{KafkaStarter, ZookeeperSetUp}

class StreamingIntegrationTest extends WordSpec with Matchers {

  type StreamType = Dataset[(String, String, java.sql.Date)]

  val partitionField = "partitionkey"

  private def dfWrite(df: StreamType, sinkFile: String): StreamingQuery = {
    val sinkURL = hdfsUri + sinkFile
    val checkpointFilename = hdfsUri + "checkpoint"
    val streamingQuery = df.writeStream.format("parquet")
      .outputMode(OutputMode.Append())
      .option("path", sinkURL)
      .option("checkpointLocation", checkpointFilename)
      .partitionBy(partitionField)
      .start()
    streamingQuery
  }

  private def dfRead(topicName: String, port: Int, hostname: String): StreamType = {
    val df = session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$hostname:$port")
      .option("subscribe", topicName)
      .load()
    import df.sqlContext.implicits._
    val today     = new java.sql.Date(new java.util.Date().getTime)
    val yesterday = new java.sql.Date(today.getTime - (3600 * 1000 * 24))
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)].map { case (key, value) =>
      println(s"key = $key")
      COUNTER.incrementAndGet()
      (key, value, if (math.random < 0.5) today else yesterday)
    }.withColumnRenamed("_1", "key").withColumnRenamed("_2", "value").withColumnRenamed("_3", partitionField).asInstanceOf[StreamType]
  }

  "Full Kafka, ZooKeeper, Spark and HDFS stack" should {
    "play nicely" in {
      println(HiveForTesting.hiveServer)

      val topicName   = "topicName"
      val kafkaPort   = PortUtils()
      val zkPort      = PortUtils()
      val hostname    = "localhost"
      val zooKeeper   = ZookeeperSetUp(hostname, zkPort)
      val kafkaEnv    = new KafkaStarter(hostname, kafkaPort, zkPort, topicName)
      val sinkFile    = "tmp_parquet"
      val kafkaServer = kafkaEnv.startKafka()

      val streamingQuery = sendMessagesToKafkaSpark(kafkaEnv, sinkFile)

      ensureMesagesSent(streamingQuery)

      val files       = checkSparkProcessedMessages(sinkFile)

      COUNTER.get() should be > (0)
      val sparkCount  = session.read.parquet(hdfsUri + sinkFile).count()
      val hiveCount   = talkToHive(sinkFile, files)
      sparkCount.toInt shouldEqual hiveCount

      kafkaServer.shutdown()
      zooKeeper.shutdown()
    }
  }

  def sendMessagesToKafkaSpark(kafkaEnv: KafkaStarter, sinkFile: String): StreamingQuery = {
    val df              = dfRead(kafkaEnv.topicName, kafkaEnv.kPort, kafkaEnv.hostname)
    val streamingQuery  = dfWrite(df, sinkFile)
    val nMessages       = 10000
    sendMessagesSynchronously(kafkaEnv.topicName, kafkaEnv.producer(), nMessages)
    streamingQuery
  }

  private def ensureMesagesSent(streamingQuery: StreamingQuery) = {
    println("Sleeping waiting for Spark")
    streamingQuery.processAllAvailable()
    streamingQuery.exception.foreach { x =>
      x.printStackTrace()
      fail(x)
    }
    println("Recent progress: " + streamingQuery.recentProgress.size)
    Thread.sleep(10000)
  }

  def talkToHive(sinkFile: String, files: List[Path]): Int = {
    val sinkURL     = hdfsUri + sinkFile
    val table_name  = "parquet_table_name"
    // see https://www.cloudera.com/documentation/enterprise/5-13-x/topics/cdh_ig_parquet.html#parquet_hive
    // https://community.cloudera.com/t5/Interactive-Short-cycle-SQL/External-Table-from-Parquet-folder-returns-empty-result/m-p/66425
    val sql         = s"create external table $table_name (key String, value String) PARTITIONED BY (`$partitionField` Date) STORED AS PARQUET LOCATION '$sinkURL'"
    import java.sql.DriverManager
    val con         = DriverManager.getConnection(s"jdbc:hive2://localhost:${HiveForTesting.hiveThriftPort}/default", "", "")
    val stmt        = con.createStatement
    System.out.println("Running: " + sql)
    stmt.execute(sql)
    //    Thread.sleep(Long.MaxValue)
    recognisePartitions(files, table_name, stmt)
    val count = countAllRows(table_name, stmt)
    con.close()
    count
  }

  private def countAllRows(table_name: String, stmt: Statement): Int = {
    val res = stmt.executeQuery("select count(*) from " + table_name)
    res.next() shouldBe true
    val count = res.getInt(1)
    println(s"Hive count = $count")
    count
  }

  private def recognisePartitions(files: List[Path], table_name: String, stmt: Statement): Unit = {
    stmt.execute(s"MSCK REPAIR TABLE  $table_name")
    //    val keyDirs = files.map(_.toString).filter(_.contains(partitionField)).map { file =>
    //      val dir       = file.substring(0, file.lastIndexOf("/"))
    //      val partition = dir.substring(dir.lastIndexOf("/") + 1)
    //      val key       = partition.substring(partition.indexOf("=") + 1)
    //      (key, dir)
    //    }
    //    keyDirs.toSet[(String, String)].foreach { case (key, dir) =>
    //      val sql = s"ALTER TABLE $table_name ADD PARTITION (partitionKey='$key') location '$dir'"
    //      println(s"Running: $sql")
    //      stmt.execute(sql)
    //    }
  }

  private def checkSparkProcessedMessages(sinkFile: String): List[Path] = {
    val actualFiles = list(hdfsUri + sinkFile)
    println(s"Files in $sinkFile:\n${actualFiles.mkString("\n")}")
    actualFiles should not be empty
    actualFiles
  }

  private def sendMessagesSynchronously(topicName: String, producer: Producer[String, String], n: Int) = {
    val aMsg = "A" * 10240
    val futures = (1 to n).map { i =>
      val future = producer.send(new ProducerRecord[String, String](topicName, i.toString, aMsg), new Callback {
        override def onCompletion(metadata: RecordMetadata, x: Exception) = {} //println(s"onCompletion: $metadata $x")
      })
      future
    }
    futures.map(_.get(10, TimeUnit.SECONDS))
  }
}

object StreamingIntegrationTest {
  val COUNTER = new AtomicInteger(0)
}