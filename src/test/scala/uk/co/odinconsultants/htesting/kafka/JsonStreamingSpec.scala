package uk.co.odinconsultants.htesting.kafka

import java.io.File.separator
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.Path
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, WordSpec}
import uk.co.odinconsultants.htesting.hdfs.HdfsForTesting._
import uk.co.odinconsultants.htesting.local.UnusedPort
import uk.co.odinconsultants.htesting.spark.SparkForTesting.session
import uk.co.odinconsultants.htesting.stack.StreamingIntegrationSpec.COUNTER

import scala.io.Source
import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class JsonStreamingSpec extends WordSpec with Matchers {

  "Json" should {
    "be read from a Kafka stream" in {

      val jsonFile    = "cloudtrail.json"
      val localFile   = JsonStreamingSpec.getClass.getResource("/").getPath + s"${separator}..${separator}..${separator}src${separator}test${separator}resources${separator}data${separator}$jsonFile"
      val remoteFile  = hdfsUri + jsonFile
      distributedFS.copyFromLocalFile(new Path(localFile), new Path(remoteFile))
      val exampleDF = session.read.json(remoteFile)
      println("PH: example")
      exampleDF.printSchema()
      val struct: StructType = exampleDF.schema
      println("PH: Schema: " + struct)
      val json = Source.fromFile(new java.io.File(localFile)).mkString
      println("PH: message size = "+ json.length)
      //val cheekyJson = Source.fromFile(new java.io.File(JsonStreamingSpec.getClass.getResource("/").getPath + s"${separator}..${separator}..${separator}src${separator}test${separator}resources${separator}data${separator}gd2acl_test_event.json")).mkString

      val prepended   = this.getClass.getSimpleName
      val topicName   = s"${prepended}_topicName"
      val kafkaPort   = UnusedPort()
      val zkPort      = UnusedPort()
      val hostname    = "localhost"
      val zooKeeper   = ZookeeperSetUp(hostname, zkPort)
      val kafkaEnv    = new KafkaStarter(hostname, kafkaPort, zkPort, topicName)
      val kafkaServer = kafkaEnv.startKafka()

      val df = session
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",  s"$hostname:$kafkaPort")
        .option("subscribe",                topicName)
        .option("offset",                   "earliest")
        .option("startingOffsets",          "earliest")
        .load()

      import df.sqlContext.implicits._

      val jsonDF = df.selectExpr("CAST(value AS STRING)").as[String].map { value =>
        COUNTER.incrementAndGet()
        value
      }

      import org.apache.spark.sql.functions._

      println("PH: df schema")
      /*
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)
       */
      df.printSchema()

      val outgoing = jsonDF.select(from_json($"value", struct).as("item"))
      println("PH: jsonDF schema")
      /*
root
 |-- value: string (nullable = true)

       */
      jsonDF.printSchema()

      println("PH: outgoing")
      /* as exampleDF but wrapped in an "item" node
root
 |-- item: struct (nullable = true)
 |    |-- Records: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- additionalEventData: struct (nullable = true)
 |    |    |    |    |-- SSEApplied: string (nullable = true)
 |    |    |    |    |-- x-amz-id-2: string (nullable = true)
 |    |    |    |-- awsRegion: string (nullable = true)
 |    |    |    |-- errorCode: string (nullable = true)
 |    |    |    |-- errorMessage: string (nullable = true)
 |    |    |    |-- eventID: string (nullable = true)
 |    |    |    |-- eventName: string (nullable = true)
 .
 .
 .
       */
      outgoing.printSchema()

      val sinkURL             = hdfsUri + "sinkfile"
      val checkpointFilename  = hdfsUri + "checkpoint"
      val streamingQuery      = outgoing.writeStream.format("parquet")
        .outputMode(OutputMode.Append())
        .option("path",               sinkURL)
        .option("checkpointLocation", checkpointFilename)
        .start()



      val futures = (1 to 1000).map { i =>
        val future = kafkaEnv.producer().send(new ProducerRecord[String, String](topicName, json), new Callback {
          override def onCompletion(metadata: RecordMetadata, x: Exception) = {}
        })
        future
      }

      println("Waiting for Kafka to consume message")
      futures.map(_.get(10, TimeUnit.SECONDS))


      println("Waiting for Spark to consume message")
      Thread.sleep(10000)
      val readSchema = list(sinkURL).map(_.toString).filter(_.endsWith(".parquet")).map { filename => println("PH: file " + filename)
        Try {
          val fromHdfsDF = session.read.parquet(filename)
          /* if we send corrupt JSON, the DF still have the expected schema but show() produces:
+----+
|item|
+----+
|  []|
|  []|
|  []|
.
.
           */
          println("PH: fromHdfsDF schema")
          fromHdfsDF.printSchema()
          fromHdfsDF.show()
          fromHdfsDF.schema
        }
      }
      val read = readSchema.filter(_.isSuccess) // some files appear only partially written. Not sure why.
      read.length should be >(0)
      read.map(_.get).foreach( _.head.dataType shouldBe struct )

      COUNTER.get() shouldBe > (0)

      streamingQuery.stop()
      kafkaServer.shutdown()
      zooKeeper.shutdown()
    }
  }
}

object JsonStreamingSpec {
  val COUNTER = new AtomicInteger(0)
}
