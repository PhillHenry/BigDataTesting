package uk.co.odinconsultants.htesting.kafka

import java.util.Properties

import kafka.server.{KafkaConfig, KafkaServerStartable}
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import uk.co.odinconsultants.htesting.local.TestingFileUtils._

case class KafkaStarter(hostname: String, kPort: Int, zkPort: Int, topicName: String) {

  def toLocalEndPoint(hostname: String, port: Int) = s"$hostname:$port"

  val props = new Properties()
  props.put("zookeeper.connect",                toLocalEndPoint(hostname, zkPort))
  props.put("bootstrap.servers",                toLocalEndPoint(hostname, kPort))
  props.put("port",                             kPort.toString)
  props.put("broker.id",                        "0")
  props.put("log.dir",                          tmpDirectory("zkLogs").getAbsolutePath)
  props.put("num.partitions",                   "1")
  props.put("key.serializer",                   classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
  props.put("value.serializer",                 classOf[org.apache.kafka.common.serialization.StringSerializer].getName)
  props.put("offsets.topic.replication.factor", "1")
  props.put("auto.offset.reset",                "earliest")
  props.put("host.name",                        hostname)
  props.put("advertised.host.name",             hostname)

  def startKafka(): KafkaServerStartable = {
    val server = new KafkaServerStartable(new KafkaConfig(props))
    server.startup()

    val adminClient = AdminClient.create(props)
    val topics = Seq[NewTopic](new NewTopic(topicName, 1, 1))
    import scala.collection.JavaConversions._
    adminClient.createTopics(topics)
    adminClient.close()

    server
  }

  def producer(): Producer[String, String] = new KafkaProducer[String, String](props)

}

