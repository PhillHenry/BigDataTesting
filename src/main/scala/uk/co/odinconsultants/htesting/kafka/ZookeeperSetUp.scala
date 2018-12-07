package uk.co.odinconsultants.htesting.kafka

import java.net.InetSocketAddress

import org.apache.zookeeper.server.{ServerCnxnFactory, ZooKeeperServer}
import uk.co.odinconsultants.htesting.local.TestingFileUtils._


class ZookeeperStarter() {

  def startZookeeper(localhost: String, zkPort: Int): ServerCnxnFactory = {
    val zookeeper = new ZooKeeperServer(tmpDirectory("zkShapshotDir"), tmpDirectory("zkLogs"), 1000)
    val zk = ServerCnxnFactory.createFactory(new InetSocketAddress(localhost, zkPort), 10)
    zk.startup(zookeeper)
    println("ZK address = " + zk.getLocalAddress)
    zk
  }

}

object ZookeeperSetUp {

  def apply(hostname: String, port: Int): ServerCnxnFactory = {
    new ZookeeperStarter().startZookeeper(hostname, port)
  }
  
}
