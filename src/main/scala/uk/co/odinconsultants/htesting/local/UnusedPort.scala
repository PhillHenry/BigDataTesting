package uk.co.odinconsultants.htesting.local

import java.net.ServerSocket

object UnusedPort {

  def apply(): Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

}
