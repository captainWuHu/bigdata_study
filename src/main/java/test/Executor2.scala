package test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor2 {
  def main(args: Array[String]): Unit = {
    val server2 = new ServerSocket(9998)

    val client2: Socket = server2.accept()
    val in: InputStream = client2.getInputStream
    val objInput = new ObjectInputStream(in)
    val task:SubTask = objInput.readObject().asInstanceOf[SubTask]
    print("ex2 result:"+task.compute())
    server2.close()
    client2.close()
    in.close()
    objInput.close()
  }

}
