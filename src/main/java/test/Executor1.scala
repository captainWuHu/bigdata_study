package test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor1 {
  def main(args: Array[String]): Unit = {
    //启动服务器
    val server = new ServerSocket(9999)
    println("server start")
    //等待客户端连接
    val client: Socket = server.accept()

    val in: InputStream = client.getInputStream
    val objectIn = new ObjectInputStream(in)
    val task: SubTask = objectIn.readObject().asInstanceOf[SubTask]

    print("ex1 result: " + task.compute())
    in.close()
    objectIn.close()
    client.close()
    server.close()
  }
}
