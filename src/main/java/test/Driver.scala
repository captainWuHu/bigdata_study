package test

import java.io.{ObjectOutput, ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    //连接服务器

    val client1 = new Socket("127.0.0.1", 9999)
    val client2 = new Socket("127.0.0.1", 9998)

    val out1: OutputStream = client1.getOutputStream
    val out2: OutputStream = client2.getOutputStream

    val objectOut1 = new ObjectOutputStream(out1)
    objectOut1.writeObject(SubTask(Task().data.take(2),Task().logic))
    objectOut1.flush()


    val objectOut2 = new ObjectOutputStream(out2)
    objectOut2.writeObject(SubTask(Task().data.takeRight(2),Task().logic))
    objectOut2.flush()


    out1.close()
    objectOut1.close()
    client1.close()

    out2.close()
    objectOut2.close()
    client2.close()


  }
}
