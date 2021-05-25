package test

case class Task() extends Serializable {
  val data = List(1,2,3,4)

  val logic: Int => Int = _ * 2
  
  def compute(): List[Int] = {
    data.map(logic)
  }
}


