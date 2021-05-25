package test

case class SubTask(var data: List[Int],
                   var logic: Int => Int){
  
  def compute(): List[Int] = {
    data.map(logic)
  }
}
