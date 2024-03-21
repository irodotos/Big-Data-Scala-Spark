object ScalaWarmUp{

  def getSecondToLast(lst: List[Int]): Int = { 
    def helper(lst: List[Int], prev: Int, curr: Int): Int = lst match {
      case Nil => prev
      case h :: tail => helper(tail, curr, h)
    }
    helper(lst, lst.head, lst.tail.head)
  }

  def getSecondToLastZip(lst: List[Int]): Int = {
    lst.zipWithIndex.filter { case (_, i) => i == lst.size - 2 }.head._1
  }
  
  def filterUnique(lst: List[String]): List[String] = {
    lst.foldRight(List[String]()) { (element, list) =>
    if (list.isEmpty || list.head != element) element :: list
    else list
  }
  }
  
  def getMostFrequentSubstring(lst: List[String], k: Int): String = {
    lst.flatMap(str => str.sliding(k))
    .groupBy(identity)
    .mapValues(_.size)
    .maxBy(_._2)
    ._1
  }
  
  // def specialMonotonic(low: Int, high: Int): List[Int] = {

  // }	

  def main(args: Array[String]): Unit = {
    val lst = List(1, 2, 3, 4, 5)

    assert(getSecondToLast(lst) == lst(lst.size - 2))  

    assert(getSecondToLastZip(lst) == lst(lst.size - 2))
    
    assert(filterUnique(List("c", "c", "d", "e", "e", "a", "a")) == List("c", "d","e", "a"))
    assert(filterUnique(List("a", "c", "b", "b", "a", "c")) == List("a", "c", "b","a", "c"))

    assert(getMostFrequentSubstring(List("abcd", "xwyuzfs", "klmbco"), 2) == "bc")
  }
}

