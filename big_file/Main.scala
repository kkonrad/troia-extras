import scala.util.parsing.json._
import java.io._
import scala.collection.parallel.ParSeq


object Main extends App {
  
  def getObjectId(map: Map[String, Any]): String = {
    val w = map.get("curiosity").get
    try {
      w.asInstanceOf[String]
    } catch {
      case _: Throwable => w.asInstanceOf[Map[String, Any]].get("curiosity").asInstanceOf[Some[String]].get
    }
  }
  
  def convertLine(line: String): ParSeq[Tuple3[String, String, String]] = {
    val json: Option[Any] = JSON.parseFull(line)
    val map: Map[String, Any] = json.get.asInstanceOf[Map[String, Any]]
    val objectId = getObjectId(map)
    val assignsInJson = map.get("judgments").get.asInstanceOf[List[Map[String, Any]]]
    def help(judgment: Map[String, Any]): Tuple3[String, String, String] = {
      val label = judgment.get("judgment").get.asInstanceOf[String]
      val worker = judgment.get("contributor").get.asInstanceOf[String]
//      println(objectId + " " + worker + " " + label)
      (objectId, worker, label)
    }
    assignsInJson.par.map(help)
  }

  def iterFile(file: File) {
    val reader = new BufferedReader(new FileReader(file))
    val output = new java.io.PrintWriter(new File(file + ".out"))

    var line = reader.readLine
    while (line != null) {
      val assigns = convertLine(line)
      for (assign: Tuple3[String, String, String] <- assigns) {
        output.println(assign._1 + " " + assign._2 + " " + assign._3)
      }
      line = reader.readLine
    }
    output.flush()
    output.close()
    reader.close()
  }

  override def main(args: Array[String]) = {
    val file = new File(args(0))
    iterFile(file)
  }
}