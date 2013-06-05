  
import scala.io.Source
import java.io.PrintWriter

abstract class FieldAnonymizerBase {
 
  def translate(ident: String): String
}

class FieldAnonymizer(prefix: String) extends FieldAnonymizerBase {
  val known = collection.mutable.Map[String, String]()
  var next_id: Long = 0
  
  def translate(ident: String): String = {
    val existing_repr = known get ident
    existing_repr match {
      case Some(repr) => repr
      case None => getNextIdAndSet(ident)
    }
  }

  def getNextIdAndSet(ident: String): String = {
    val nextId = getNextId
    known += (ident -> nextId)
    nextId
  }

  def getNextId(): String = {
    next_id += 1
    "%s%010d".format(prefix, next_id)
  }
}

class IdentFieldAnonymizer extends FieldAnonymizerBase {

  def translate(ident: String): String = ident
}

class ShortenenFieldAnonymizer(length: Int = 1) extends FieldAnonymizerBase {

  def translate(ident: String): String = ident.substring(0, length)
}

class RowAnonymizer(fieldsAnonymizers: List[FieldAnonymizerBase], separator: String = " ") {
  def anonymizeRow(row: String): String = {
    val rowEls = row.stripLineEnd.split(separator)
    assert (rowEls.length == fieldsAnonymizers.size, println("Wrong sizes"))
    def pom = (fa:FieldAnonymizerBase, i:Int) => fa.translate(rowEls(i))
    val translated = fieldsAnonymizers.zipWithIndex.map(x => pom(x._1, x._2))
    translated.mkString(separator)
  }
}

object DataAnonimizer extends App {
  val fname = args(0)
  val out = new PrintWriter(args(1))
  val rowAnonymizer = new RowAnonymizer(List(new FieldAnonymizer("o"), new FieldAnonymizer("w"), new ShortenenFieldAnonymizer(1)))
  for(line <- Source.fromFile(fname).getLines()) {
    out.println(rowAnonymizer.anonymizeRow(line))
  }
  out.close
  
}