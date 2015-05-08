package org.hammerlab.spear

import com.mongodb.DBObject
import com.mongodb.casbah.Imports.MongoDBObject

object MongoCaseClassSerializer {
  def isCaseClass(p: AnyRef with Product): Boolean = p.getClass.getInterfaces.contains(classOf[Product])
  def isTuple(x: AnyRef): Boolean = x.getClass.getName.startsWith("scala.Tuple")
  def to(o: Any): DBObject = {
    val builder = MongoDBObject.newBuilder
    o match {
      case p: AnyRef with Product if isCaseClass(p) =>
        val pi = p.productIterator.toList
        val fields = p.getClass.getDeclaredFields.toList
        pi.zip(fields).map {
          case (value, field) =>
            toValue(value).foreach(builder += field.getName -> _)
        }
      case m: Map[_, _] =>
        builder ++= (for {
          (k,v) <- m.toList
          value <- toValue(v)
        } yield {
            (k.toString, value)
          })
      case _ => throw new IllegalArgumentException(s"Can't convert $o to DBObject")
    }
    builder.result()
  }

  def toValue(o: Any): Option[Any] = {
    o match {
      case o if o == null => None
      case o: Option[_] => o.flatMap(toValue)
      case t: AnyRef with Product if isTuple(t) => Some(t.productIterator.flatMap(toValue).toList)
      case p: AnyRef with Product if isCaseClass(p) =>
        val builder = MongoDBObject.newBuilder
        p.productIterator.toList.zip(p.getClass.getDeclaredFields.toList).foreach {
          case (value, field) =>
            toValue(value).foreach(builder += field.getName -> _)
        }
        Some(builder.result())
      case n: Number => Some(n)
      case s: String => Some(s)
      case m: Map[_, _] =>
        val builder = MongoDBObject.newBuilder
        builder ++= (for {
          (k,v) <- m.toList
          value <- toValue(v)
        } yield {
            (k.toString, value)
          })
        Some(builder.result())
      case it: Iterable[_] => Some(it.flatMap(toValue))
      case arr: Array[_] => Some(arr.flatMap(toValue))
      case b: Boolean => Some(b)
    }
  }
}

