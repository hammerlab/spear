package org.hammerlab.spear

import com.mongodb.casbah.MongoClient
import org.scalatest.{Matchers, FunSuite}

case class A(n: Int)
case class B(a: A)
case class C(a: (Int, A))
case class D(as: List[(Int, A)])
case class E(a: (Int, Int, A))
case class F(as: List[(Int, Int, A)])

case class G(a: (Long, Int, Int, A))
case class H(as: List[(Long, Int, Int, A)])

case class I(name: String, a: (Long, Int, Int, A))
case class J(name: String, as: List[(Long, Int, Int, A)])

case class M(a: Option[A])
case class N(name: String, as: List[(Long, Int, Int, M)])
case class O(name: String, as: List[M])

case class P(as: List[(Long, Int, Int, M)])
case class Q(as: List[M])
case class R(as: List[(Int, M)])
case class S(as: List[(Int, Int, M)])

case class T(as: List[A])

class MongoCaseClassSerializerTest extends FunSuite with Matchers with MongoTestCollection {

  def testToMongo[T <: AnyRef](t: T): Unit = {
    val dbo = MongoCaseClassSerializer.to(t)
    collection.insert(dbo)
  }

  test("toMongos") {
    // Currently this just tests that something gets written to Mongo, sans exceptions.
    // TODO(ryan): close the SerDe loop, verify the decoded contents match the originals.
    testToMongo(B(A(4)))
    testToMongo(C((3, A(4))))

    testToMongo(D(List((1, A(2)), (3, A(4)))))

    testToMongo(E((1, 2, A(3))))
    testToMongo(F(List((1, 2, A(3)), (4, 5, A(6)))))

    testToMongo(G((1L, 2, 3, A(4))))
    testToMongo(H(List((1L, 2, 3, A(4)), (5L, 6, 7, A(8)))))

    testToMongo(I("name", (1L, 2, 3, A(4))))
    testToMongo(J("name", List((1L, 2, 3, A(4)), (5L, 6, 7, A(8)))))

    testToMongo(N("name", List((1L, 2, 3, M(Some(A(4)))), (5L, 6, 7, M(Some(A(8)))))))
    testToMongo(O("name", List(M(Some(A(4))), M(Some(A(8))))))

    testToMongo(P(List((1L, 2, 3, M(Some(A(4)))), (5L, 6, 7, M(Some(A(8)))))))
    testToMongo(Q(List(M(Some(A(4))), M(Some(A(8))))))
    testToMongo(R(List((1, M(Some(A(4)))), (5, M(Some(A(8)))))))
    testToMongo(S(List((1, 2, M(Some(A(4)))), (5, 6, M(Some(A(8)))))))

    testToMongo(T(List(A(1),A(2),A(3))))
  }
}
