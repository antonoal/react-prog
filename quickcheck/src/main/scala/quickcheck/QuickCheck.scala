package quickcheck

import common._

import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }

  property("min2") = forAll { (a: Int, b: Int) =>
    val mn = Math.min(a, b)
    val mx = Math.max(a, b)
    val h = insert(b, insert(a, empty))
    findMin(h) == mn
    findMin(deleteMin(insert(mn, insert(mx, (insert(mn, empty)))))) == mn
  }

  property("delete1") = forAll { a: Int =>
    val h = deleteMin(insert(a, empty))
    h == empty
  }

  property("delete2") = forAll { (a: Int, b: Int) =>
    val mn = Math.min(a, b)
    val mx = Math.max(a, b)
    val h = insert(mx, insert(mn, insert(mn, empty)))
    findMin(deleteMin(deleteMin(h))) == mx
  }

  property("min3") = forAll { h: H =>
    def findAllMin(h: H, acc: List[A]): List[A] = {
      if (isEmpty(h)) acc.reverse
      else findAllMin(deleteMin(h), findMin(h) :: acc)
    }
    if (isEmpty(h)) true
    else {
      val l = findAllMin(h, Nil)
      l == l.sorted
    }
  }

  property("meld1") = forAll { (h1: H, h2: H) =>
    if (isEmpty(h1) && isEmpty(h2)) isEmpty(meld(h1, h2))
    else if (isEmpty(h1)) meld(h1, h2) == h1
    else if (isEmpty(h2)) meld(h1, h2) == h2
    else findMin(meld(h1, h2)) == Math.min(findMin(h1), findMin(h2))
  }

  property("meld2") = forAll { h: H =>
    if (isEmpty(h)) isEmpty(meld(h, empty))
    else findMin(meld(h, empty)) == findMin(h)
  }

  def count(h: H): Int = {
    def cntAcc(h:H, acc:Int): Int = {
      if (isEmpty(h)) acc
      else cntAcc(deleteMin(h), acc + 1)
    }
    cntAcc(h, 0)
  }

  property("meld3") = forAll {(h1: H, h2: H)=>
    val h = meld(h1, h2)
    count(h) == count(h1) + count(h2)
  }

  property("min4") = forAll { a: Int =>
    val h = insert(a, insert(a, empty))
    findMin(h) == findMin(deleteMin(h))
  }

  property("min5") = forAll { h: H =>
    val m = if (isEmpty(h)) 0
      else findMin(h)
    deleteMin(insert(m, h)) == h
    deleteMin(deleteMin(insert(m, (insert(m, h))))) == h
  }

  property("min5") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0
    else findMin(h)
    deleteMin(insert(m, h)) == h
    deleteMin(deleteMin(insert(m, (insert(m, h))))) == h
  }

  property("gen1") = forAll { (h: H) =>
    val m = if (isEmpty(h)) 0 else findMin(h)
    findMin(insert(m, h))==m
  }

  lazy val genHeap: Gen[H] = for {
    a <- arbitrary[A]
    h <- oneOf(const(empty), genHeap)
  } yield insert(a, h)

  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
