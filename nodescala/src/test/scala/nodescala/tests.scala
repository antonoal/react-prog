package nodescala

import scala.language.postfixOps
import scala.util.{Try, Success, Failure}
import scala.collection._
import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.async.Async.{async, await}
import org.scalatest._
import NodeScala._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NodeScalaSuite extends FunSuite {

  test("A Future should always be completed") {
    val always = Future.always(517)

    assert(Await.result(always, 0 nanos) == 517)
  }
  test("A Future should never be completed") {
    val never = Future.never[Int]
    try {
      Await.result(never, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => // ok!
    }
  }
  test("All futures should complete") {
    val all0 = Future.all(List(Future { 1 }, Future { 2 }))
    assert(Await.result(all0, 1 second) == List(1, 2))

    val all1 = Future.all(List(Future { 1 }, Future { throw new Exception ("Boom!") }))
    try {
      Await.result(all1, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => assert(false)
      case t: Exception => assert(t.getMessage.contains("Boom!"))
    }

    val all2 = Future.all(List(Future { 1 }, Future.never))
    try {
      Await.result(all2, 1 second)
      assert(false)
    } catch {
      case t: TimeoutException => //ok
    }
  }

  test("Any should complete or fail the first Future") {
    val any0 = Future.any(List(Future { Thread.sleep(100000) }, Future { throw new Exception }))
    try {
      Await.result(any0, 1 second)
      assert(false)
    } catch {
      case t: Exception => //ok
    }

    val any2 = Future.any(List(Future.never, Future { 1 }))
    assert(Await.result(any2, 1 second) == 1)
  }

  test("Delay") {
    val res0 = Future.delay(5 second)
    try {
      Await.result(res0, 2 second)
      assert(false)
    } catch {
      case t: TimeoutException => //ok
    }

    val res1 = Future.delay(10 millis)
    Await.result(res1, 100 millis) //shouldn't throw
  }

  test("Now") {
    val f0 = Future { 1 }
    Await.result(f0, 1 second)
    assert(f0.now == 1)
    try {
      Future.never.now
    } catch {
      case t: NoSuchElementException => //ok
    }
  }

  test("ContinueWith Future") {
    val f0 = Future { 1 }
    val f1 = f0.continueWith(f => Await.result(f, 1 second))
    assert(Await.result(f1, 1 second) == 1)
  }

  test("ContinueWith Try") {
    val f0 = Future { 1 }
    val f1 = f0.continue(t => t.get)
    assert(Await.result(f1, 1 second) == 1)
  }

  class DummyExchange(val request: Request) extends Exchange {
    @volatile var response = ""
    val loaded = Promise[String]()
    def write(s: String) {
      response += s
    }
    def close() {
      loaded.success(response)
    }
  }

  class DummyListener(val port: Int, val relativePath: String) extends NodeScala.Listener {
    self =>

    @volatile private var started = false
    var handler: Exchange => Unit = null

    def createContext(h: Exchange => Unit) = this.synchronized {
      assert(started, "is server started?")
      handler = h
    }

    def removeContext() = this.synchronized {
      assert(started, "is server started?")
      handler = null
    }

    def start() = self.synchronized {
      started = true
      new Subscription {
        def unsubscribe() = self.synchronized {
          started = false
        }
      }
    }

    def emit(req: Request) = {
      val exchange = new DummyExchange(req)
      if (handler != null) handler(exchange)
      exchange
    }
  }

  class DummyServer(val port: Int) extends NodeScala {
    self =>
    val listeners = mutable.Map[String, DummyListener]()

    def createListener(relativePath: String) = {
      val l = new DummyListener(port, relativePath)
      listeners(relativePath) = l
      l
    }

    def emit(relativePath: String, req: Request) = this.synchronized {
      val l = listeners(relativePath)
      l.emit(req)
    }
  }

  test("Server should serve requests") {
    val dummy = new DummyServer(8191)
    val dummySubscription = dummy.start("/testDir") {
      request => for (kv <- request.iterator) yield (kv + "\n").toString
    }

    // wait until server is really installed
    Thread.sleep(500)

    def test(req: Request) {
      val webpage = dummy.emit("/testDir", req)
      val content = Await.result(webpage.loaded.future, 1 second)
      val expected = (for (kv <- req.iterator) yield (kv + "\n").toString).mkString
      assert(content == expected, s"'$content' vs. '$expected'")
    }

    test(immutable.Map("StrangeRequest" -> List("Does it work?")))
    test(immutable.Map("StrangeRequest" -> List("It works!")))
    test(immutable.Map("WorksForThree" -> List("Always works. Trust me.")))

    dummySubscription.unsubscribe()
  }

}




