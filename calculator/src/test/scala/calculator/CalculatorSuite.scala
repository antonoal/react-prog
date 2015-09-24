package calculator

import org.scalatest.FunSuite

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import org.scalatest._

import TweetLength.MaxTweetLength

@RunWith(classOf[JUnitRunner])
class CalculatorSuite extends FunSuite with ShouldMatchers {

  /******************
   ** TWEET LENGTH **
   ******************/

  def tweetLength(text: String): Int =
    text.codePointCount(0, text.length)

  test("tweetRemainingCharsCount with a constant signal") {
    val result = TweetLength.tweetRemainingCharsCount(Var("hello world"))
    assert(result() == MaxTweetLength - tweetLength("hello world"))

    val tooLong = "foo" * 200
    val result2 = TweetLength.tweetRemainingCharsCount(Var(tooLong))
    assert(result2() == MaxTweetLength - tweetLength(tooLong))
  }

  test("tweetRemainingCharsCount with a supplementary char") {
    val result = TweetLength.tweetRemainingCharsCount(Var("foo blabla \uD83D\uDCA9 bar"))
    assert(result() == MaxTweetLength - tweetLength("foo blabla \uD83D\uDCA9 bar"))
  }


  test("colorForRemainingCharsCount with a constant signal") {
    val resultGreen1 = TweetLength.colorForRemainingCharsCount(Var(52))
    assert(resultGreen1() == "green")
    val resultGreen2 = TweetLength.colorForRemainingCharsCount(Var(15))
    assert(resultGreen2() == "green")

    val resultOrange1 = TweetLength.colorForRemainingCharsCount(Var(12))
    assert(resultOrange1() == "orange")
    val resultOrange2 = TweetLength.colorForRemainingCharsCount(Var(0))
    assert(resultOrange2() == "orange")

    val resultRed1 = TweetLength.colorForRemainingCharsCount(Var(-1))
    assert(resultRed1() == "red")
    val resultRed2 = TweetLength.colorForRemainingCharsCount(Var(-5))
    assert(resultRed2() == "red")
  }

  test("compute delta on Polynomial") {
    val d0 = Polynomial.computeDelta(Var(1), Var(1), Var(1))
    assert(d0() == -3.0)
    val d1 = Polynomial.computeDelta(Var(1), Var(2), Var(1))
    assert(d1() == 0.0)
  }

  test("compute solution to a Polynomial") {
    val negDelta = Polynomial.computeSolutions(Var(0), Var(0), Var(0), Var(-1))
    assert(negDelta() == Set.empty)
    val s0 = Polynomial.computeSolutions(Var(1), Var(2), Var(1), Var(0))
    assert(s0() == Set(-1.0))
    val s1 = Polynomial.computeSolutions(Var(1), Var(1), Var(0), Var(1))
    assert(s1() == Set(0.0, -1.0))
  }

  test("Calculator") {
    val cycle0 = Calculator.computeValues(
      Map("a" -> Var(Plus(Ref("b"), Literal(1.0))), "b" -> Var(Times(Literal(2.0), Ref("a")))))
    assert(Double.NaN.equals(cycle0("a")()))
    val wrongRef = Calculator.computeValues(Map("a" -> Var(Plus(Ref("b"), Literal(1.0)))))
    assert(Double.NaN.equals(wrongRef("a")()))
    val simpleRes = Calculator.computeValues(
      Map("a" -> Var(Plus(Ref("b"), Literal(1.0))), "b" -> Var(Literal(2.0))))
    assert(simpleRes("a")() == 3.0)
    assert(simpleRes("b")() == 2.0)
  }

}
