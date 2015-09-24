package calculator

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Var(b() * b() - 4 * a() * c())
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = Var {
    if (delta() < 0) Set.empty[Double]
    else {
      val d = Math.sqrt(delta())
      Set((-1 * b() + d) / (2 * a()), (-1 * b() - d) / (2 * a()))
    }
  }
}
