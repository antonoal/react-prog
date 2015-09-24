import quickcheck.{IntHeap, Bogus4BinomialHeap, BinomialHeap, QuickCheckHeap}

class MyHeap extends IntHeap with Bogus4BinomialHeap

val h = new MyHeap
val h1 = h.insert(3, h.insert(1, h.insert(1, h.empty)))
h.deleteMin(h1)
