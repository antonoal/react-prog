/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import akka.event.LoggingReceive
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor with ActorLogging {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case op: Operation => root ! op
    case GC =>
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case GC => //ignoring
    case op: Operation => pendingQueue = pendingQueue.enqueue(op)
    case CopyFinished =>
      while (pendingQueue.nonEmpty) {
        val (msg, newQueue) = pendingQueue.dequeue
        pendingQueue = newQueue
        newRoot ! msg
      }
      root ! PoisonPill
      root = newRoot
      context.become(normal)
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  def getPos(currElem: Int, candElem: Int) = {
    require(currElem != candElem)
    if (candElem > currElem) Right else Left
  }

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor with ActorLogging {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def subtreeProcessor(op: Operation, noneProcessor: Position => Unit) = {
    val pos = getPos(this.elem, op.elem)
    subtrees.get(pos) match {
      case Some(st) => st ! op
      case None => noneProcessor(pos)
    }
  }

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case msg @ Insert(requester, id, elem) =>
      if (elem == this.elem) {
        removed = false
        requester ! OperationFinished(id)
      } else subtreeProcessor(msg, {
        pos =>
          subtrees += pos -> context.actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))
          requester ! OperationFinished(id)
      })
    case msg @ Remove(requester, id, elem) =>
      if (elem == this.elem) {
        removed = true
        requester ! OperationFinished(id)
      } else subtreeProcessor(msg, _ => requester ! OperationFinished(id))
    case msg @ Contains(requester, id, elem) =>
      if (elem == this.elem) requester ! ContainsResult(id, !removed)
      else subtreeProcessor(msg, _ => requester ! ContainsResult(id, false))
    case CopyTo(node) =>
      if (!removed) node ! Insert(self, 0, this.elem)
      val kids = subtrees.values.toSet
      kids.foreach(_ ! CopyTo(node))
      notifyParentIfCopied(kids, removed)
      context.become(copying(kids, removed))
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def notifyParentIfCopied(expected: Set[ActorRef], insertConfirmed: Boolean) = {
    if(expected.isEmpty && insertConfirmed) context.parent ! CopyFinished
  }

  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished =>
      val exp = expected - sender()
      notifyParentIfCopied(exp, insertConfirmed)
      context.become(copying(exp, insertConfirmed))
    case OperationFinished(_) =>
      notifyParentIfCopied(expected, true)
      context.become(copying(expected, true))
  }


}
