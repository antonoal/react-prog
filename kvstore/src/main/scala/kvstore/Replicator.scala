package kvstore

import akka.actor._
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  case object Timeout

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
//  var pending = Vector.empty[Snapshot]
  var pending = Map.empty[Long, Snapshot]
  var pendingSnaps = Map.empty[Long, Long]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  var _scheduler: Cancellable = context.system.scheduler.schedule(100 millis, 100 millis, self, Timeout)

  /* TODO Behavior for the Replicator. */
  def receive: Receive = {
    case msg@Replicate(key, valOpt, id) =>
      acks += id -> (sender(), msg)
      val snap = Snapshot(key, valOpt, nextSeq)
      pending += snap.seq -> snap
      pendingSnaps += snap.seq -> id
      replica ! snap
    case SnapshotAck(key, seq) =>
      for {
        id <- pendingSnaps.get(seq)
        (prim, _) <- acks.get(id)
      } {
        prim ! Replicated(key, id)
        pendingSnaps -= seq
        acks -= id
        pending -= seq
      }
    case Timeout => pending foreach (replica ! _._2)
  }

}
