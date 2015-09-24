package kvstore

import akka.actor._
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  case class AckTimeout(id: Long)
  case object PersistTimeout

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy

  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var schedules = Map.empty[Long, Cancellable]

  var persister = context.actorOf(persistenceProps)
  context.watch(persister)

  var pendingClients = Map.empty[Long, ActorRef]
  var pendingPersists = Map.empty[Long, Persist]
  var pendingReplicators = Map.empty[Long, Set[ActorRef]]

  var lastSnapSeq = -1L

  var isPrimary = false

  arbiter ! Join

  def scheduleTimeout(id: Long) =
    schedules += id -> context.system.scheduler.scheduleOnce(1.second, self, AckTimeout(id))
  def cancelTimeout(id: Long) = schedules(id).cancel()

  var _persScheduler: Cancellable = context.system.scheduler.schedule(100 millis, 100 millis, self, PersistTimeout)

  def persistOp(op: Operation) = {
    val msg = op match {
      case Insert(key, value, id) => Persist(key, Some(value), id)
      case Remove(key, id) => Persist(key, None, id)
    }
    pendingPersists += op.id -> msg
    persister ! msg
  }

  def update(op: Operation) = {
    op match {
      case Insert(key, value, _) => kv.get(key) match {
        case Some(_) => kv = kv.updated(key, value)
        case None => kv += key -> value
      }
      case Remove(key, _) => kv -= key
    }
    persistOp(op)
    replicateOp(op)
    pendingClients += op.id -> sender()
    scheduleTimeout(op.id)
  }

  def tryAcknowledge(id: Long, key: String = null) = {
    println("trying ack of id="+id)
    if (!pendingPersists.contains(id) && !pendingReplicators.contains(id)) {
      val msg = if (isPrimary) OperationAck(id) else SnapshotAck(key, id)
      pendingClients(id) ! msg
      pendingClients -= id
      cancelTimeout(id)
      println("ack'ed id="+id)
    }
  }

  def restartPersister() = {
    context.unwatch(persister)
    persister = context.actorOf(persistenceProps)
    pendingPersists.foreach(persister ! _._2)
    context.watch(persister)
  }

  def replicateOp(op: Operation) = if (!replicators.isEmpty) {
    val msg = op match {
      case Insert(key, value, id) => Replicate(key, Some(value), id)
      case Remove(key, id) => Replicate(key, None, id)
    }
    replicators foreach (_ ! msg)
    pendingReplicators += op.id -> replicators
  }

  def replicated(id: Long, replicator: ActorRef) = {
    println("******** Replicated id="+id)
    val reps = pendingReplicators(id)
    val newReps = reps - replicator
    if (newReps.isEmpty) {
      pendingReplicators -= id
      tryAcknowledge(id)
    }
    else pendingReplicators = pendingReplicators.updated(id, newReps)
  }

  def addReplica(r: ActorRef) = {
    val replicator = context.actorOf(Replicator.props(r))
    replicators += replicator
    secondaries += r -> replicator
    kv.zipWithIndex.map(t => Replicate(t._1._1, Some(t._1._2), t._2)) foreach (replicator !)
  }

  def removeReplica(r: ActorRef) = {
    val replicator = secondaries(r)
    context.stop(replicator)
    replicators -= replicator
    secondaries -= r
    for ((id, reps) <- pendingReplicators if reps.contains(replicator)) {
      replicated(id, replicator)
    }
  }

  def persisted(id: Long, key: String = null) = pendingPersists.get(id) match {
    case Some(_) =>
      pendingPersists -= id
      tryAcknowledge(id, key)
    case None => //ok
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case op: Operation => update(op)
    case Persisted(_, id) => persisted(id)
    case Terminated(actor) if actor == persister => restartPersister()
    case Replicas(rs) =>
      ((rs - self) -- secondaries.keys) foreach addReplica
      (secondaries.keys.toSet -- rs) foreach removeReplica
    case Replicated(key, id) =>
      if (pendingReplicators.contains(id)) replicated(id, sender())
    case AckTimeout(id) =>
      pendingClients(id) ! OperationFailed(id)
      pendingClients -= id
      pendingPersists -= id
      pendingReplicators -= id
    case PersistTimeout => pendingPersists foreach (persister ! _._2)
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) => sender ! GetResult(key, kv.get(key), id)
    case Terminated(actor) if actor == persister => restartPersister()
    case Snapshot(key, valueOption, seq) =>
      if (seq <= lastSnapSeq) sender() ! SnapshotAck(key, seq)
      else if ((seq - lastSnapSeq) == 1) {
        lastSnapSeq += 1
        val op = valueOption match {
          case Some(value) => Insert(key, value, seq)
          case None => Remove(key, seq)
        }
        update(op)
      }
    case Persisted(key, id) => persisted(id, key)
    case PersistTimeout => pendingPersists foreach (persister ! _._2)
  }

  def receive = {
    case JoinedPrimary   =>
      context.become(leader)
      isPrimary = true
    case JoinedSecondary => context.become(replica)
  }
}

