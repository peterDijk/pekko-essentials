package part2actors

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import part2actors.ActorState.WordCounter_v2.statelessCounter

object ChildActorsExercise {

  /**
   * Exercise: distributed word counting
   * task 1: child 1
   * task 2: child 2
   * task 10: child 10
   * task 11: child 1
   * task 19: child 9
   * task 21: child 1
   */

  trait MasterProtocol
  trait WorkerProtocol
  trait UserProtocol

  // master messages
  case class Initialize(nChildren: Int) extends MasterProtocol
  case class WordCountTask(text: String, replyTo: ActorRef[UserProtocol]) extends MasterProtocol
  case class WordCountReply(id: Int, count: Int) extends MasterProtocol

  // worker messages
  case class WorkerTask(id: Int, text: String) extends WorkerProtocol

  // requester (user) messages
  case class Reply(count: Int) extends UserProtocol

  object WordCounterMaster {
    def apply(): Behavior[MasterProtocol] = active(Map())
    def active(workers: Map[Int, (ActorRef[WorkerProtocol], Option[ActorRef[UserProtocol]])]): Behavior[MasterProtocol] = Behaviors.receive { (context, message) =>
      message match {
        case Initialize(nChildren) =>
          // use % nChildren?
          val range = 1 to nChildren
          val initWorkers = range.foldLeft(Map.empty[Int, (ActorRef[WorkerProtocol], Option[ActorRef[UserProtocol]])]) { (acc, id) =>
            context.log.info(s"spawning worker with id ${id}")
            val worker = context.spawn(WordCounterWorker(), s"worker-$id")
            acc.updated(id, (worker, None))
          }
          active(initWorkers)
        case WordCountTask(text, replyTo) =>
          val workerId = 1 // ??? % workers.length?
          val workerOption = workers.get(workerId)
          workerOption.fold(context.log.info(s"worker with id ${workerId} not found"))(
            (worker, replyTo) => worker ! WorkerTask(workerId, text)
          )
          Behaviors.same
        case WordCountReply(id, count) =>
          val workerOption = workers.get(id)
          workerOption.foreach(workerTuple =>
            val (worker, replyTo) = workerTuple
            replyTo.foreach(_ ! Reply(count)))
          Behaviors.same
      }
    }
  }

  object WordCounterWorker {
    def apply(): Behavior[WorkerProtocol] = Behaviors.receive { (context, message) =>
      message match {
        case WorkerTask(id, text) =>
          val splitWords = text.split(" ")
          val countWords: Int = splitWords.length
          context.log.info(s"[worker-$id] The current number of words is: $countWords")
          val parent: ActorRef[MasterProtocol] = context.self.path.parent.asInstanceOf[ActorRef[MasterProtocol]]
          // report back the found count to WCM
          parent ! WordCountReply(id, countWords)
          Behaviors.same
      }

    }
  }

  object Aggregator {
    def apply(): Behavior[UserProtocol] = active()
    def active(totalWords: Int = 0): Behavior[UserProtocol] = Behaviors.receive { (context, message) =>
      message match {
        case Reply(count) =>
          context.log.info(s"[aggregator] I've received $count, total is ${totalWords + count}")
          active(totalWords + count)
      }
    }
  }

  def testWordCounter(): Unit = {
    def userGuardian: Behavior[Unit] = Behaviors.setup { context =>
      val aggregator = context.spawn(Aggregator(), "aggregator")
      val wcm = context.spawn(WordCounterMaster(), "master")

      wcm ! Initialize(3)
      wcm ! WordCountTask("I love Pekko", aggregator)
      wcm ! WordCountTask("another sentence", aggregator)

      Behaviors.empty
    }

    val system = ActorSystem(userGuardian, "WordCounterSystem")
    Thread.sleep(1000)
    system.terminate()
  }

  def main(args: Array[String]): Unit = {
    testWordCounter()
  }
}