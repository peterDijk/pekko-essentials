package part2actors

import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}

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
    def apply(): Behavior[MasterProtocol] = ???
  }

  object WordCounterWorker {
    def apply(): Behavior[WorkerProtocol] = ???
  }

  object Aggregator {
    def apply(): Behavior[UserProtocol] = ???
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