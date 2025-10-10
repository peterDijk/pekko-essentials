package part2actors

import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object Supervision {

  object FussyWordCounter {
    def apply(): Behavior[String] = active()
    def active(total: Int = 0): Behavior[String] = Behaviors.receive { (context, message) =>
      val wordCount = message.split(" ").length
      context.log.info(s"Received piece of text: '$message', counted $wordCount words, total: ${total + wordCount}")

      if (message.startsWith("Q")) throw new RuntimeException("I Hate queues!")
      if (message.startsWith("W")) throw new NullPointerException

      active(total + wordCount)
    }
  }

  // actor throwing exception gets killed

  def demoCrash(): Unit = {
    val guardian: Behavior[Unit] = Behaviors.setup { context =>
      val fussyCounter = context.spawn(FussyWordCounter(), "fussyCounter")

      fussyCounter ! "Starting to get it"
      fussyCounter ! "Quick!"
      fussyCounter ! "Are you there?"

      Behaviors.empty
    }

    val system = ActorSystem(guardian, "DemoCrash")
    Thread.sleep(1000)
    system.terminate()
  }

  def main(args: Array[String]): Unit = {
    demoCrash()

  }
}