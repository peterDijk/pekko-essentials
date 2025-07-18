package part2actors

import org.apache.pekko.actor.typed.{ActorSystem, Behavior, PostStop}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object StoppingActors {

  object SensitiveActor {
    def apply(): Behavior[String] = Behaviors.receive[String] { (context, message) =>
      context.log.info(s"Received: $message")
      if (message == "you're ugly")
//        Behaviors.stopped // optionally pass a () => Unit to clear up resources after the actor is stopped
        Behaviors.stopped(() => context.log.info("I'm stopped now"))
      else
        Behaviors.same
    }
      .receiveSignal {
        case (context, PostStop) =>
          // clean up resources that actor might use
          context.log.info(s"I'm stopping")
          Behaviors.same
      }
  }

  def main(args: Array[String]): Unit = {
    val userGuardian = Behaviors.setup[Unit] { context =>
      val sensitiveActor = context.spawn(SensitiveActor(), "sensitiveActor")

      sensitiveActor ! "hi"
      sensitiveActor ! "whats up"
      sensitiveActor ! "you're ugly"
      sensitiveActor ! "sorry for telling"

      Behaviors.same
    }

    val system = ActorSystem[Unit](userGuardian, "system")
    Thread.sleep(1000)
    system.terminate()

  }
}