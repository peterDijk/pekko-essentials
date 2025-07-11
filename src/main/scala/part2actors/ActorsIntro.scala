package part2actors

import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object ActorsIntro {

  // part 1
  // behaviour
  val simpleActorBehavior: Behavior[String] = Behaviors.receiveMessage{ (message: String) =>
    println(s"[simple actor] I have received: $message")

    // new behavior for the NEXT message
    Behaviors.same
  }

  def demoSimpleActor(): Unit = {
    // part 2: Instantiate
    val actorSystem = ActorSystem[String](SimpleActor_v2(), "FirstActorSystem")

    // part 3: Communicate
    actorSystem ! "I am learning Pekko"
//    actorSystem.tell("I am learning Pekko")

    // part 4: gracefully shut down
    Thread.sleep(1000)
    actorSystem.terminate()
  }

  object SimpleActor {
    def apply(): Behavior[String] = Behaviors.receiveMessage{ (message: String) =>
      println(s"[simple actor] I have received: $message")

      // new behavior for the NEXT message
      Behaviors.same
    }
  }

  object SimpleActor_v2 {
    def apply(): Behavior[String] = Behaviors.receive { (context, message) =>
      // context is a data structure that has access to a lot of APIs
      // simple example: logging
      context.log.info(s"[simple actor] I have received: $message")
      Behaviors.same
    }
  }

  object SimpleActor_v3 {
    def apply(): Behavior[String] = Behaviors.setup { (context) =>
      // for actor "private" data and behaviour
      // YOUR CODE WHATEVER

      Behaviors.receiveMessage { message =>
        context.log.info(s"[simple actor] I have received: $message")
        Behaviors.same
      }
    }
  }

  def main(args: Array[String]): Unit = {
    demoSimpleActor()
  }
}