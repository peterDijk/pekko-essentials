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
    val actorSystem = ActorSystem[String](SimpleActor_v3(), "FirstActorSystem")

    // part 3: Communicate
    actorSystem ! "Pekko is bad"

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

  object Person {
    def happy(): Behavior[String] = Behaviors.receive { (context, message) =>
      message match {
        case "Pekko is bad" =>
          context.log.info("dont say bad on pekko")
          sad()
        case _ =>
          context.log.info(s"I've received --$message. That's great!")
          Behaviors.same
      }
    }
    def sad(): Behavior[String] = Behaviors.receive { (context, message) =>
      message match {
        case "Pekko is awesome" =>
          context.log.info("knew you'd like it")
          happy()
        case _ =>
          context.log.info(s"I've received --$message. That sucks")
          Behaviors.same
      }
    }

    def apply() = happy()
  }

  object WeirdActor {
    def apply(): Behavior[Any] = Behaviors.receive{ (context, message) =>
      message match {
        case aInt: Int =>
          context.log.info(s"I received an Int: $aInt")
          Behaviors.same
        case sString: String =>
          context.log.info(s"I received a String: $sString")
          Behaviors.same
      }
    }
  }

  object BetterActor {
    trait Message
    case class IntMessage(number: Int) extends Message
    case class StringMessage(aString: String) extends Message

    def apply(): Behavior[Message] = Behaviors.receive { (context, message) =>
      message match {
        case IntMessage(aNumber) =>
          context.log.info(s"I received an Int: $aNumber")
          Behaviors.same
        case StringMessage(aString) =>
          context.log.info(s"I received a String: $aString")
          Behaviors.same
      }
    }
  }

  def testPerson(): Unit =
    val person = ActorSystem(Person(), "PersonTest")

    person ! "I love blue"
    person ! "Pekko is bad"
    person ! "I also love red"
    person ! "Pekko is awesome"
    person ! "Pekko is ok"

    Thread.sleep(1000)
    person.terminate()

  def demoBetterActor(): Unit = {
    import BetterActor._
    val actor = ActorSystem(BetterActor(), "ActorTest")

    actor ! IntMessage(43)
    actor ! StringMessage("Hoooiiiii")
  }

  def main(args: Array[String]): Unit = {
//    demoSimpleActor()
//    testPerson()
    demoBetterActor()
  }
}