package part3testing

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.{ActorRef, Behavior}

import scala.concurrent.duration.*
import org.scalatest.wordspec.AnyWordSpecLike

class EssentialTestingSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike {

  "A simple actor" should {
    // test suite
    import EssentialTestingSpec.*

    "send back a duplicated message" in {
      // code for testing
      val simpleActor = testKit.spawn(SimpleActor(), "simpleActor1")
      val probe = testKit.createTestProbe[SimpleProtocol]() // "inspector for message replies"

      // scenario
      simpleActor ! SimpleMessage("I love Pekko", probe.ref)

      probe.expectMessage(SimpleReply("Received: I love Pekko"))
    }
  }

  "A black hole actor" should {
    import EssentialTestingSpec.*

    "not respond to any messages" in {
      val blankActor = testKit.spawn(BlankActor(), "blankActor1")
      val probe = testKit.createTestProbe[SimpleProtocol]()

      blankActor ! SimpleMessage("Hello?", probe.ref)

      probe.expectNoMessage(1.second)
    }
  }

  "A simple actor with a separate test suite" should {
    import EssentialTestingSpec.*
    val simpleActor = testKit.spawn(SimpleActor(), "simpleActor")
    val probe = testKit.createTestProbe[SimpleProtocol]()

    "uppercase a string message" in {
      simpleActor ! UpperCaseString("make me uppercase", probe.ref)

      val receivedMessage = probe.expectMessageType[SimpleReply]

      assert(receivedMessage.contents == receivedMessage.contents.toUpperCase()) // Scala standard assertion
      receivedMessage.contents should be ("MAKE ME UPPERCASE") // ScalaTest library assertion
    }

    "send back multiple favorite tech messages" in {
      simpleActor ! FavoriteTech(probe.ref)

      val messages: Seq[SimpleProtocol] = probe.receiveMessages(2, 1.second)
      val replies = messages.collect {
        case SimpleReply(contents) => contents
      }

      messages.map {
        case SimpleReply(contents) => contents
      } should contain allOf ("Pekko", "Scala")

      replies should contain allOf ("Pekko", "Scala")
    }
  }
}

object EssentialTestingSpec {
  // code under test for convenience
  trait SimpleProtocol
  case class SimpleMessage(message: String, replyTo: ActorRef[SimpleProtocol]) extends SimpleProtocol
  case class UpperCaseString(message: String, replyTo: ActorRef[SimpleProtocol]) extends SimpleProtocol
  case class FavoriteTech(replyTo: ActorRef[SimpleProtocol]) extends SimpleProtocol
  case class SimpleReply(contents: String) extends SimpleProtocol

  object SimpleActor {
    def apply(): Behavior[SimpleProtocol] =
      Behaviors.receiveMessage {
        case SimpleMessage(message, sender) =>
          sender ! SimpleReply(s"Received: $message")
          Behaviors.same
        case UpperCaseString(message, sender) =>
          sender ! SimpleReply(message.toUpperCase)
          Behaviors.same
        case FavoriteTech(replyTo) =>
          replyTo ! SimpleReply("Pekko")
          replyTo ! SimpleReply("Scala")
          Behaviors.same
      }
  }

  object BlankActor {
    def apply(): Behavior[SimpleProtocol] =
      Behaviors.ignore
  }

}