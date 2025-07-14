package part2actors

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object ChildActors {

  // actors can create other actors (child)

  object Parent {
    trait Command
    case class CreateChild(name: String) extends Command
    case class TellChild(message: String) extends Command

    def apply(): Behavior[Command] = Behaviors.receive { (context, message) =>
      message match {
        case CreateChild(name) =>
          context.log.info(s"[parent] Create child with name $name")
          // creatig child actor reference (used to send messages to this child)
          val childRef: ActorRef[String] = context.spawn(Child(), name)
          active(childRef)
      }
    }

    def active(childRef: ActorRef[String]): Behavior[Command] = Behaviors.receive { (context, message) =>
      message match {
        case TellChild(message) =>
          context.log.info(s"[parent] sending message [$message] to child")
          childRef ! message // <- send message to other actor
          Behaviors.same

        case _ =>
          context.log.info("[parent] command not supported")
          Behaviors.same
      }
    }
  }

  object Child {
    def apply(): Behavior[String] = Behaviors.receive { (context, message) =>
      context.log.info(s"[child] Receive $message")
      Behaviors.same
    }
  }

  def demoParentChild(): Unit = {
    import Parent._
    val userGuardianBehaviour: Behavior[Unit] = Behaviors.setup { context =>
      // set up all the important actors in application
      // setup the initial interaction
      val parent = context.spawn(Parent(), "parent")
      parent ! CreateChild("child")
      parent ! TellChild("hey kid, you there?")

      // user guardian usually has no behavior of its own
      Behaviors.empty
    }

    val system = ActorSystem(userGuardianBehaviour, "DemoParentChild")
    Thread.sleep(1000)
    system.terminate()
  }

  def main(args: Array[String]): Unit = {
    demoParentChild()
  }
}