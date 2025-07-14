package part2actors

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object ChildActors {

  /*
    - actors can create other actors (children): parent -> child -> grandChild -> ...
                                                        -> child2 -> ...
    - actor hierarchy = tree-like structure
    - root of the hierarchy = "guardian" actor (created with the ActorSystem)
    - actors can be identified via a path: /user/parent/child/grandChild/
    - ActorSystem creates
      - the top-level (root) guardian, with children
        - system guardian (for Pekko internal messages)
        - user guardian (for our custom actors)
    - ALL OUR ACTORS are child actors of the user guardian
   */

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
      context.log.info(s"[child ${context.self.path.name}] Receive $message")
      Behaviors.same
    }
  }

  /**
   * Exercise: write a Parent_V2 that can manage MULTIPLE child actors.
   * @param args
   */
  object Parent_v2 {
    trait Command
    case class CreateChild(name: String) extends Command
    case class TellChild(childName: String, message: String) extends Command

    def apply(): Behavior[Command] =
//      Behaviors.receive { (context, message) =>
//      message match {
//        case CreateChild(name) =>
//          context.log.info(s"[parent] Create child with name $name")
//          // creatig child actor reference (used to send messages to this child)
//          val childRef: ActorRef[String] = context.spawn(Child(name), name)
//          active(Map(name -> childRef))
//      }
//    }
      active(Map())

    def active(children: Map[String, ActorRef[String]]): Behavior[Command] = Behaviors.receive { (context, message) =>
      message match {
        case CreateChild(name) =>
          context.log.info(s"[parent] Create child with name $name")
          // creatig child actor reference (used to send messages to this child)
          val childRef: ActorRef[String] = context.spawn(Child(), name)
          active(children.updated(name, childRef))
        case TellChild(childName, message) =>
          context.log.info(s"[parent] telling child '$childName' the message [$message]'")
          val childRef = children.get(childName)
//          childRef match {
//            case Some(foundChild) =>
//              foundChild ! message
//              Behaviors.same
//            case None =>
//              context.log.info("I dont have a child with that name")
//              Behaviors.same
//          }
          childRef.fold(context.log.info("I dont have a child with that name"))(_ ! message)
          Behaviors.same

      }
    }

  }

  def demoParentChild(): Unit = {
    import Parent_v2._
    val userGuardianBehaviour: Behavior[Unit] = Behaviors.setup { context =>
      // set up all the important actors in application
      // setup the initial interaction
      val parent = context.spawn(Parent_v2(), "parentActor")
      parent ! CreateChild("Sam")
      parent ! TellChild("Sam", "hey kid, you there?")
      parent ! CreateChild("Nico")
      parent ! TellChild("Nico", "I love you dude!")

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