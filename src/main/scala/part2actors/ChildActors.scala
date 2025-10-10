package part2actors

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, Terminated}
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
    case class StopChild(name: String) extends Command
    case object WatchChild extends Command

    def apply(): Behavior[Command] = idle()
    def idle(): Behavior[Command] = Behaviors.receive { (context, message) =>
      message match {
        case CreateChild(name) =>
          context.log.info(s"[parent] Create child with name $name")
          // creatig child actor reference (used to send messages to this child)
          val childRef: ActorRef[String] = context.spawn(Child(), name)
          active(childRef)
        case _ =>
          Behaviors.same
      }
    }

    def active(childRef: ActorRef[String]): Behavior[Command] = Behaviors.receive[Command] { (context, message) =>
      message match {
        case TellChild(message) =>
          context.log.info(s"[parent] sending message [$message] to child")
          childRef ! message // <- send message to other actor
          Behaviors.same
        case StopChild(name: String) =>
          context.log.info(s"[parent] Stopping child '$name''")
          context.stop(childRef) // only works with child actors
          idle()
        case WatchChild =>
          context.log.info(s"[parent] Watching child")
          context.watch(childRef) // can use any ref, doesnt need to be child
          Behaviors.same
        case _ =>
          context.log.info("[parent] command not supported")
          Behaviors.same
      }
    }.receiveSignal {
      case (context, Terminated(refThatDied)) =>
        context.log.info(s"[parent] Child ${refThatDied.path} was killed by something")
        idle()
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
    case class StopChild(name: String) extends Command
    case class WatchChild(name: String) extends Command


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

    def active(children: Map[String, ActorRef[String]]): Behavior[Command] = Behaviors.receive[Command] { (context, message) =>
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
        case StopChild(name) =>
          context.log.info(s"[parent] stopping child with name $name")
          val childRef = children.get(name)
          childRef.fold(context.log.info("I dont have a child with that name"))(context.stop)
          active(children.removed(name))
        case WatchChild(name) =>
          context.log.info(s"[parent] Watching child")
          val childOption = children.get(name)
          childOption.fold(context.log.info("I dont have a child with that name"))(context.watch)
          Behaviors.same
      }
    }.receiveSignal {
      case (context, Terminated(diedRef)) =>
        val name = diedRef.path.name
        context.log.info(s"[parent] Child $name was killed by something")
        active(children.removed(name)) // doesnt work, rely on this and you get the dead letter error message
    }

  }

  def demoParentChild(): Unit = {
    import Parent._
    val userGuardianBehaviour: Behavior[Unit] = Behaviors.setup { context =>
      // set up all the important actors in application
      // setup the initial interaction
      val parent = context.spawn(Parent(), "parentActor")
      parent ! CreateChild("Sam")
      parent ! WatchChild
      parent ! TellChild("hey kid, you there?")
      parent ! StopChild("Sam")
      parent ! CreateChild("jsjs")
      parent ! TellChild("Same")

      // user guardian usually has no behavior of its own
      Behaviors.empty
    }

    val system = ActorSystem(userGuardianBehaviour, "DemoParentChild")
    Thread.sleep(1000)
    system.terminate()
  }

  def demoParentChild_v2(): Unit = {
    import Parent_v2._  
    val userGuardianBehaviour: Behavior[Unit] = Behaviors.setup { context =>
      // set up all the important actors in application
      // setup the initial interaction
      val parent = context.spawn(Parent_v2(), "parentActor")
      parent ! CreateChild("Sam")
      parent ! CreateChild("Nico")
      parent ! WatchChild("Sam")
      parent ! WatchChild("Nico")
      parent ! TellChild("Sam", "hey kid, you there?")
      parent ! StopChild("Sam")
      parent ! TellChild("Nico", "I love you dude!")
      parent ! TellChild("Sam", "you there?")

      // user guardian usually has no behavior of its own
      Behaviors.empty
    }

    val system = ActorSystem(userGuardianBehaviour, "DemoParentChild")
    Thread.sleep(1000)
    system.terminate()
  }

  def main(args: Array[String]): Unit = {
    demoParentChild_v2()
  }
}