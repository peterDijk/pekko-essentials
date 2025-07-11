package part2actors

import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{Behaviors}

object ActorState {

  /*
  use setup method to create a word counter which
  - split each message into words
  - keep track of the total number of words
  - log the current number of words + the total # of words
   */

   object WordCounter {
     def apply(): Behavior[String] = Behaviors.setup { context =>
     var totalWords = 0

       Behaviors.receiveMessage { message =>
         val splitWords = message.split(" ")
         val countWords: Int = splitWords.length
         totalWords += 1
         context.log.info(s"The current number of words is: $countWords")
         context.log.info(s"The total number of words is now: $totalWords")
         Behaviors.same
       }
     }
   }

   def wordActorSystem(): Unit = {
     def actorSystem = ActorSystem[String](WordCounter(), "WordCounterSystem")

     actorSystem ! "This is a few little words"
     actorSystem ! "Another couple of words"
     actorSystem ! "Count me then!"

     Thread.sleep(1000)
     actorSystem.terminate()
   }

  trait SimpleThing
  case object EatChocolate extends SimpleThing
  case object CleanUpTheFloor extends SimpleThing
  case object LearnPekko extends SimpleThing
  /*
    Message types must be IMMUTABLE and SERIALIZABLE.
    - use case classes/objects
    - use a flat type hierarchy
   */

  object SimpleHuman {
    def apply(): Behavior[SimpleThing] = Behaviors.setup { context =>
      var happiness = 0

      Behaviors.receiveMessage {
        case EatChocolate =>
          context.log.info(s"[$happiness] Eating chocolate")
          happiness += 1
          Behaviors.same
        case CleanUpTheFloor =>
          context.log.info(s"[$happiness] Wiping the floor, ugh...")
          happiness -= 2
          Behaviors.same
        case LearnPekko =>
          context.log.info(s"[$happiness] Learning Pekko, YAY!")
          happiness += 99
          Behaviors.same
      }
    }
  }

  def demoSimpleHuman(): Unit = {
    val human = ActorSystem(SimpleHuman(), "DemoSimpleHuman")

    human ! LearnPekko
    human ! EatChocolate
    (1 to 30).foreach(_ => human ! CleanUpTheFloor)

    Thread.sleep(1000)
    human.terminate()
  }

  def main(args: Array[String]): Unit = {
    wordActorSystem()
//  demoSimpleHuman()

  }
}