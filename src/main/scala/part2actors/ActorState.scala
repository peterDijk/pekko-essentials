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
        totalWords += countWords
        context.log.info(s"The current number of words is: $countWords")
        context.log.info(s"The total number of words is now: $totalWords")
        Behaviors.same
      }
    }
  }

  object WordCounter_v2 {
    def apply() = statelessCounter(0)
    def statelessCounter(totalWords: Int): Behavior[String] = Behaviors.receive { (context, message) =>
      val splitWords = message.split(" ")
      val countWords: Int = splitWords.length
      val newTotal = totalWords + countWords
      context.log.info(s"The current number of words is: $countWords")
      context.log.info(s"The total number of words is now: $newTotal")
      statelessCounter(newTotal)
    }
  }

  def wordActorSystem(): Unit = {
    val actorSystem = ActorSystem(WordCounter_v2(), "WordCounterSystem")

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

  object SimpleHuman_v2 {
    def apply(): Behavior[SimpleThing] = statelessSimpleHuman(0)

    def statelessSimpleHuman(happiness: Int): Behavior[SimpleThing] = Behaviors.receive { (context, message) =>
      message match {
        case EatChocolate =>
          context.log.info(s"[$happiness] Eating chocolate")
          statelessSimpleHuman(happiness + 1)
        case CleanUpTheFloor =>
          context.log.info(s"[$happiness] Wiping the floor, ugh...")
          statelessSimpleHuman(happiness - 2)
        case LearnPekko =>
          context.log.info(s"[$happiness] Learning Pekko, YAY!")
          statelessSimpleHuman(happiness + 99)
      }
    }
  }

  def demoSimpleHuman(): Unit = {
    val human = ActorSystem(SimpleHuman_v2(), "DemoSimpleHuman")

    human ! LearnPekko
    human ! EatChocolate
    (1 to 30).foreach(_ => human ! CleanUpTheFloor)

    Thread.sleep(1000)
    human.terminate()
  }

  def main(args: Array[String]): Unit = {
    wordActorSystem()
//      demoSimpleHuman()
  }
}