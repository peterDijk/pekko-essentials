package part2actors

import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors

object ActorState {

  /*
  use setup method to create a word counter which
  - split each message into words
  - keep track of the total number of words
  - log the current number of words + the total # of words
   */

   object WordCounter {
     def apply(): Behavior[String] = Behaviors.setup { context =>
     var totalWords: Int = 0

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

   def wordActorSystem(): Unit = {
     def actorSystem = ActorSystem[String](WordCounter(), "WordCounterSystem")

     actorSystem ! "This is a few little words"
     actorSystem ! "Another couple of words"
     actorSystem ! "Count me then!"

     Thread.sleep(1000)
     actorSystem.terminate()
   }

  def main(args: Array[String]): Unit = {
    wordActorSystem()


  }
}