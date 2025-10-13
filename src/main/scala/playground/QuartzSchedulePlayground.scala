package playground

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.extension.quartz.QuartzSchedulerTypedExtension

trait ScheduledMessages
case object Start extends ScheduledMessages
case object Tick extends ScheduledMessages
case object Stop extends ScheduledMessages

object QuartzSchedulePlayground {

  object ScheduledActor {
    def startScheduler(scheduleName: String, scheduleCronExpression: String, context: ActorContext[ScheduledMessages]): Unit = {
      val quartzScheduler = QuartzSchedulerTypedExtension(context.system)
      quartzScheduler.createTypedJobSchedule[ScheduledMessages](
        name = scheduleName,
        receiver = context.self,
        msg = Tick,
        cronExpression = scheduleCronExpression)
    }

    def apply(scheduleName: String, scheduleCronExpression: String): Behavior[ScheduledMessages] = created(scheduleName, scheduleCronExpression)
    private def created(scheduleName: String, scheduleCronExpression: String): Behavior[ScheduledMessages] = Behaviors.setup { (context ) =>
      context.log.info(s"Actor created, starting scheduling...")

      // schedule messages with quartz
      startScheduler(
        scheduleName,
        scheduleCronExpression,
        context
      )

      active(scheduleName, scheduleCronExpression)
    }

    def active(scheduleName: String, scheduleCronExpression: String): Behavior[ScheduledMessages] = Behaviors.receive[ScheduledMessages] { (context, message) =>
      message match {
        case Tick =>
          context.log.info("Received a Tick...")
          // Can call whatever outside services here
          Behaviors.same
        case Stop =>
          context.log.info("Stopping scheduler...")
          val quartzScheduler = QuartzSchedulerTypedExtension(context.system)
          quartzScheduler.cancelJob(scheduleName)
          context.log.info("Scheduler stopped")
          stopped(scheduleName, scheduleCronExpression)
        case _ =>
          context.log.info("Unknown message")
          Behaviors.same
      }
    }.receiveSignal {
      case (context, signal) =>
        context.log.info(s"Received signal: $signal")
        Behaviors.same
    }

    def stopped(scheduleName: String, scheduleCronExpression: String): Behavior[ScheduledMessages] = Behaviors.receive[ScheduledMessages] { (context, message) =>
      message match {
        case Start =>
          // schedule messages with quartz
          startScheduler(
            scheduleName,
            scheduleCronExpression,
            context
          )

          active(scheduleName, scheduleCronExpression)
      }
    }
  }

  def demoQuartzScheduling(): Unit = {
    val guardian: Behavior[Unit] = Behaviors.setup { context =>
      val scheduleCronExpression: String = "*/10 * * ? * *" // Will fire every ten seconds
      val scheduledActor = context.spawn(ScheduledActor("jobName_1", scheduleCronExpression), "scheduledActor")

      Behaviors.empty
    }

    val system = ActorSystem(guardian, "QuartzSchedulingDemo")
//    Thread.sleep(20000)
//    system.terminate()
  }

  def main(args: Array[String]): Unit = {
    demoQuartzScheduling()
  }
}
