package playground

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.extension.quartz.QuartzSchedulerTypedExtension

trait ScheduledMessages
case object Start extends ScheduledMessages
case object Tick extends ScheduledMessages
case object Stop extends ScheduledMessages

object QuartzSchedulePlayground {

  object ScheduledActor {
    def apply(scheduleCronExpression: String): Behavior[ScheduledMessages] = created(scheduleCronExpression: String)
    private def created(scheduleCronExpression: String): Behavior[ScheduledMessages] = Behaviors.setup { (context ) =>
      context.log.info(s"Actor created, starting scheduling...")

      // schedule messages with quartz
      val quartzScheduler = QuartzSchedulerTypedExtension(context.system)
      quartzScheduler.createTypedJobSchedule[ScheduledMessages](
        name = "myJobName_1",
        receiver = context.self,
        msg = Tick,
        cronExpression = scheduleCronExpression)

      active(scheduleCronExpression)
    }

    def active(scheduleCronExpression: String): Behavior[ScheduledMessages] = Behaviors.receive[ScheduledMessages] { (context, message) =>
      message match {
        case Tick =>
          context.log.info("Received a Tick...")
          // Can call whatever outside services here
          Behaviors.same
        case Stop =>
          context.log.info("Stopping...")
          stopped(scheduleCronExpression)
        case _ =>
          context.log.info("Unknown message")
          Behaviors.same
      }
    }.receiveSignal {
      case (context, signal) =>
        context.log.info(s"Received signal: $signal")
        Behaviors.same
    }

    def stopped(scheduleCronExpression: String): Behavior[ScheduledMessages] = Behaviors.setup { context =>
      context.log.info("Stopping scheduler...")
      val quartzScheduler = QuartzSchedulerTypedExtension(context.system)
      quartzScheduler.cancelJob(scheduleCronExpression)
      context.log.info("Scheduler stopped")
      Behaviors.same
    }

    def restarted(scheduleCronExpression: String): Behavior[ScheduledMessages] = Behaviors.setup { context =>
      context.log.info("Restarting scheduler...")
      val quartzScheduler = QuartzSchedulerTypedExtension(context.system)
      quartzScheduler.scheduleTyped(scheduleCronExpression, context.self, Tick)
      context.log.info("Scheduler restarted")
      active(scheduleCronExpression)
    }

  }

  def demoQuartzScheduling(): Unit = {
    val guardian: Behavior[Unit] = Behaviors.setup { context =>
      val scheduleCronExpression: String = "*/10 * * ? * *" // Will fire every ten seconds
      val scheduledActor = context.spawn(ScheduledActor(scheduleCronExpression), "scheduledActor")

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
