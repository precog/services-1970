package com.reportgrid.analytics

import Periodicity._
import org.joda.time.DateTime

/**
 * A PeriodGrouping groups periods into a certain periodicity.
 */
trait PeriodicityGrouping {
  val group: PartialFunction[Periodicity, Periodicity]

  def expand(start: DateTime, end: DateTime): List[Period] = {
    def expand0(start: DateTime, end: DateTime, periodicity: Periodicity): List[Period] = if (start.getMillis >= end.getMillis) Nil else {
      def finer(periodicity: Periodicity): Option[Periodicity] = periodicity.previousOption.filter(group.isDefinedAt)

      def expandFiner(start: DateTime, end: DateTime, periodicity: Periodicity): List[Period] = {
        finer(periodicity) match {
          case Some(periodicity) => expand0(start, end, periodicity)

          case None => 
            val length = end.getMillis - start.getMillis
            val period = periodicity.period(start)

            if (length.toDouble / period.size.getMillis >= 0.5) period :: Nil
            else Nil
        }
      }

      (periodicity.period(start) to end).toList match {
        case Nil => error("Not possible")

        case period :: Nil => 
          expandFiner(start, end, periodicity)

        case spans => 
          val periodStart = spans.head
          val periodEnd   = spans.last

          val periodMiddle = spans.tail.init
          
          expandFiner(start, periodStart.end, periodicity) ++ periodMiddle ++ expandFiner(periodEnd.start, end, periodicity)
      }
    }

    expand0(start, end, Periodicity.Year)
  }
}

object PeriodicityGrouping {
  object Default extends PeriodicityGrouping {
    override val group : PartialFunction[Periodicity, Periodicity] = {
      case Minute   => Month
      case Hour     => Year
      case Day      => Year
      case Week     => Eternity
      case Month    => Eternity
      case Year     => Eternity
      case Eternity => Eternity
    }
  }
}
