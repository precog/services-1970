package com.reportgrid.analytics

import Periodicity._
import org.joda.time.DateTime

/**
 * A PeriodGrouping groups periods into a certain periodicity.
 */
trait PeriodicityGrouping {
  val group: PartialFunction[Periodicity, Periodicity]

  def expand(start: DateTime, end: DateTime): List[Period] = {
    def span(start: DateTime, end: DateTime, periodicity: Periodicity): List[Period] = (periodicity.period(start) to (periodicity.period(end))).toList

    def finer(periodicity: Periodicity): Option[Periodicity] = periodicity.previousOption.filter(group.isDefinedAt)

    def expand0(start: DateTime, end: DateTime, periodicity: Periodicity): List[Period] = {
      def expandFiner(start: DateTime, end: DateTime, periodicity: Periodicity): List[Period] = {
        finer(periodicity) match {
          case Some(periodicity) => expand0(start, end, periodicity)

          case None => Nil
        }
      }

      span(start, end, periodicity) match {
        case Nil => error("Not possible")

        case _ :: Nil => expandFiner(start, end, periodicity)

        case list => 
          val periodStart = list.head
          val periodEnd   = list.last

          val periodMiddle = list.tail.init

          val substart1 = start
          val subend1   = periodStart.start

          val substart2 = end
          val subend2   = periodEnd.end

          expandFiner(substart1, subend1, periodicity) ++ periodMiddle ++ expandFiner(substart2, subend2, periodicity)
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
