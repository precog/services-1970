

/**
 * A PeriodGrouping groups periods into a certain periodicity.
 */
case class PeriodicityGrouping(value: Map[Periodicity, Periodicity]) {
  val periodicities: Seq[Periodicity] = value.keys.toSeq.sort 

  def expand(start: DateTime, end: DateTime): List[Period] = {
    def span(start: DateTime, end: DateTime, periodicity: Periodicity): List[Period] = periodicity.period(start) to (periodicity.period(end))

    def finer(periodicity: Periodicity): Option[Periodicity] = periodicity.previous.filter(periodicities.contains _)
    def courser(periodicity: Periodicity): Option[Periodicity] = periodicity.next.filter(periodicities.contains _)

    def expand0(start: DateTime, end: DateTime, periodicity: Periodicity): List[Period] = {
      def expand0Finer(start: DateTime, end: DateTime, periodicity: Periodicity): List[Period] = {
        finer(periodicity) match {
          case Some(periodicity) => expand0(start, end, periodicity)

          case None => Nil
        }
      }

      span(start, end, periodicity) match {
        case _ :: Nil => expandFiner(start, end, periodicity)

        case list => 
          val periodStart = list.head
          val periodEnd   = list.last

          val periodMiddle = list.tail.init

          val substart1 = start
          val subend1   = periodStart.start

          val substart2 = end
          val subend2   = periodEnd.end

          expand0Finer(substart1, subend1) ++ periodMiddle ++ expand0Finer(substart2, subend2)
      }
    }

    expand0(start, end, Periodicity.Year)
  }
}

object PeriodicityGrouping {
  val Default = PeriodicityGrouping {
    Map(
      Minute   -> Month,
      Hour     -> Year,
      Day      -> Year,
      Week     -> Eternity,
      Month    -> Eternity,
      Year     -> Eternity,
      Eternity -> Eternity
    )
  }
}