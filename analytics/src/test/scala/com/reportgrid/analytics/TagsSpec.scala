package com.reportgrid.analytics

import org.specs.Specification

class TagsSpec extends Specification {
  "Hiearchy.respectsRefinementRule" should {
    "accept a valid simple hierarchy" in {
      val valid = List(
        com.reportgrid.analytics.Path("/foo"),
        com.reportgrid.analytics.Path("/foo/bar"),
        com.reportgrid.analytics.Path("/foo/bar/baz")
      )

      Hierarchy.respectsRefinementRule(valid) must beTrue
    }

    "accept a valid hierarchy with gaps" in {
      val valid = List(
        com.reportgrid.analytics.Path("/foo"),
        com.reportgrid.analytics.Path("/foo/bar"),
        com.reportgrid.analytics.Path("/foo/bar/baz/gweep")
      )

      Hierarchy.respectsRefinementRule(valid) must beTrue
    }

    "detect refinement rule violations" in {
      val invalid = List(
        com.reportgrid.analytics.Path("/foo"),
        com.reportgrid.analytics.Path("/foo/bar"),
        com.reportgrid.analytics.Path("/foo/baz")
      )

      Hierarchy.respectsRefinementRule(invalid) must beFalse
    }
  }
}

// vim: set ts=4 sw=4 et:
