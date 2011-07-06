package com.reportgrid

package object ct {
  implicit def mActions[M[_], A](m: M[A]): Actions[M, A] = new Actions[M, A] {
    override val value = m
  }
}

// vim: set ts=4 sw=4 et:
