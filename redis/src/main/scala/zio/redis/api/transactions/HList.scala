package zio.redis.api.transactions

import scala.annotation.tailrec

sealed trait HList { self =>
  import HList._

  def concat(right: HList): HList =
    self match {
      case HCons(head, tail) => HCons(head, tail.concat(right))
      case HNil              => right
    }

  def reverse: HList = {
    @tailrec
    def go(hlist: HList, acc: HList): HList =
      hlist match {
        case HCons(head, tail) => go(tail, HCons(head, acc))
        case HNil              => acc
      }

    go(this, HNil)
  }
}

object HList {
  final case class HCons[A](elem: A, rest: HList) extends HList
  case object HNil                                extends HList

  def single[A](elem: A): HList = HCons(elem, HNil)
}
