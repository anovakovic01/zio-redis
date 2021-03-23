package zio.redis.api.transactions

sealed trait HList { self =>
  import HList._

  def concat(right: HList): HList =
    self match {
      case HCons(elem, rest) => HCons(elem, rest.concat(right))
      case HNil              => right
    }
}

object HList {
  final case class HCons[A](elem: A, rest: HList) extends HList
  case object HNil                                extends HList
}
