package datatypes

case class ResultVO(ingestionTime: Long, data: String) {
  override def toString: String = {
    data
  }
}
