package data

case class Councillor(val name: String, party: String, profession: String) {
  var mandates = List[Mandate]()
}