package data

class Councillor(val name: String, party: String, profession: String) {
  var mandates = List[Mandate]()

  override def toString: String = {
    return "Councillor[name=" + name + ",party=" + party + ",profession=" + profession + ",mandates=[" + mandates + "]]"
  }
}