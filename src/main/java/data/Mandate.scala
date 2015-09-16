package data

class Mandate(name: String, legalForm: String, position: String) {
  override def toString: String = {
    return "Mandate[name=" + name + ",legalForm=" + legalForm + ",position=" + position + "]"
  }
}
