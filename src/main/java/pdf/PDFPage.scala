package pdf

import scala.collection.JavaConversions._

import technology.tabula.extractors.BasicExtractionAlgorithm
import technology.tabula.{Page, Table}

class PDFPage(private val page: Page) {
  def tables(): List[Table] = {
    val extractionAlgorithm = new BasicExtractionAlgorithm()
    return extractionAlgorithm.extract(page).toList
  }
}