package pdf

import org.apache.pdfbox.pdmodel.PDDocument
import technology.tabula.ObjectExtractor

class PDFDocument(private val document: PDDocument) {
  def close() = {
    document.close()
  }

  def page(number: Int): PDFPage = {
    val extractor = new ObjectExtractor(document)
    return new PDFPage(extractor.extract(number))
  }
}

object PDFDocument {
  def open(file: String): PDFDocument = {
    val document = PDDocument.load(file)
    return new PDFDocument(document)
  }
}
