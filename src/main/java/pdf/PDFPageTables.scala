package pdf

import technology.tabula.Table

object PDFPageTables {
  def extractFrom(file: String, from: Int, to: Int): List[Table] = {
    val document = PDFDocument.open(file)
    val tables = List.range(from, to)
      .map(pageNumber => document.page(pageNumber))
      .flatMap(page => page.tables())
    document.close()
    return tables
  }
}
