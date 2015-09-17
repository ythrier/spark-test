package data

import java.util

import pdf.PDFPageTables
import technology.tabula.{HasText, RectangularTextContainer, Table}

import scala.collection.JavaConversions._

class InputFile(val path: String, val from: Int, val to: Int)

object CouncillorDataReader {
  private val numberOfCouncillorInformationRows = 3

  def extractFromFiles(files: List[InputFile]): List[Councillor] = {
    var councillors = List[Councillor]()
    files.foreach {
      file => {
        councillors :::= extractFromFile(file.path, file.from, file.to)
      }
    }
    return councillors
      .groupBy(councillor => councillor.name)
      .map(group => groupMandatesFromDuplicateCouncillors(group._2))
      .toList
  }

  def extractFromFile(file: String, from: Int, to: Int): List[Councillor] = {
    val tables = PDFPageTables.extractFrom(file, from, to)
    return extractFromTables(tables)
  }

  private def extractFromTables(tables: List[Table]): List[Councillor] = {
    return tables.map(table => getRowsWithoutHeaderTable(table))
      .flatMap(rows => splitIntoCouncillors(rows))
      .groupBy(councillor => councillor.name)
      .map(group => groupMandatesFromDuplicateCouncillors(group._2))
      .toList
  }

  private def groupMandatesFromDuplicateCouncillors(group: List[Councillor]): Councillor = {
    val firstCouncillor = group.head
    group.tail.foreach(councillor => firstCouncillor.mandates :::= councillor.mandates)
    return firstCouncillor
  }

  private def getRowsWithoutHeaderTable(table: Table): List[util.List[RectangularTextContainer[_ <: HasText]]] = {
    return table.getRows().toList.drop(6)
  }

  private def splitIntoCouncillors(rows: List[util.List[RectangularTextContainer[_ <: HasText]]]): List[Councillor] = {
    var index: Int = 0
    var councillors = List[Councillor]()
    while (index != rows.size - 1) {
      val councillor = createCouncillor(rows, index)
      index += numberOfCouncillorInformationRows
      while (hasNextMandate(rows, index)) {
        councillor.mandates ::= createMandate(rows, index)
        index += 1
      }
      councillors ::= councillor
    }
    return councillors
  }

  private def hasNextMandate(rows: List[util.List[RectangularTextContainer[_ <: HasText]]], index: Int): Boolean = {
    return !isEndOfPage(rows, index) && !isNextCouncillorNameRow(rows, index)
  }

  private def isNextCouncillorNameRow(rows: List[util.List[RectangularTextContainer[_ <: HasText]]], index: Int): Boolean = {
    return cellValueAt(rows, index, 1).isEmpty || cellValueAt(rows, index, 2).isEmpty
  }

  private def isEndOfPage(rows: List[util.List[RectangularTextContainer[_ <: HasText]]], index: Int): Boolean = {
    return index == rows.size
  }

  private def createMandate(rows: List[util.List[RectangularTextContainer[_ <: HasText]]], index: Int): Mandate = {
    val company = cellValueAt(rows, index, 0)
    val legalForm = cellValueAt(rows, index, 1)
    val position = cellValueAt(rows, index, 2)
    return new Mandate(company, legalForm, position)
  }

  private def createCouncillor(rows: List[util.List[RectangularTextContainer[_ <: HasText]]], index: Int): Councillor = {
    val name = cellValueAt(rows, index, 0)
    val party = cellValueAt(rows, index + 1, 0)
    val profession = cellValueAt(rows, index + 2, 0)
    return new Councillor(name, party, profession)
  }

  private def cellValueAt(rows: List[util.List[RectangularTextContainer[_ <: HasText]]], row: Int, col: Int): String = {
    return rows.get(row).get(col).getText
  }
}
