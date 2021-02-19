package com.zeger

import com.zeger.util.SparkSessionSetup
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import scala.collection.mutable.ListBuffer

/**
 * @author Pavel Zeger
 * @since 28/01/2021
 * @note get-dummies
 */
object Reshape extends SparkSessionSetup {

  /**
   * Convert categorical variable into dummy/indicator variables.
   *
   * @param sourceDataFrame     Data of which to get dummy indicators.
   * @param aggregateColumnName Column name to aggregate by.
   * @param columnsList         Column names in the `sourceDataFrame` to be encoded.
   *                            If `columnsList` is empty then all the columns will be converted.
   *                            Default is `List()` (empty).
   * @param prefixSeparator     Separator/delimiter to use in new column names. Default is `_`.
   * @return DataFrame with dummy-coded data.
   */
  def getDummies(sourceDataFrame: DataFrame,
                 aggregateColumnName: String,
                 columnsList: List[String] = List(),
                 prefixSeparator: String = "_"): DataFrame = {

    val inputColumnsList: List[String] = columnsList match {
      case Nil => sourceDataFrame.columns.toList
      case _ => columnsList
    }
    val dataFrameList: List[DataFrame] = inputColumnsList
      .filter(!_.equals(aggregateColumnName))
      .map(getPivotedDataFrame(sourceDataFrame, aggregateColumnName, _, prefixSeparator))
    dataFrameList match {
      case head :: tail => tail.foldLeft(head) {
        (leftDataFrame, rightDataFrame) =>
          leftDataFrame.join(rightDataFrame, Seq(aggregateColumnName), "left_outer")
      }
      case Nil => sparkSession.emptyDataFrame
    }

  }

  /**
   * Create pivoted DataFrame with one pivoted column.
   *
   * @param sourceDataFrame     Data of which to get dummy indicators.
   * @param aggregateColumnName Column name to aggregate by.
   * @param pivotColumnName     Column name to pivot by.
   * @param prefixSeparator     Separator/delimiter to use in new column names. Default is `_`.
   * @return Intermediate DataFrame with dummy-coded data for one column only.
   */
  def getPivotedDataFrame(sourceDataFrame: DataFrame,
                          aggregateColumnName: String,
                          pivotColumnName: String,
                          prefixSeparator: String = "_"): DataFrame = {

    val valuesListBuffer: ListBuffer[String] = new ListBuffer[String]()
    sourceDataFrame
      .select(col(pivotColumnName).cast("String"))
      .collect()
      .map(valuesListBuffer += _.toString().replaceAll("[\\[|\\]]", ""))
    val uniquePivotValues: Seq[String] = valuesListBuffer.toSet[String].toSeq

    val pivotDataframe: DataFrame = sourceDataFrame
      .select(col(aggregateColumnName), col(pivotColumnName))
      .dropDuplicates()
      .groupBy(col(aggregateColumnName))
      .pivot(pivotColumnName, uniquePivotValues)
      .count()
    val existingColumns: Array[String] = pivotDataframe.columns
    val pivotColumns: Array[String] = pivotDataframe.columns
      .map(columnName =>
        if (columnName != aggregateColumnName)
          s"$pivotColumnName$prefixSeparator${columnName.replaceAll("\\s", prefixSeparator)}"
        else aggregateColumnName)

    val columnNameMap: Map[String, String] = (existingColumns zip pivotColumns).toMap
    columnNameMap
      .foldLeft(pivotDataframe)((pivotDataframe, columnNameMap) =>
        pivotDataframe.withColumnRenamed(columnNameMap._1, columnNameMap._2))
      .na.fill(0)
  }

}
