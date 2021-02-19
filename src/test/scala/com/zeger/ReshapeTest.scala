package com.zeger

import com.zeger.util.SparkSessionSetupTest
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

/**
 * @author Pavel Zeger
 * @since 28/01/2021
 */
class ReshapeTest extends FunSuite with SparkSessionSetupTest {

  val aggregateColumnName: String = "id"
  val pivotColumnName: String = "cat_id"
  val prefixSeparator: String = "_"

  test("testGetPivotedDataFrame") {

    val data: Array[(String, Int, String)] = Array(
      ("7c5d5c4b4d3d4567ac9efe937cf7bc96", 87, "Hardware"),
      ("7c5d5c4b4d3d4567ac9efe937cf7bc96", 48, "Computers"),
      ("d8d9e6f25f3a4be081a100c624d4d6cf", 13, "Hardware"),
      ("7f682eda0e714e11bef4d7178ada264a", 71, "Hardware"),
      ("7f682eda0e714e11bef4d7178ada264a", 94, "Internet"),
      ("7f682eda0e714e11bef4d7178ada264a", 54, "Moderated"),
      ("a9e8c00a91c44fcc9a95176a051c3115", 35, "Computers"),
      ("76ac5b46816f4a878eabf1ebd94b6210", 47, "Computers"),
      ("bf39712537a04a2fa5c39f8a2ede5424", 35, "Hardware"),
      ("bf39712537a04a2fa5c39f8a2ede5424", 92, "Hardware"),
      ("bf39712537a04a2fa5c39f8a2ede5424", 61, "Computers"),
      ("bf39712537a04a2fa5c39f8a2ede5424", 87, "Technology"),
      ("bf39712537a04a2fa5c39f8a2ede5424", 92, "Technology"),
      ("5571a1cd44664bbdb7b04f2050e78431", 87, "Hardware"),
      ("5571a1cd44664bbdb7b04f2050e78431", 69, "Hardware"),
      ("17960456040745c7aed1cfc8df1fa618", 87, "Moderated"),
      ("17960456040745c7aed1cfc8df1fa618", 23, "Moderated"),
      ("17960456040745c7aed1cfc8df1fa618", 54, "Hardware"),
      ("80214a702b33489fb4febf3ef4b14b83", 69, "Moderated"),
      ("80214a702b33489fb4febf3ef4b14b83", 23, "Technology"))

    val sourceDataFrame: DataFrame = sparkSession
      .createDataFrame(data)
      .toDF(aggregateColumnName, pivotColumnName, "contextual_categories_name")
      .select(col(aggregateColumnName), col(pivotColumnName))

    val pivotedDataFrameActual: DataFrame = Reshape.getPivotedDataFrame(
      sourceDataFrame, aggregateColumnName, pivotColumnName, prefixSeparator)

    val rowsNumberActual: Long = pivotedDataFrameActual.count()
    val rowsNumberExpected: Long = sourceDataFrame.select(col(aggregateColumnName)).distinct().count()

    val columnsNumberActual: Long = pivotedDataFrameActual.columns.length
    val columnsNumberExpected: Long = sourceDataFrame.select(col(pivotColumnName)).distinct().count() + 1

    val rowActual: Array[Row] = pivotedDataFrameActual
      .filter(col(aggregateColumnName) === "bf39712537a04a2fa5c39f8a2ede5424")
      .collect()
    val rowExpected: Array[Row] = Array(Row("bf39712537a04a2fa5c39f8a2ede5424", 0, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1))

    assert(rowsNumberExpected === rowsNumberActual)
    assert(columnsNumberExpected === columnsNumberActual)
    assert(rowExpected.sameElements(rowActual))

  }

  test("testGetDummies") {

    val data: Array[(String, String, Int, String, Int, String, String, String, Int, Int, Int)] = Array(
      ("087ab0ac7a894df9bf77762aab45e053", "2020-07-03 00:00:00.000", 18, "Video", 70, "or", "Smartphone", "iOS", 0, 0, 0),
      ("be27f404a57e4678a55e9e8ae4953542", "2020-08-09 00:00:00.000", 18, "Page", 70, "me", "Tablet", "iPadOS", 0, 0, 0),
      ("cd01eca544784b438cac2c61362e6fdc", "2020-09-10 00:00:00.000", 21, "Video", 70, "ca", "Desktop", "macOS", 0, 0, 1),
      ("fbd7632a388143888c78f3f373046f5e", "2020-08-17 00:00:00.000", 3, "Page", 70, "tx" , "Smartphone", "iOS" , 0, 0, 0),
      ("83373796c41f4dc1804295df19578e12", "2020-08-19 00:00:00.000", 16, "Page", 70, "wa", "Smartphone", "iOS", 0, 0, 0),
      ("3a630e57aaa8477e85e2ebfcaafeedb6", "2020-09-07 00:00:00.000", 14, "Page", 70, "mn", "Smartphone", "iOS", 0, 0, 3),
      ("2fee6dfd891e42c2aff36b75988b3c28", "2020-08-08 00:00:00.000", 2, "Page", 70, "ca", "Smartphone", "iOS", 0, 0, 0),
      ("0b845c4344f946af85287a2edf8eb27d", "2020-07-31 00:00:00.000", 15, "Page", 70, "id", "Smartphone", "iOS", 0, 0, 0),
      ("61645799c8714ccf8512df491f7550bb", "2020-07-11 00:00:00.000", 18, "Video", 70, "oh","Smartphone", "Android" , 0, 0, 0),
      ("b403e6bd18554b0eb0b2386be01885e2", "2020-08-17 00:00:00.000", 16, "Video", 70, "il","Smartphone", "iOS", 1, 0, 1),
      ("959d0f3c9c614b78868afd45a4fded33", "2020-07-26 00:00:00.000", 11, "Video", 70, "ga","Desktop", "Windows", 0, 0, 0),
      ("08753a6925aa4113a5821a7497490e9a", "2020-09-17 00:00:00.000", 16, "Page", 70, "ct","Smartphone", "iOS", 0, 0, 0),
      ("3fd82bb76906439aae70d466a0cde90a", "2020-08-19 00:00:00.000", 3, "Page", 70, "wa","Smartphone","iOS", 0, 0, 0),
      ("d3e97dba9b5c4bdd8b2c27e0980cc397", "2020-07-16 00:00:00.000", 12, "Video", 70, "pa","Smartphone", "iOS", 0, 0, 0),
      ("f4c8e5122cb74fdfaf2efc0dda29e2f3", "2020-08-11 00:00:00.000", 0, "Video", 70, "ga","Smartphone", "iOS", 0, 0, 0),
      ("1713258e4c1f41f6b993bb2ab71c30fd", "2020-08-04 00:00:00.000", 16, "Page", 70, "ga", "Smartphone", "iOS", 0, 0, 0),
      ("d36af62b1f774d82ac9900583a744787", "2020-08-13 00:00:00.000", 3, "Page", 70, "fl", "Smartphone", "iOS", 0, 0, 0),
      ("9598a727f7234255bccb8a28d4fd3f97", "2020-08-21 00:00:00.000", 4, "Page", 70, "tx", "Smartphone", "iOS", 0, 0, 0),
      ("c9c741fe991240a2a0e1d9359cd15492", "2020-08-07 00:00:00.000", 16, "Page", 70, "co", "Tablet", "iOS", 0, 0, 0),
      ("c9c741fe991240a2a0e1d9359cd15492", "2020-08-11 00:00:00.000", 16, "Page", 70, "co", "Tablet", "iOS", 0, 0, 0))

    val sourceDataFrame: DataFrame = sparkSession
      .createDataFrame(data)
      .toDF(aggregateColumnName, "day", "hour", "product", "number",
        "region", "type", "os", "clicks", "completes", "interactions")

    val columnsList: List[String] = List(aggregateColumnName, "hour", "region", "type", "os")
    val dummiesDataFrameActual: DataFrame = Reshape.getDummies(sourceDataFrame, aggregateColumnName, columnsList, prefixSeparator)

    val rowsNumberActual: Long = dummiesDataFrameActual.count()
    val rowsNumberExpected: Long = sourceDataFrame.select(col(aggregateColumnName)).distinct().count()

    val columnsNumberActual: Long = dummiesDataFrameActual.columns.length
    val columnsNumberExpected: Long = columnsList
      .filter(!_.equals(aggregateColumnName))
      .map(sourceDataFrame.select(_).distinct().count())
      .sum + 1

    val rowActual: Array[Row] = dummiesDataFrameActual
      .filter(col(aggregateColumnName) === "c9c741fe991240a2a0e1d9359cd15492")
      .collect()
    val rowExpected: Array[Row] = Array(Row("c9c741fe991240a2a0e1d9359cd15492", 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0 ))

    assert(rowsNumberExpected === rowsNumberActual)
    assert(columnsNumberExpected === columnsNumberActual)
    assert(rowExpected.sameElements(rowActual))

  }

}
