import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

case class Transaction(id: Long, product: String, saleAmount: BigDecimal)

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Diff Demo").config("spark.master", "local").getOrCreate()
    import spark.implicits._

    val jobAOutput = Seq(Transaction(1, "apple", 2.00), Transaction(2, "apple", 2.00), Transaction(3, "apple", 2.00)).toDF
    val jobBOutput = Seq(Transaction(1, "apple", 3.00), Transaction(2, "apple", 2.00), Transaction(4, "banana", 0.50)).toDF

    val transactionsInANotInB = jobAOutput.join(
      right = jobBOutput,
      joinExprs = jobAOutput("id") === jobBOutput("id"),
      joinType = "leftanti"
    )

    val transactionsInBNotInA = jobBOutput.join(
      right = jobAOutput,
      joinExprs = jobAOutput("id") === jobBOutput("id"),
      joinType = "leftanti"
    )

    transactionsInANotInB.show
    transactionsInBNotInA.show

    val jobAOutputPrefixed = jobAOutput.columns.foldLeft(jobAOutput) { case (df, colName) =>
      df.withColumnRenamed(colName, s"JobA_$colName")
    }

    val jobBOutputPrefixed = jobBOutput.columns.foldLeft(jobBOutput) { case (df, colName) =>
      df.withColumnRenamed(colName, s"JobB_$colName")
    }

    val transactionPairs = jobAOutputPrefixed.join(
      right = jobBOutputPrefixed,
      joinExprs = $"JobA_id" === $"JobB_id",
      joinType = "inner"
    )

    transactionPairs.show

    val transactionPairComparisonResult = jobAOutput.columns.foldLeft(transactionPairs) {
      case (df, colName) =>
        df.withColumn(s"${colName}Check", col(s"JobA_$colName") === col(s"JobB_$colName"))
    }

    transactionPairComparisonResult.show

    // Generates a "NOT productCheck OR NOT saleAmountCheck OR ..." sql WHERE clause
    val filterExpr = jobAOutput.columns
      .map(colName => s"NOT ${colName}Check")
      .reduce(_ + " OR " + _)

    val mismatchedTransactions = transactionPairComparisonResult.filter(expr(filterExpr))
    mismatchedTransactions.show

    spark.stop()
  }
}