import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, StandardCopyOption}
import org.apache.spark.sql.Column

import org.apache.spark.sql.types._

// DataCleaner class definition
class DataCleaner(spark: SparkSession) {

  // Method to download a file from a URL
  def downloadFile(url: String, localFilename: String): Unit = {
    val in = new java.net.URL(url).openStream()
    Files.copy(in, Paths.get(localFilename), StandardCopyOption.REPLACE_EXISTING)
    in.close()
  }

  // Method to load data from CSV
  def loadData(filepath: String, hasHeader: Boolean = false, inferSchema: Boolean = true): DataFrame = {
    spark.read
      .option("header", hasHeader.toString)
      .option("inferSchema", inferSchema.toString)
      .csv(filepath)
  }

  // Method to assign column names if dataset does not have headers
  def setColumnNames(df: DataFrame, columnNames: Seq[String]): DataFrame = {
    df.toDF(columnNames: _*)
  }

  // Method to clean data
  def cleanData(df: DataFrame, fillString: String = "Unknown", fillNumber: Int = 0, trimColumns: Boolean = true, duplicateRemoval: Boolean = true): DataFrame = {
    var cleanedDF = df

    // Remove duplicates if specified
    if (duplicateRemoval) {
      cleanedDF = cleanedDF.dropDuplicates()
    }

    // Handle null values
    cleanedDF = cleanedDF.na.fill(fillString).na.fill(fillNumber)

    // Trim whitespace in string columns
    if (trimColumns) {
      cleanedDF = cleanedDF.columns.foldLeft(cleanedDF) {
        (df, colName) => df.withColumn(colName, when(col(colName).isNotNull, trim(col(colName))).otherwise(col(colName)))
      }
    }

    cleanedDF
  }

  // Method to filter out invalid rows based on specified column and condition
  def filterInvalidData(df: DataFrame, columnName: String, condition: Column => Column): DataFrame = {
    df.filter(condition(col(columnName)))
  }

  // Method to save cleaned data
  def saveData(df: DataFrame, outputPath: String, saveHeader: Boolean = true): Unit = {
    df.write.option("header", saveHeader.toString).csv(outputPath)
  }
}


object App {
    def dataCleanerExample(spark: SparkSession) = {
      // Instantiate DataCleaner
      val dataCleaner = new DataCleaner(spark)

      // Download the dataset
      val url = "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data"
      val localFilename = "adult.csv"
      dataCleaner.downloadFile(url, localFilename)

      // Load data
      val df = dataCleaner.loadData(localFilename)

      // Set column names
      val columns = Seq("age", "workclass", "fnlwgt", "education", "education_num", "marital_status", "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country", "income")
      val dfWithHeaders = dataCleaner.setColumnNames(df, columns)

      // Clean data
      val cleanedDF = dataCleaner.cleanData(dfWithHeaders)

      // Filter out invalid data (e.g., remove rows with negative values in "capital_gain")
      val validDataDF = dataCleaner.filterInvalidData(cleanedDF, "capital_gain", _ >= 0)

      // Show the cleaned data
      validDataDF.show(10)
    }

    def exercise1(spark: SparkSession) = {
      // Define the schema structure
      val customSchema = new StructType(
        Array(
            StructField("order_id", LongType, nullable = false),
            StructField("customer_id", LongType, nullable = true),
            StructField("country", StringType, nullable = true),
            StructField("product", StringType, nullable = true),
            StructField("category", StringType, nullable = true),
            StructField("unit_price", DoubleType, nullable = true),
            StructField("quantity", IntegerType, nullable = true),
            StructField("order_timestamp", StringType, nullable = true) 
        )
    )
    // Load the CSV
    val df = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(customSchema)
      .load("data/online_retail.csv")

    // Clean
    val dfCleaned = df.filter(
        col("customer_id").isNotNull &&
        col("quantity") > 0 &&
        col("unit_price") > 0
    )

    // Questions
    val dfWithTotalPrice = dfCleaned.withColumn(
        "total_amount", 
        col("quantity")*col("unit_price")
    )

    val dfSalesByCountry = dfWithTotalPrice
      .groupBy(col("country"))
      .agg(
        sum("total_amount").alias("total_sales_amount"),
        count("country").alias("num_of_countries"),
        round(avg("total_amount"), 2).alias("total_avg_amount")
      )
      .orderBy(col("total_avg_amount").desc)

    val dfTop5Products = dfWithTotalPrice
      .groupBy(col("product"))
      .agg(sum("quantity").alias("total_quantity_sold"))
      .orderBy(col("total_quantity_sold").desc)
      .limit(5)
    
    var dfPerCategory = dfWithTotalPrice
      .groupBy(col("category"))
      .agg(
        sum("quantity").alias("total_quantity_sold"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("unit_price"), 2).alias("avg_unit_price")
      )
      .orderBy(col("total_revenue"))
      
    dfWithTotalPrice.show(false)
    dfSalesByCountry.show(false)
    dfTop5Products.show(false)
    dfPerCategory.show(false)
  }

  def exercise2(spark: SparkSession) = {
    // TODO: 
      val dfCustomers = spark.read
        .format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load("data/customers.csv")

      val dfOrders = spark.read
        .format("csv")
        .option("header", true)
        .option("inferSchema", true)
        .load("data/orders.csv")

      // df1.join(df2, joinCondition, "inner")
      val joinCondition = dfCustomers("customer_id") === dfOrders("customer_id")
      val dfJoin = dfCustomers.join(dfOrders, joinCondition, "inner")
      import spark.implicits._
      // Using import spark.implicits._ allows us to use $("column_name") syntax instead of col("column_name")
      dfJoin.select(
        dfCustomers("customer_id"), //we need to use the data-frame to avoid ambiguity since both tables have the same column name
        $"name",
        $"email",
        $"country",
        $"signup_date",
        $"order_id",
        $"order_timestamp",
        $"order_total"
      ).show(false)

      val dfTotalSpentPerCustomer = dfOrders
        .groupBy($"customer_id")
        .agg(
          sum($"order_total").alias("customer_total_spent")
      )

      val dfFinal = dfCustomers.join(
        dfTotalSpentPerCustomer,
        Seq("customer_id"), // joins by customer_id and removes one of the columns
        "inner"
      )

      dfFinal.select(
        $"customer_id", // customer_id is not ambiguos since have only 1
        $"name",
        $"customer_total_spent"
      ).show(false)
  
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("DataCleaningExample")
      .master("local[*]") // Modify for cluster
      .getOrCreate()

  
    exercise2(spark)

    // Run: sbt clean run
    // Stop Spark session
    spark.stop()
  }
}
