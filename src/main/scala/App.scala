import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, StandardCopyOption}
import org.apache.spark.sql.Column

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
  def main(args: Array[String]): Unit = {
    // val spark = SparkSession.builder()
    //   .appName("Codespaces Spark Hello")
    //   .master("local[*]")
    //   .config("spark.ui.showConsoleProgress", "false")
    //   .getOrCreate()

    // spark.sparkContext.setLogLevel("WARN")

    // import spark.implicits._

    // val df = Seq(
    //   ("Alice", 34),
    //   ("Bob", 28),
    //   ("Carol", 41)
    // ).toDF("name", "age")

    // df.show()

    // // Keep job alive briefly so you can view the Spark UI on port 4040
    // println("Open the Spark UI on port 4040 (Ports panel). Sleeping 10s...")
    // Thread.sleep(10000)

    // spark.stop()

    // Usage of DataCleaner class
    val spark = SparkSession.builder
      .appName("DataCleaningExample")
      .master("local[*]") // Modify for cluster
      .getOrCreate()

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


    // Stop Spark session
    spark.stop()
  }
}
