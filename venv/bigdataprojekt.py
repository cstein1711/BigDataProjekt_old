from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DoubleType
import geopandas as gpd

def extract_data(spark,startyear=2000, endyear=2015):
    co2_data = spark.read.option("inferSchema", "true").option("header", "true").csv(
        "C:/Users/Stone/Desktop/Uni/BigData/Project/API_EN.ATM.CO2E.PC_DS2_en_csv_v2_1217665.csv")
    return co2_data.select("Country Name", str(startyear), str(endyear))

def remove_null(word):
    if "null" in word:
        word = "0"
    return word


def main():

    startyear=2000
    endyear=2019

    sc = SparkContext("local", "BigDataProject")
    sc.setLogLevel("ERROR")
    spark = SparkSession(sc)


    result = extract_data(spark,startyear=startyear,endyear=endyear)

    result = result.withColumn(str(startyear), result[str(startyear)].cast(DoubleType()))
    result = result.withColumn(str(endyear), result[str(endyear)].cast(DoubleType()))

    result = result.na.fill(0)

    result.show()
    print(result)


if __name__ == "__main__":
    main()

