# Databricks notebook source
# DBTITLE 1,Library Functions import
# MAGIC %run "../hospital-analytics/LibraryFunctions"

# COMMAND ----------

# DBTITLE 1,read the CSV file
hospitaDataPath="/Volumes/hospital/mainschema/hospitalrawfiles/Hospital_Inpatient_Discharges.csv"

# COMMAND ----------

hospitaDataDF=spark.read.csv(hospitaDataPath,header=True,inferSchema=True)

# COMMAND ----------

# DBTITLE 1,column renaming
hospitaDataDF = reduce(
    lambda df, c: df.withColumnRenamed(c, c.replace(" ", "_")) if " " in c else df,
    hospitaDataDF.columns,
    hospitaDataDF
)

# COMMAND ----------

PerviousDF=spark.sql("select * from hospital.mainschema.BronzeHospitalData")
appendDF=hospitaDataDF.subtract(PerviousDF)
if appendDF.limit(1).count()>0:
  print("New Data Found")
  hospitaDataDF.write.format('delta').mode('append').saveAsTable('hospital.mainschema.BronzeHospitalData')
else:
  print("No New Data Found")
