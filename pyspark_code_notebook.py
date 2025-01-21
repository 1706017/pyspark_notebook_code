df_csv =spark.read.format("csv")\
          .option("header",True)\
          .option("inferSchema",True)\
          .option("mode","PERMISSIVE")\
          .load("/FileStore/tables/BigMart_Sales.csv")

#Here we have used format as csv as we are trying to read a csv file 

#Here we have used header as True because we want that first row should be header not included as data 

#Here we have used inferSchema as True because we want spark to infer the schema by itself by reading few records of data 

#Here we have used mode as PERMISSIVE this mode is actually the read mode and here PERMISSIVE means it will replace all the malformed data #with null 

#Here in load we are providing the path to the dataset 

# Displaying the dataframe

display(df1)

#Displaying the schema for the dataframe 

df1.printSchema()

Output:
root
 |-- Item_Identifier: string (nullable = true)
 |-- Item_Weight: double (nullable = true)
 |-- Item_Fat_Content: string (nullable = true)
 |-- Item_Visibility: double (nullable = true)
 |-- Item_Type: string (nullable = true)
 |-- Item_MRP: double (nullable = true)
 |-- Outlet_Identifier: string (nullable = true)
 |-- Outlet_Establishment_Year: integer (nullable = true)
 |-- Outlet_Size: string (nullable = true)
 |-- Outlet_Location_Type: string (nullable = true)
 |-- Outlet_Type: string (nullable = true)
 |-- Item_Outlet_Sales: double (nullable = true)


#Reading json format data

df_json = spark.read.format("json")\
           .option("inferSchema",True)\
           .option("multiLine",False)\
           .load("/FileStore/tables/drivers.json")

#Here we have used an option named multiLine as False because this json is not multi Line json 
df_json.show(18)

+----+----------+--------+----------+--------------------+-----------+------+--------------------+
|code|       dob|driverId| driverRef|                name|nationality|number|                 url|
+----+----------+--------+----------+--------------------+-----------+------+--------------------+
| HAM|1985-01-07|       1|  hamilton|   {Lewis, Hamilton}|    British|    44|http://en.wikiped...|
| HEI|1977-05-10|       2|  heidfeld|    {Nick, Heidfeld}|     German|    \N|http://en.wikiped...|
| ROS|1985-06-27|       3|   rosberg|     {Nico, Rosberg}|     German|     6|http://en.wikiped...|
| ALO|1981-07-29|       4|    alonso|  {Fernando, Alonso}|    Spanish|    14|http://en.wikiped...|
| KOV|1981-10-19|       5|kovalainen|{Heikki, Kovalainen}|    Finnish|    \N|http://en.wikiped...|
| NAK|1985-01-11|       6|  nakajima|  {Kazuki, Nakajima}|   Japanese|    \N|http://en.wikiped...|
| BOU|1979-02-28|       7|  bourdais|{Sébastien, Bourd...|     French|    \N|http://en.wikiped...|
| RAI|1979-10-17|       8| raikkonen|   {Kimi, Räikkönen}|    Finnish|     7|http://en.wikiped...|
| KUB|1984-12-07|       9|    kubica|    {Robert, Kubica}|     Polish|    88|http://en.wikiped...|
| GLO|1982-03-18|      10|     glock|       {Timo, Glock}|     German|    \N|http://en.wikiped...|
| SAT|1977-01-28|      11|      sato|      {Takuma, Sato}|   Japanese|    \N|http://en.wikiped...|
| PIQ|1985-07-25|      12| piquet_jr|{Nelson, Piquet Jr.}|  Brazilian|    \N|http://en.wikiped...|
| MAS|1981-04-25|      13|     massa|     {Felipe, Massa}|  Brazilian|    19|http://en.wikiped...|
| COU|1971-03-27|      14| coulthard|  {David, Coulthard}|    British|    \N|http://en.wikiped...|
| TRU|1974-07-13|      15|    trulli|     {Jarno, Trulli}|    Italian|    \N|http://en.wikiped...|
| SUT|1983-01-11|      16|     sutil|     {Adrian, Sutil}|     German|    99|http://en.wikiped...|
| WEB|1976-08-27|      17|    webber|      {Mark, Webber}| Australian|    \N|http://en.wikiped...|


#To Exlpicitly define schema of DataFrame

from pyspark.sql.types import *
from pyspark.sql.functions import *

df_csv.printSchema()

df_csv_schema = StructType([StructField("Item_Identifier",StringType(),True),\
                           StructField("Item_Weight",StringType(),True),\
                           StructField("Item_Fat_Content",StringType(),True),\
                           StructField("Item_Visibility",DoubleType(),True),\
                           StructField("Item_Type",StringType(),True),\
                           StructField("Item_MRP",DoubleType(),True),\
                           StructField("Outlet_Identifier",StringType(),True),\
                           StructField("Outlet_Establishment_Year",IntegerType(),True),\
                           StructField("Outlet_Size",StringType(),True),\
                           StructField("Outlet_Location_Type",StringType(),True),\
                           StructField("Outlet_Type",StringType(),True),\
                           StructField("Item_Outlet_Sales",DoubleType(),True)])

#Attaching the schema to the dataframe

df_csv_with_schema = spark.read.format("csv")\
                          .option("header",True)\
                          .schema(df_csv_schema)\
                          .option("mode","PERMISSIVE")\
                          .load("/FileStore/tables/BigMart_Sales.csv")

#Priniting the schema of Dataframe

df_csv_with_schema.printSchema()


#Day-02 Code Changes 
#Date: 16th Jan 2025 

#SELECT Transformation on DataFrame to select only specific columns
======================================================================
### Case1) When directly using the column name with select 

df_csv_with_schema.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

#case 2 ) When using col() method with the select 

df_csv_with_schema.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()




ALIAS Transformation on DataFrame columns
=============================================
df_csv_with_schema.select(col('Item_Identifier').alias('Item_Id')).display()




FILTER Transformation on DataFrames
====================================
#Case1)  Filter the data with fat content = Regular
df_csv_with_schema.filter(col('Item_Fat_Content')=='Regular').display()


#Case2) Filter the data where the Item type is Soft Drink and Item Weight is less than 10

df_csv_with_schema.filter((col('Item_Type') == 'Soft Drinks') & (col('Item_Weight')<10)).display()


#Case3) Fetch the data where location Type is in Tier1 or Tier2 and Outlet size is NULL

df_csv_with_schema.filter( (col('Outlet_Location_Type').isin('Tier 1','Tier 2')) & (col('Outlet_Size').isNull())).display()

#withColumnRenamed() Transformation in Pyspark:
================================================
from pyspark.sql.types import *
from pyspark.sql.functions import *

df_csv_with_schema.withColumnRenamed('Item_Weight','Item_Wt').display()


          
""".withColumn() Transformation
===================================
It has below use cases:
Use case 1) To add a new column in data frame having constant value

Use case 2) To add a new column by the help of existing columns of dataframe

Use case 3) To modify an existing column in dataframe """

# To add a new column with constant value
df_new = df_csv_with_schema.withColumn('Flag_new',lit('new'))
df_new.display()


# To add a new column based on existing columns of dataframe
df_csv_with_schema.withColumn('Multiply_new_col',col('Item_Weight')*col('Item_MRP')).display()

#To modify the existing column of a dataframe
df_csv_with_schema.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Regular','Reg'))\
                  .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),'Low Fat','Lf')).display()


"""cast() Transformation in Pyspark
========================================
To change the data type of the column in a dataframe"""

df_csv_with_schema.withColumn('Item_Weight',col('Item_Weight').cast(IntegerType())).printSchema()







          






