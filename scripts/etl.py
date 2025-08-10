from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import to_timestamp, current_date, datediff, year, when, col, lit, lpad, avg, sum as _sum, count, upper, row_number, desc
from pyspark.sql.window import Window


# Create Spark & Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df_311 = spark.read.option("header","true").parquet("s3://cdac-final-project-data/Bronze-level/311_nyc_dataset/311_nyc_raw_data.parquet")

df_311 = df_311.withColumnRenamed("complaint type", "complaint_type")


df_311 = df_311.withColumn("complaint_category",
    when(col("complaint_type").isin("heat/hot water", "heating", "heat/hot water"), "HEAT_ISSUE")
    .when(col("complaint_type").isin("water leak", "water drainage"), "WATER_LEAK")
    .when(col("complaint_type").isin("plumbing", "general construction/plumbing", "boilers", "boiler"), "PLUMBING_ISSUE")
    .when(col("complaint_type").isin("building condition", "structural", "unstable building", "building/use"), "BUILDING_CONDITION")
    .when(col("complaint_type").isin("electric", "electrical", "no power"), "ELECTRICAL_ISSUE")
    .when(col("complaint_type") == "mold", "MOLD")
    .when(col("complaint_type").isin("elevator"), "ELEVATOR")
    .when(col("complaint_type").isin("paint - plaster", "paint/plaster"), "PAINT_PLASTER")
    .when(col("complaint_type").isin("door/window"), "DOOR_WINDOW")
    .when(col("complaint_type").isin("safety", "construction safety enforcement", "facade insp safety pgm", "best/site safety", "scaffold safety"), "SAFETY")
    .when(col("complaint_type").isin("construction", "general construction", "interior demo"), "CONSTRUCTION")
    .when(col("complaint_type").isin("miscellaneous categories", "quality of life", "lost property", "maintenance or facility", "forms", "outside building", "dob posted notice or order", "traffic signal condition", "borough office", "building marshal's office", "building marshals office"), "MISC")
    .when(col("complaint_type").isin("dept of investigations", "investigations and discipline (iad)", "forensic engineering", "executive inspections", "special projects inspection team (spit)", "special enforcement", "special operations", "ahv inspection unit", "special natural area district (snad)", "sustainability enforcement"), "INVESTIGATION")
    .when(col("complaint_type").isin("appliance"), "APPLIANCE")
    .when(col("complaint_type").isin("flooring/stairs"), "FLOORING_STAIRS")
    .when(col("complaint_type").isin("unsanitary condition"), "UNSANITARY_CONDITION")
    .when(col("complaint_type").isin("hpd literature request"), "EVICTION_LITERATURE")
    .otherwise("OTHER")
)

df_hpd = spark.read.option("header","true").parquet("s3://cdac-final-project-data/Bronze-level/HPD_dataset/HPD_raw_data.parquet")

df_311 = df_311.withColumnRenamed("Unique_Key", "Unique_Key_311")


# Step 1: Select only necessary columns from HPD
df_hpd_trimmed = df_hpd.select(
    col("unique_key").alias("hpd_unique_key"),
    col("bbl")
)

# Step 2: Left join on Unique_Key
df_joined = df_311.join(
    df_hpd_trimmed,
    df_311["Unique_Key_311"] == df_hpd_trimmed["hpd_unique_key"],
    how="left"
)

# Step 3: Add 'validation' column
df_validated = df_joined.withColumn(
    "validation",
    when(col("hpd_unique_key").isNotNull(), lit(1)).otherwise(lit(0))
)

# Step 4: Drop all HPD columns except 'bbl'
df_result = df_validated.drop("hpd_unique_key")

# Final DataFrame has:
# - All columns from df_311
# - 'validation' column
# - 'bbl' column from HPD

df_pluto = spark.read.option("header","true").parquet("s3://cdac-final-project-data/Bronze-level/PLUTO_dataset/PLUTO_raw_data.parquet")

df_pluto = df_pluto.withColumnRenamed("bbl", "bbl_pluto")
df_pluto = df_pluto.withColumnRenamed("latitude", "latitude_pluto")
df_pluto = df_pluto.withColumnRenamed("longitude", "longitude_pluto")
df_pluto = df_pluto.withColumnRenamed("community_board", "community_board_pluto")
df_pluto = df_pluto.withColumnRenamed("borough", "borough_pluto")
df_pluto = df_pluto.withColumnRenamed("landmark", "landmark_pluto")


# Step 1: Standardize BBL data type (10-digit string)
df_result = df_result.withColumn("bbl", lpad(col("bbl").cast("string"), 10, "0"))
df_pluto = df_pluto.withColumn("bbl_pluto", lpad(col("bbl_pluto").cast("string"), 10, "0"))

df_final = df_result.join(
    df_pluto,
    df_result["bbl"] == df_pluto["bbl_pluto"],
    how="left"
)

df_final = df_final.drop("bbl_pluto")

# Define the useful KPI columns to retain
columns_to_keep = [
    # 311 complaint info
    'Unique_Key_311',
    'Created_Date', 'Closed_Date', 'created_date_stand', 'closed_date_stand',
    'complaint_type', 'Descriptor', 'complaint_category',
    'Status', 'validation',
    'borough', 'Incident_Zip', 'City', 'full_address',
    'Latitude', 'Longitude',

    # HPD BBL
    'bbl',

    # PLUTO property info
    'tax_block', 'tax_lot',
    'landuse', 'landuse_category',
    'bldgclass', 'ownertype', 'ownername',
    'lotarea', 'bldgarea', 'resarea', 'comarea',
    'unitsres', 'unitstotal',
    'numfloors', 'yearbuilt', 'yearalter1', 'yearalter2',
    'zonedist1', 'overlay1',
    'latitude_pluto', 'longitude_pluto',
    'bbl_standard'
]

# Create the trimmed DataFrame
df_kpi = df_final.select(columns_to_keep)

df_master=df_kpi

df_aff = spark.read.option("header","true").csv("s3://cdac-final-project-data/Bronze-level/Affordable_Housing_dataset/Affordable_Housing_raw_data.csv")


df_complaints_by_borough=df_master.groupBy("borough").agg(
    count("*").alias("total_complaints"),
    _sum(col("validation").cast("int")).alias("validated_complaints"),
    avg(col("validation").cast("int")).alias("validation_rate")
)


df_master_enriched = df_master.join(
    df_complaints_by_borough,
    on="borough",
    how="left"
)

df_aff_summary = df_aff.groupBy("Borough").agg(
    count("*").alias("total_projects"),
    _sum(col("`Total Units`").cast("int")).alias("total_affordable_units"),
    avg(col("`Total Units`").cast("int")).alias("avg_units_per_project")
).orderBy("Borough")


df_aff_summary = df_aff_summary.withColumn("borough", upper(col("borough")))
df_master_enriched = df_master_enriched.withColumn("borough", upper(col("borough")))

df_fully_enriched = df_master_enriched.join(
    df_aff_summary,
    on="borough",
    how="left"
)

df=df_fully_enriched


# Step 1: Convert string to timestamp
df = df.withColumn(
    "created_date_stand", to_timestamp("created_date_stand", "yyyy-MM-dd HH:mm:ss")
).withColumn(
    "closed_date_stand", to_timestamp("closed_date_stand", "yyyy-MM-dd HH:mm:ss")
)

# Step 2: Calculate resolution_time in days
df = df.withColumn(
    "resolution_time",
    when(
        col("created_date_stand").isNotNull() &
        col("closed_date_stand").isNotNull() &
        (col("closed_date_stand") >= col("created_date_stand")),
        datediff("closed_date_stand", "created_date_stand")
    )
)


# Step 1: Convert yearbuilt to integer safely
df = df.withColumn(
    "yearbuilt", col("yearbuilt").cast("int")
)

# Step 2: Calculate building age only if yearbuilt is not null
df = df.withColumn(
    "building_age",
    when(
        col("yearbuilt").isNotNull(),
        year(current_date()) - col("yearbuilt")
    )
)


# Filter out unresolved complaints (i.e., null resolution times)
df_resolved = df.filter(col("resolution_time").isNotNull())

# Group by borough and calculate average resolution time
df_avg_resolution_by_borough = df_resolved.groupBy("borough").agg(
    avg("resolution_time").alias("avg_resolution_time")
)

df = df.join(df_avg_resolution_by_borough,on="borough",how="left")


# Count complaints per borough and category
complaint_ranked = df.groupBy("borough", "complaint_category") \
    .count() \
    .withColumn("rank", row_number().over(Window.partitionBy("borough").orderBy(desc("count"))))

# Get top 5 complaint types
top5_complaints = complaint_ranked.filter("rank <= 5") \
    .select("borough", "complaint_category") \
    .withColumn("top5_complaint_in_borough", lit("Yes"))

# Join back to main DF
df = df.join(top5_complaints, on=["borough", "complaint_category"], how="left") \
    .withColumn("top5_complaint_in_borough", when(col("top5_complaint_in_borough").isNull(), "No").otherwise("Yes"))

building_age_avg_df = df.groupBy("borough") \
    .agg(avg("building_age").alias("avg_building_age"))

df = df.join(building_age_avg_df, on="borough", how="left")

df = df.drop("created_date_stand", "closed_date_stand", "ownertype", "ownername",
             "latitude_pluto", "longitude_pluto", "zonedist1", "overlay1", "bbl_standard")

df = df.drop("descriptor")

df.coalesce(1).write.mode("overwrite").option("header","true").parquet("s3://cdac-final-project-data/Silver-level/transformed_data/")