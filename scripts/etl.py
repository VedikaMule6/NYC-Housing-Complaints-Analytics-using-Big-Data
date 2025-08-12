import sys
import re
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import when, lit, lpad, to_timestamp, datediff, when, col, year, current_date, avg, upper, count, sum as _sum, trim
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Parse job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df_311=spark.read.option("header","true").csv("s3://nyc-housing-complaints-g4/complaints-data/Housing_Complaints.csv")

df_311 = df_311.withColumn("complaint_category",
    when(col("complaint type").isin("heat/hot water", "heating", "heat/hot water"), "HEAT_ISSUE")
    .when(col("complaint type").isin("water leak", "water drainage"), "WATER_LEAK")
    .when(col("complaint type").isin("plumbing", "general construction/plumbing", "boilers", "boiler"), "PLUMBING_ISSUE")
    .when(col("complaint type").isin("building condition", "structural", "unstable building", "building/use"), "BUILDING_CONDITION")
    .when(col("complaint type").isin("electric", "electrical", "no power"), "ELECTRICAL_ISSUE")
    .when(col("complaint type") == "mold", "MOLD")
    .when(col("complaint type").isin("elevator"), "ELEVATOR")
    .when(col("complaint type").isin("paint - plaster", "paint/plaster"), "PAINT_PLASTER")
    .when(col("complaint type").isin("door/window"), "DOOR_WINDOW")
    .when(col("complaint type").isin("safety", "construction safety enforcement", "facade insp safety pgm", "best/site safety", "scaffold safety"), "SAFETY")
    .when(col("complaint type").isin("construction", "general construction", "interior demo"), "CONSTRUCTION")
    .when(col("complaint type").isin("miscellaneous categories", "quality of life", "lost property", "maintenance or facility", "forms", "outside building", "dob posted notice or order", "traffic signal condition", "borough office", "building marshal's office", "building marshals office"), "MISC")
    .when(col("complaint type").isin("dept of investigations", "investigations and discipline (iad)", "forensic engineering", "executive inspections", "special projects inspection team (spit)", "special enforcement", "special operations", "ahv inspection unit", "special natural area district (snad)", "sustainability enforcement"), "INVESTIGATION")
    .when(col("complaint type").isin("appliance"), "APPLIANCE")
    .when(col("complaint type").isin("flooring/stairs"), "FLOORING_STAIRS")
    .when(col("complaint type").isin("unsanitary condition"), "UNSANITARY_CONDITION")
    .when(col("complaint type").isin("hpd literature request"), "EVICTION_LITERATURE")
    .otherwise("OTHER")
)

df_311 = df_311.withColumn("created_date_stand", to_timestamp(col("Created Date"), "MM/dd/yyyy hh:mm:ss a"))
df_311 = df_311.withColumn("closed_date_stand",to_timestamp(col("Closed Date"), "MM/dd/yyyy hh:mm:ss a"))
df_311 = df_311.withColumn("borough",
    when(col("borough").isin("BK", "BKLYN", "BROOKLYN"), "BROOKLYN")
    .when(col("borough").isin("MN", "MANHATTAN"), "MANHATTAN")
    .when(col("borough").isin("BX", "BRONX"), "BRONX")
    .when(col("borough").isin("QN", "QUEENS"), "QUEENS")
    .when(col("borough").isin("SI", "STATEN ISLAND"), "STATEN ISLAND")
    .otherwise(col("borough"))
)

# Replace invalid characters for Parquet in all column names
df_311 = df_311.toDF(*[
    re.sub(r"[ ,;{}()\n\t=]", "_", col) for col in df_311.columns
])

df_hpd=spark.read.option("header","true").csv("s3://nyc-hpd-g4/Housing_Maintenance_Code_Complaints_and_Problems_20250728.csv")

# Rename all columns in HPD DataFrame by replacing special characters with "_"
df_hpd = df_hpd.toDF(*[
    re.sub(r"[ ,;{}()\n\t=\\/]", "_", col) for col in df_hpd.columns
])

df_hpd = df_hpd.withColumn("borough", trim(upper(col("borough"))))

df_hpd = df_hpd.withColumn("borough",
    when(col("borough").isin("BK", "BKLYN"), "BROOKLYN")
    .when(col("borough").isin("MN", "MANHATTAN"), "MANHATTAN")
    .when(col("borough").isin("BX", "BRONX"), "BRONX")
    .when(col("borough").isin("QN", "QUEENS"), "QUEENS")
    .when(col("borough").isin("SI", "STATEN ISLAND"), "STATEN ISLAND")
    .otherwise(col("borough"))
)

#Step 1: Select only necessary columns from HPD
df_hpd_trimmed = df_hpd.select(
    col("unique_key").alias("hpd_unique_key"),
    col("bbl")
)

# Step 2: Left join on Unique_Key
df_joined = df_311.join(
    df_hpd_trimmed,
    df_311["Unique_Key"] == df_hpd_trimmed["hpd_unique_key"],
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

df_pluto=spark.read.option("header","true").csv("s3://pluto311new/Primary_Land_Use_Tax_Lot_Output_PLUTO_20250724.csv")

def standardize_col_names(df):
    def clean_name(name):
        # Strip leading/trailing spaces, replace all whitespace with single underscore, lowercase
        name = name.strip()
        name = re.sub(r"\s+", "_", name)  # handles spaces, tabs, multiple spaces
        name = name.lower()
        return name

    new_cols = [clean_name(col) for col in df.columns]

    for old, new in zip(df.columns, new_cols):
        df = df.withColumnRenamed(old, new)
    return df

# Apply transformation
df_pluto = standardize_col_names(df_pluto)

df_pluto = df_pluto.withColumnRenamed("bbl", "bbl_pluto")
df_pluto = df_pluto.withColumnRenamed("latitude", "latitude_pluto")
df_pluto = df_pluto.withColumnRenamed("longitude", "longitude_pluto")
df_pluto = df_pluto.withColumnRenamed("community_board", "community_board_pluto")
df_pluto = df_pluto.withColumnRenamed("borough", "borough_pluto")
df_pluto = df_pluto.withColumnRenamed("landmark", "landmark_pluto")

# Mapping based on NYC PLUTO documentation
landuse_map = {
    "1": "One & Two Family Buildings",
    "2": "Multi-Family Walk-Up Buildings",
    "3": "Multi-Family Elevator Buildings",
    "4": "Mixed Residential & Commercial Buildings",
    "5": "Commercial & Office Buildings",
    "6": "Industrial & Manufacturing",
    "7": "Transportation & Utility",
    "8": "Public Facilities & Institutions",
    "9": "Open Space & Outdoor Recreation",
    "10": "Parking Facilities",
    "11": "Vacant Land"
}

# Flatten dict into [F.lit(k1), F.lit(v1), F.lit(k2), F.lit(v2), ...]
mapping_expr = F.create_map(
    *[x for kv in landuse_map.items() for x in (F.lit(kv[0]), F.lit(kv[1]))]
)

# Add category column
df_pluto = df_pluto.withColumn(
    "landuse_category",
    mapping_expr.getItem(F.col("landuse").cast("string"))
)

# Fill unknowns
df_pluto = df_pluto.withColumn(
    "landuse_category",
    F.when(F.col("landuse_category").isNull(), F.lit("Unknown"))
     .otherwise(F.col("landuse_category"))
)

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
    'Unique_Key',
    'Created_Date', 'Closed_Date', 'created_date_stand', 'closed_date_stand',
    'complaint_type', 'Descriptor', 'complaint_category',
    'Status', 'validation',
    'borough', 'Incident_Zip', 'City',
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
    'latitude_pluto', 'longitude_pluto'
]

# Create the trimmed DataFrame
df_kpi = df_final.select(columns_to_keep)

df_master=df_kpi

df_aff=spark.read.option("header","true").csv("s3://affordable311/Affordable_Housing_Production_by_Building_20250803.csv")

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
    _sum(col("Total Units").cast("int")).alias("total_affordable_units"),
    avg(col("Total Units").cast("int")).alias("avg_units_per_project")
).orderBy("Borough")

df_aff_summary = df_aff_summary.withColumn("borough", upper(col("borough")))
df_master_enriched = df_master_enriched.withColumn("borough", upper(col("borough")))

df_fully_enriched = df_master_enriched.join(
    df_aff_summary,
    on="borough",
    how="left"
)

df=df_fully_enriched

# Lowercase all column names
df = df.toDF(*[c.lower() for c in df.columns])

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
    .withColumn("rank", F.row_number().over(Window.partitionBy("borough").orderBy(F.desc("count"))))

# Get top 5 complaint types
top5_complaints = complaint_ranked.filter("rank <= 5") \
    .select("borough", "complaint_category") \
    .withColumn("top5_complaint_in_borough", F.lit("Yes"))

# Join back to main DF
df = df.join(top5_complaints, on=["borough", "complaint_category"], how="left") \
    .withColumn("top5_complaint_in_borough", F.when(F.col("top5_complaint_in_borough").isNull(), "No").otherwise("Yes"))

building_age_avg_df = df.groupBy("borough") \
    .agg(F.avg("building_age").alias("avg_building_age"))

df = df.join(building_age_avg_df, on="borough", how="left")

df = df.drop("created_date", "closed_date", "ownertype", "ownername",
             "latitude_pluto", "longitude_pluto", "zonedist1", "overlay1", "bbl_standard","descriptor")

df = (
    df.withColumnRenamed("created_date_stand", "created_date")
      .withColumnRenamed("closed_date_stand", "closed_date")
)

df.coalesce(1).write.mode("overwrite").option("header","true").parquet("s3://cdac-final-project-data/Silver-level/transformed_data/")

# Categorical: Fill null with "Unknown"
categorical_cols = [
    "borough", "complaint_category", "complaint_type", "status",
    "incident_zip", "city", "landuse", "landuse_category", "bldgclass"
]
df = df.fillna("Unknown", subset=categorical_cols)

# Numeric: Fill null with 0
numeric_cols = [
    "total_projects", "total_affordable_units", "avg_units_per_project",
    "total_complaints", "validated_complaints", "validation",
    "resolution_time", "building_age", "avg_resolution_time", "avg_building_age",
    "lotarea", "bldgarea", "resarea", "comarea", "unitsres", "unitstotal", "numfloors"
]
df = df.fillna(0, subset=numeric_cols)
df = df.withColumn(
    "validation_category",
    F.when(F.col("validation") == 1, "Legal Violation")
     .when(F.col("validation") == 0, "Non-Legal Violation")
     .otherwise("Unknown")
)

df.coalesce(1).write.mode("overwrite").option("header","true").parquet("s3://cdac-final-project-data/Gold-level/aggregated_data/")

job.commit()