#!/usr/bin/env python
# Project 3: Analytics on Real Data (SF Fire Calls)
# Usage: spark-submit p3_SubinMin.py <path-to-SF-Fire-Calls.csv>

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None


def section(title):
    print(f"\n{title}")


if len(sys.argv) != 2:
    print("Usage: spark-submit p3_SubinMin.py <path-to-SF-Fire-Calls.csv>", file=sys.stderr)
    sys.exit(1)

spark = (SparkSession
         .builder
         .appName("DF SF Fire Example")
         .getOrCreate())


# Reading structured data using an explicit schema (cheaper than inferring it)
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('ReceivedDtTm', StringType(), True),
                          StructField('EntryDtTm', StringType(), True),
                          StructField('DispatchDtTm', StringType(), True),
                          StructField('ResponseDtTm', StringType(), True),
                          StructField('OnSceneDtTm', StringType(), True),
                          StructField('TransportDtTm', StringType(), True),
                          StructField('HospitalDtTm', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', DoubleType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('DataAsOf', StringType(), True),
                          StructField('DataLoadedAt', StringType(), True)])

sf_fire_file = sys.argv[1]
timestamp_format = "yyyy MMM dd hh:mm:ss a"

# Delay = minutes from call received to unit on scene
fire_df = (spark.read.csv(sf_fire_file, header=True, schema=fire_schema)
           .withColumn("Zipcode", col("Zipcode").cast("int"))
           .withColumn("Delay", (unix_timestamp("OnSceneDtTm", timestamp_format)
                                 - unix_timestamp("ReceivedDtTm", timestamp_format)) / 60))


# Projections (select columns) and filters (select rows)
section("Non-medical calls")
few_fire_df = (fire_df.select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, False)

# How many distinct call types are there?
section("Number of distinct call types")
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(count_distinct("CallType").alias("DistinctCallTypes"))
 .show(truncate=False))

# List the distinct call types
section("Distinct call types")
(fire_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(truncate=False))


# Renaming, adding, and dropping columns
fire_df.cache()
fire_df.count()

# Rename Delay and look at response times over five minutes
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")

section("Response delays over 5 minutes")
(new_fire_df
 .select("ResponseDelayedinMins")
 .where(col("ResponseDelayedinMins") > 5)
 .show(5, False))

# Convert date/time strings to timestamps
fire_ts_df = (new_fire_df
              .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
              .drop("CallDate")
              .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
              .drop("WatchDate")
              .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), timestamp_format))
              .drop("AvailableDtTm"))

section("Converted timestamp columns")
(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, False))

# Years covered by the data set
section("Years in the data set")
(fire_ts_df
 .select(year('IncidentDate'))
 .distinct()
 .orderBy(year('IncidentDate'))
 .show(30))


# Aggregations: most common call types
section("Most common call types")
(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(truncate=False))

# Statistical functions: sum, avg, min, max
section("Alarm and response delay statistics")
(fire_ts_df
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
         F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
 .show())


# Further data explorations (2025)
fire_2025_df = fire_ts_df.where(year("IncidentDate") == 2025).cache()

# Q1. What were all the different types of fire calls in 2025?
section("Q1. Different types of fire calls in 2025")
(fire_2025_df
 .where(col("CallType").isNotNull())
 .select("CallType")
 .distinct()
 .show(50, truncate=False))

# Q2. What months within the year 2025 saw the highest number of fire calls?
section("Q2. Months with the most fire calls in 2025")
(fire_2025_df
 .withColumn("Month", month("IncidentDate"))
 .groupBy("Month")
 .agg(count_distinct("CallNumber").alias("Calls"))
 .orderBy("Calls", ascending=False)
 .show())

# Q3. Which neighborhood in San Francisco generated the most fire calls in 2025?
section("Q3. Neighborhoods with the most fire calls in 2025")
(fire_2025_df
 .where(col("Neighborhood") != "None")
 .groupBy("Neighborhood")
 .agg(count_distinct("CallNumber").alias("Calls"))
 .orderBy("Calls", ascending=False)
 .show(truncate=False))

# Q4. Which neighborhoods had the worst response times to fire calls in 2025?
section("Q4. Neighborhoods with the worst response times in 2025")
(fire_2025_df
 .where((col("ResponseDelayedinMins") >= 0) & (col("Neighborhood") != "None"))
 .groupBy("Neighborhood")
 .agg(round(avg("ResponseDelayedinMins"), 2).alias("AvgDelayMins"),
      round(median("ResponseDelayedinMins"), 2).alias("MedianDelayMins"))
 .orderBy("AvgDelayMins", ascending=False)
 .show(truncate=False))

# Q5. Which week in 2025 had the most fire calls?
section("Q5. Weeks with the most fire calls in 2025")
(fire_2025_df
 .withColumn("Week", to_date(date_trunc("week", "IncidentDate")))
 .groupBy("Week")
 .agg(count_distinct("CallNumber").alias("Calls"))
 .orderBy("Calls", ascending=False)
 .show())

# Q6. Is there a correlation between neighborhood, zip code, and number of fire calls?
# Interpreted as: are a neighborhood's calls usually concentrated in one zip code?
section("Q6. Calls by neighborhood and zip code in 2025")
pair_df = (fire_2025_df
           .where(col("Zipcode").isNotNull() & (col("Neighborhood") != "None"))
           .groupBy("Neighborhood", "Zipcode")
           .agg(count_distinct("CallNumber").alias("Calls"))
           .cache())

pair_df.orderBy("Calls", ascending=False).show(truncate=False)

# Share of calls in their neighborhood's most common (representative) zip
section("Q6. Share of calls in the neighborhood's top zip")
top_zip_calls, total_calls = (pair_df
                              .groupBy("Neighborhood")
                              .agg(F.max("Calls").alias("TopZipCalls"),
                                   F.sum("Calls").alias("TotalCalls"))
                              .agg(F.sum("TopZipCalls"), F.sum("TotalCalls"))
                              .first())
print(f"Calls in their neighborhood's top zip: {top_zip_calls / total_calls:.1%}")

if plt is not None:
    # Calls by zip code
    zip_pdf = (pair_df.groupBy("Zipcode")
               .agg(F.sum("Calls").alias("Calls"))
               .orderBy(F.desc("Calls"))
               .toPandas())

    plt.figure(figsize=(12, 4))
    plt.bar(zip_pdf["Zipcode"].astype(str), zip_pdf["Calls"])
    plt.xticks(rotation=90)
    plt.ylabel("Calls")
    plt.title("Calls by Zip (2025)")
    plt.tight_layout()
    plt.savefig("q6_calls_by_zip.png")
    plt.close()

    # Heatmap: rows = neighborhoods, columns = zip codes,
    # color = share of the neighborhood's calls from that zip
    pair_pdf = (pair_df.toPandas()
                .pivot(index="Neighborhood", columns="Zipcode", values="Calls")
                .fillna(0))
    row_share = pair_pdf.div(pair_pdf.sum(axis=1), axis=0)

    plt.figure(figsize=(14, 10))
    plt.imshow(row_share, aspect="auto", cmap="Reds", vmin=0, vmax=1)
    plt.xticks(range(len(row_share.columns)), row_share.columns.astype(str), rotation=90)
    plt.yticks(range(len(row_share.index)), row_share.index)
    plt.colorbar(label="Share of neighborhood's calls")
    plt.title("Share of each neighborhood's calls by zip (2025)")
    plt.tight_layout()
    plt.savefig("q6_neighborhood_zip_heatmap.png")
    plt.close()

    print("Saved q6_calls_by_zip.png and q6_neighborhood_zip_heatmap.png")
else:
    print("matplotlib not available; skipping Q6 charts")

# Result: about 71.8% of calls were in their neighborhood's representative zip,
# so a neighborhood's calls can generally be tied to one zip code.


# More explorations (full data range)

# Extra Q1. Distribution of call types by hour: which types are common in the early morning?
section("Extra Q1. Call type share by hour")

hour_type_df = (fire_ts_df
                .where(col("CallType").isNotNull())
                .withColumn("Hour", hour(to_timestamp("ReceivedDtTm", timestamp_format)))
                .where(col("Hour").isNotNull())
                .groupBy("Hour", "CallType")
                .agg(count_distinct("CallNumber").alias("Calls"))
                .withColumn("ShareInHour",
                            round(col("Calls") / sum("Calls").over(Window.partitionBy("Hour")), 3))
                .cache())

rank_win = Window.partitionBy("Hour").orderBy(F.desc("Calls"))

(hour_type_df
 .withColumn("Rank", F.row_number().over(rank_win))
 .where(col("Rank") <= 3)
 .orderBy("Hour", "Rank")
 .show(72, truncate=False))


# Call types relatively more common at 0-5 AM
# Lift = early-morning share / all-day share
min_calls = 100

night_df = (hour_type_df.where(col("Hour").between(0, 5))
            .groupBy("CallType").agg(sum("Calls").alias("NightCalls")))
all_df = hour_type_df.groupBy("CallType").agg(sum("Calls").alias("AllCalls"))
night_total = night_df.agg(sum("NightCalls")).first()[0]
all_total = all_df.agg(sum("AllCalls")).first()[0]

section("Extra Q1. Call types relatively more common at 0-5 AM")
(night_df.join(all_df, "CallType")
 .withColumn("NightShare", col("NightCalls") / night_total)
 .withColumn("AllShare", col("AllCalls") / all_total)
 .withColumn("Lift", col("NightShare") / col("AllShare"))
 .where(col("NightCalls") >= min_calls)
 .select("CallType", "NightCalls", "AllCalls",
         F.round("NightShare", 4).alias("NightShare"),
         F.round("AllShare", 4).alias("AllShare"),
         F.round("Lift", 2).alias("Lift")) 
 .orderBy(F.desc("Lift"))
 .show(10, truncate=False))

# Result: fire types such as Vehicle Fire (Lift 1.5) and Outside Fire (1.21) are
# more common in the early morning; Traffic Collision (0.78) and Citizen Assist
# (0.81) are less common. Medical Incident is about 2/3 of calls at every hour.

# Extra Q2. Yearly trend of mean/median response time: is response time improving?
section("Extra Q2. Mean/median response time by year")
(fire_ts_df
 .where(col("ResponseDelayedinMins") >= 0)
 .groupBy(year("IncidentDate").alias("Year"))
 .agg(F.round(F.avg("ResponseDelayedinMins"), 2).alias("AvgDelayMins"),
      F.round(F.median("ResponseDelayedinMins"), 2).alias("MedianDelayMins"))
 .orderBy("Year")
 .show(30))

# Result: response times have not gotten shorter over the years; if anything,
# they have gotten longer.

spark.stop()
