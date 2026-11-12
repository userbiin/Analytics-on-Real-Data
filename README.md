# Project 3: Analytics on Real Data

* Author: Subin Min
* Class: CS435-535 Section 1
* Semester: Fall 2026

## Overview

This project analyzes the full San Francisco Fire Department Calls for Service data set with PySpark DataFrames. Starting from the class notebook, I adjusted the schema to the new form of the data, repeated the basic operations, answered the six given questions for 2025, and added two questions of my own that use the full data range. The program runs with `spark-submit` on the cscluster00 YARN cluster, and I compared its running time with 4 and 8 executors.

## Reflection

Most of the work was adapting the class notebook to the new data. The new file has 36 columns with spaces in their names, a different timestamp format (`2016 Apr 08 01:59:43 PM`), zip codes written as both `94127` and `94127.0`, and no `Delay` column. Because Spark 4 runs in ANSI mode by default, a wrong format did not silently become `null`. Instead I got `CANNOT_PARSE_TIMESTAMP`, and later `DIVIDE_BY_ZERO` when I rounded a share before dividing by it. Both errors only appeared at an action such as `show()`, which made lazy evaluation very concrete.


## Compiling and Using

The program is written in Python (PySpark), so no compilation is needed. It runs with `spark-submit` on Spark 4.2 and Python 3.11. The input path is the only argument. Results are printed to stdout, and the two Q6 figures are saved as PNG files in the current directory.

```
spark-submit [options] p3_SubinMin.py <input csv>
```

### Sample run (cscluster00, local mode)

```bash
spark-submit --master "local[4]" p3_SubinMin.py \
    hdfs:///user/amit/data/SF-Fire-Calls-20261007-100th.csv > out-sample.txt
```

### Cluster run (cscluster00, YARN)

```bash
export HADOOP_CONF_DIR=/home/amit/hadoop-install/hadoop-3.5.0/etc/hadoop
FULL=hdfs:///user/amit/data/SF-Fire-Calls-20261007.csv

time spark-submit --conf spark.log.level=WARN --master yarn \
    --num-executors 4 --executor-cores 1 p3_SubinMin.py $FULL > out-4.txt
time spark-submit --conf spark.log.level=WARN --master yarn \
    --num-executors 8 --executor-cores 1 p3_SubinMin.py $FULL > out-8.txt
```


## Testing

1. **Development on the sample.** I wrote and checked every query in Jupyter on the 1/100 sample (36 MB). I checked the new schema against the CSV header and with Spark's `CSVHeaderChecker` warning.

2. **Notebook vs. script.** The `spark-submit` run on the sample printed the same results as the notebook (e.g. Tenderloin 532 calls, 71.8% in Q6).

3. **Independent check on the full data.** I aggregated the full CSV with a separate Python script and compared it with the Spark output.

    | Check | Python | Spark |
    |---|---|---|
    | Q2 January / December | 16,374 / 16,340 | 16,374 / 16,340 |
    | Q3 Tenderloin | 24,749 | 24,749 |
    | Q4 South of Market avg / Visitacion Valley median | ≈ 15.5 / 11.3 | 15.48 / 11.3 |
    | Q5 week of Dec 15 | 4,057 | 4,057 |

4. **4 vs. 8 executors.** Both runs produced the same results. Only the row order of unordered lists and the last digit of one floating-point average (`…521` vs `…52`, from a different summation order) differed.

5. **Limits of the sample found during testing.**
    - In the row-level sample, distinct calls ≈ rows, so Q5 picked a different week than the full data.
    - The `None` neighborhood did not appear in the 2025 sample but did in the full data.
    - On the full data, the shares of rare call types rounded to `0.0` and caused `DIVIDE_BY_ZERO`. I fixed this by computing the ratio first and rounding only for display.

## Results (full data)

### Q1. Different types of fire calls in 2025

30 call types: Administrative, Aircraft Emergency, Alarms, Assist Police, Citizen Assist / Service Call, Confined Space / Structure Collapse, Electrical Hazard, Elevator / Escalator Rescue, Explosion, Extrication / Entrapped (Machinery, Vehicle), Fuel Spill, Gas Leak (Natural and LP Gases), HazMat, High Angle Rescue, Industrial Accidents, Marine Fire, Medical Incident, Mutual Aid / Assist Outside Agency, Odor (Strange / Unknown), Other, Outside Fire, Smoke Investigation (Outside), Structure Fire / Smoke in Building, Suspicious Package, Traffic Collision, Train / Rail Fire, Train / Rail Incident, Vehicle Fire, Water Rescue, Watercraft in Distress.

### Q2. Months with the most fire calls in 2025

| Rank | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Month | Jan | Dec | Oct | Aug | May | Sep | Mar | Feb | Jul | Apr | Jun | Nov |
| Calls | 16,374 | 16,340 | 15,659 | 15,363 | 15,346 | 15,268 | 15,240 | 14,948 | 14,911 | 14,711 | 14,621 | 14,544 |

January had the most calls, with December a close second.

### Q3. Neighborhoods with the most fire calls in 2025

| Neighborhood | Calls |
|---|---|
| **Tenderloin** | **24,749** |
| South of Market | 22,848 |
| Mission | 19,193 |
| Financial District/South Beach | 10,937 |
| Bayview Hunters Point | 9,613 |

### Q4. Neighborhoods with the worst response times in 2025

| Neighborhood | Avg (min) | Median (min) |
|---|---|---|
| **South of Market** | **15.48** | 8.40 |
| Lincoln Park | 15.15 | 10.48 |
| Visitacion Valley | 13.97 | **11.30** |
| Presidio | 13.17 | 10.57 |
| Treasure Island | 12.97 | 9.18 |

South of Market is the worst by average, and Visitacion Valley is the worst by median. The two rankings differ because a few extreme delays raise the average, so the median is the more robust measure.

### Q5. Weeks with the most fire calls in 2025

| Week starting | Calls |
|---|---|
| **2025-12-15** | **4,057** |
| 2025-02-03 | 4,009 |
| 2025-02-10 | 3,784 |

### Q6. Correlation between neighborhood, zip code, and number of calls

Neighborhood and zip code are labels, not quantities, so a Pearson correlation of the zip code value against the number of calls would be meaningless. I interpreted the question as: **"Are a neighborhood's calls usually concentrated in one zip code?"**

**71.6%** of 2025 calls came from their neighborhood's top zip code, so in most cases the neighborhood determines the zip code. It is not one-to-one, though, because USPS zip codes and city neighborhood boundaries do not line up.

| Neighborhood | Top zip (share of its calls) | Second zip |
|---|---|---|
| South of Market | 94103 (≈ 85%) | 94107 |
| Tenderloin | 94102 (≈ 66%) | 94109 (≈ 33%) |
| Mission | 94110 (≈ 55%) | 94103 (≈ 45%) |

| Calls by zip code | Share of each neighborhood's calls by zip |
|---|---|
| <img width="1200" height="400" alt="Image" src="https://github.com/user-attachments/assets/eb7a79f7-08a5-4213-b7c7-555c2754e779" /> | <img width="1400" height="1000" alt="Image" src="https://github.com/user-attachments/assets/46222b50-c099-41cb-95cb-0f7296d5bff8" /> |

### Extra Q1. Which call types are more common late at night (0–5 AM)?

In every hour, the top three types are the same: Medical Incident, Alarms, and Structure Fire. To see what changes at night, I computed Lift = (share of night calls) / (share of all calls).

| Call type | Night calls | Lift |
|---|---|---|
| **Outside Fire** | 16,348 | **1.39** |
| **Vehicle Fire** | 2,932 | **1.38** |
| Structure Fire / Smoke in Building | 27,276 | 1.06 |
| Medical Incident | 395,675 | 1.02 |
| Smoke Investigation (Outside) | 1,554 | 0.86 |
| Odor (Strange / Unknown) | 736 | 0.84 |

Outside and vehicle fires are about 1.4x more common at night than over the whole day, while smoke and odor investigations are less common.

### Extra Q2. Are response times improving over the years?

| Year | 2000 | 2005 | 2010 | 2013 | 2014 | 2019 | 2020 | 2022 | 2025 | 2026* |
|---|---|---|---|---|---|---|---|---|---|---|
| Avg (min) | 8.68 | 7.96 | 8.14 | 9.05 | 10.34 | 9.43 | 10.08 | 11.46 | 12.03 | 11.46 |
| Median (min) | 6.42 | 6.48 | 6.25 | 6.98 | 7.43 | 7.35 | 7.37 | 8.03 | 8.18 | 8.23 |


No, response times are getting worse. From 2000 to 2025 the median rose from 6.42 to 8.18 minutes and the average from 8.68 to 12.03 minutes, with jumps around 2013–2014 and 2020–2022. The data also contains invalid delays, so the median is the more reliable measure.

## Performance: 4 vs. 8 executors

| | |
|---|---|
| Cluster | cscluster00 YARN, 8 NodeManagers (cscluster01–08) |
| Setting | `--executor-cores 1`, `--num-executors` 4 or 8 |
| Input | `SF-Fire-Calls-20261007.csv`, 3.64 GB, ≈ 28 input partitions (128 MB each) |

| Executors | real | user | sys |
|---|---|---|---|
| 4 | 2m 23.38s (143.4 s) | 1m 11.07s | 4.24s |
| 8 | 1m 19.57s (79.6 s) | 1m 5.76s | 3.84s |

Speedup: 143.4 s / 79.6 s = 1.80x



## Sources used

- PySpark DataFrame API — https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
- PySpark SQL Functions — https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
- Datetime Patterns for Formatting and Parsing — https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
- Running Spark on YARN — https://spark.apache.org/docs/latest/running-on-yarn.html
- Claude (Anthropic) — used to translate the project specification, to check the format of the new data and draft the parsing code, to debug the code, to suggest ways and candidate methods for examining the Q6 correlation, and to translate my Korean draft of this README into English.
