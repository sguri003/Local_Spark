# Running PySpark + Delta Lake locally on Windows

Windows needs a few extra fixes that Mac/Linux don't (see the [Mac guide](mac_pyspark_setup_guide.md) for comparison). This covers all of them in order.

Should take about 20 minutes.

---

## 1. Install Java 17

Spark needs Java 17 or 21. If you already have a different JDK version installed for other work, install 17 *alongside* it — no need to replace anything.

```powershell
winget install EclipseAdoptium.Temurin.17.JDK
```

Find where it installed:

```powershell
Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Directory
```

## 2. Set up Hadoop's Windows compatibility files

Windows is missing some file-permission calls that Spark's Hadoop libraries expect. Two small files patch that — this is **not** a full Hadoop install, just two files.

Create the folder:

```powershell
New-Item -ItemType Directory -Force -Path "C:\hadoop\bin"
```

Download `winutils.exe` and `hadoop.dll` from the `hadoop-3.3.6/bin` folder of the community-maintained [cdarlint/winutils](https://github.com/cdarlint/winutils) GitHub repo (Apache doesn't ship an official Windows build, so this is the standard source the Spark community uses), and place both files in `C:\hadoop\bin`.

## 3. Install PySpark and Delta Lake

```powershell
pip install pyspark delta-spark
```

## 4. Find your Python executable's exact path

Windows sometimes resolves the `python` command to a Microsoft Store placeholder instead of your real install, which silently breaks Spark. Pinning the exact path avoids this.

```powershell
(Get-Command python).Source
```

Keep this path handy for the next step.

## 5. Write one shared setup file

This bakes every Windows fix into the script itself, so nothing needs setting manually in the terminal each session. Save as `spark_session.py`:

```python
import os

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot"  # match your actual folder name
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = (
    os.environ["JAVA_HOME"] + r"\bin;"
    + os.environ["HADOOP_HOME"] + r"\bin;"
    + os.environ["PATH"]
)
os.environ["PYSPARK_PYTHON"] = r"C:\path\to\your\python.exe"  # from step 4
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\path\to\your\python.exe"
os.environ.pop("SPARK_HOME", None)  # let pip's bundled pyspark use its own launcher

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

_JAVA_OPTS = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
)

def get_spark(app_name="local-job"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", _JAVA_OPTS)
        .enableHiveSupport()
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
```

`.enableHiveSupport()` matters — it makes table names saved with `saveAsTable()` persist across separate script runs instead of resetting every time.

## 6. Run the smoke test

```python
from spark_session import get_spark

spark = get_spark("smoke-test")
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df.show()
spark.stop()
```

```powershell
python test_spark.py
```

A small table printed with `id`/`val` columns and no errors means everything's wired correctly.

## 7. (Optional) Add Jupyter Notebook

```powershell
pip install notebook
jupyter notebook
```

First cell of a new notebook:

```python
from spark_session import get_spark
spark = get_spark("notebook-session")
```

**Two gotchas specific to notebooks on Windows:**

- Spark keeps files under `spark-warehouse/` locked while the kernel is alive. Use **Kernel → Restart** before deleting or renaming any table folder.
- If a Spark session crashes or is stopped mid-notebook, calling `get_spark()` again in the *same* kernel can throw `ConnectionRefusedError` — it's trying to reconnect to a JVM that no longer exists. Restart the kernel to clear the stale state, then rerun from the top.

---

## Example: pulling live data with yfinance

A full worked example — pulls live market data with `yfinance`, cleans it up with pandas, loads it into Spark, and saves it as a Delta table.

Install the extra packages this example needs:

```powershell
pip install yfinance pandas numpy
```

```python
import yfinance as yf
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from spark_session import get_spark

TICKERS = {
    'GC=F': 'Gold',
    'SI=F': 'Silver',
    'PL=F': 'Platinum',
    'PA=F': 'Palladium',
    'HG=F': 'Copper',
    'CL=F': 'WTI Crude Oil',
    'BZ=F': 'Brent Crude',
    'XEL':  'Xcel Energy',
    'CVX':  'Chevron',
    'BAC':  'Bank of America',
    'BAH':  'Booz Allen Hamilton',
}

spark = get_spark("stocks")

ticker_lst = list(TICKERS.keys())
dt = yf.download(ticker_lst, start='2020-01-01', group_by='ticker')

dt = pd.DataFrame(data=dt)
dt_f = dt.reset_index()
dt_f.columns = ['_'.join(col).strip() for col in dt_f.columns.values]   # flatten multi-index columns
dt_f.columns = ["".join(col).replace('=', '_') for col in dt_f.columns.values]  # e.g. GC=F -> GC_F
dt_f = np.round(dt_f, decimals=2)

df_spark = spark.createDataFrame(dt_f)
df_spark = df_spark.withColumn('DateKey', F.date_format(F.col('Date_'), 'yyyyMMdd').cast("int"))
df_spark = df_spark.withColumn('Date_', F.date_format(F.col('Date_'), 'yyyy-MM-dd'))
df_spark.printSchema()
df_spark.show()

df_spark.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("stocks")
```

Read it back:
```python
spark.table("stocks").show()
```

**A note on running this in a notebook** — this is the exact script that hit the `ConnectionRefusedError` covered in step 7's gotchas. If `get_spark("stocks")` throws that error, it means a previous cell's Spark session died and this cell is trying to reconnect to a JVM that's gone. Restart the kernel and rerun from the top.

---

## Quick reference — what each fix solves

| Symptom | Caused by skipping |
|---|---|
| `IllegalAccessError: sun.nio.ch.DirectBuffer` | Missing `--add-opens` flags (step 5) |
| `Did not find winutils.exe` | Missing Hadoop files (step 2) |
| Python worker times out / "Python was not found" | Store-alias stub instead of pinned path (step 4) |
| `FileNotFoundError [WinError 2]` on startup | Stale `SPARK_HOME` not cleared (step 5) |
| Table exists after write but "not found" on next run | Missing `.enableHiveSupport()` (step 5) |
