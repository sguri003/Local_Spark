# Running PySpark + Delta Lake locally on a Mac

A quick guide to get Spark running on your own machine — no cluster, no cloud account, nothing to pay for. Just Python scripts you can run and re-run locally.

Should take about 10 minutes.

---

## 1. Install Homebrew

Homebrew is the standard package manager for Mac — it's how you'll install Java in the next step. Skip this if you already have it (check with `brew --version` in Terminal).

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

## 2. Install Java 17

Spark needs Java specifically — version 17 is the safe choice (newer versions can cause compatibility errors).

```bash
brew install openjdk@17
```

If you're on a newer Mac (M1/M2/M3/M4 chip), Homebrew automatically installs the right version for your chip — nothing extra to do.

## 3. Tell your Mac where Java lives

Open `~/.zshrc` in a text editor (or create it if it doesn't exist), and add these two lines:

```bash
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
export PATH="$JAVA_HOME/bin:$PATH"
```

Then reload it and check it worked:

```bash
source ~/.zshrc
java -version
```

You should see `17.x.x` printed.

## 4. Install PySpark and Delta Lake

```bash
pip3 install pyspark delta-spark
```

That's it — no separate Hadoop install needed. (On Windows this step requires an extra workaround; Mac doesn't need it, since it works natively.)

## 5. Create a shared setup file

Instead of repeating the Spark setup in every script, put it in one file you can reuse. Save this as `spark_session.py`:

```python
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

The `enableHiveSupport()` part matters — it means table names you save (with `saveAsTable()`) are still there the next time you run a script, instead of disappearing when the script ends.

## 6. Test it

Save this as `test_spark.py` in the same folder:

```python
from spark_session import get_spark

spark = get_spark("smoke-test")
df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
df.show()
spark.stop()
```

Then run it:

```bash
python3 test_spark.py
```

If you see a small table printed with `id`/`val` columns and no errors, everything's working.

---

## 7. (Optional) Set up Jupyter Notebook

If you'd rather work in notebook cells instead of running whole scripts each time, Jupyter is an easy add. It also has a nice side benefit: Spark stays running between cells, so you're not restarting it (and re-downloading the Delta package) every single time.

Install it:

```bash
pip3 install notebook
```

Launch it from your project folder (the same one with `spark_session.py` in it):

```bash
jupyter notebook
```

This opens Jupyter in your browser. Create a new notebook, and in the first cell:

```python
from spark_session import get_spark
spark = get_spark("notebook-session")
```

Then in later cells, use `spark` freely — read data, try transformations, write output — without re-running the setup each time:

```python
df = spark.table("my_table")
df.show()
```

**One thing to remember:** since Spark keeps running as long as the notebook is open, it also keeps files locked while it's running. If you ever need to delete or rename a table's folder on disk, use Jupyter's **Kernel → Restart** first to shut Spark down cleanly, then make the change.

*(If you'd rather use notebooks inside VS Code instead of the browser, that works too — install the "Jupyter" extension in VS Code, open a `.ipynb` file, and pick your Python interpreter as the kernel. Everything else above works the same way.)*

---

## Reading and writing Delta tables

Once the setup above is working, this is the pattern for saving and loading data as a Delta table (same technology Databricks tables use under the hood):

```python
# write
df.write.format("delta").mode("overwrite").saveAsTable("my_table")

# read
spark.table("my_table").show()
```

That's the whole setup — good luck!
