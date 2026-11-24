# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window

dbutils.widgets.text('file_path', 'abfss://bronze@marekdelearning01.dfs.core.windows.net/payments_exam/')
file_path = dbutils.widgets.get('file_path')
df = (spark.read.csv(file_path, header = True).withColumn('source_file', F.col('_metadata.file_path')))

# print('Liczba rekordów:', df.count())
# df.show()

df_clean = (df
            .withColumn('payment_id_clean', F.trim(F.upper(F.col('payment_id'))))
            .withColumn('order_id_clean', F.trim(F.upper(F.col('order_id'))))
            .withColumn('amount', F.expr('try_cast(replace(trim(amount_raw), ",",".") AS decimal(12,2))'))
            .withColumn('status', F.upper(F.trim(F.col('status_raw')))).withColumn('status', F.when(F.col('status').isin('SUCCESS','PENDING','FAILED'),
                F.col('status')).otherwise(None))
            .withColumn('updated_at', F.expr('try_to_timestamp(trim(updated_at_raw), "yyyy-MM-dd HH:mm:ss")'))
            .withColumn('event_id_clean', F.expr('try_cast(trim(event_id) AS INT)'))
            .withColumn('rejected_reason', F.when((F.col('payment_id_clean').isNull()) | (F.col('payment_id_clean') == ''), 'WRONG_PAYMENT_ID')
                .when((F.col('order_id_clean').isNull()) | (F.col('order_id_clean') == ''), 'WRONG_ORDER_ID')
                .when(((F.col('status').isNull()) | (F.col('status') == '')), 'WRONG_STATUS')
                .when(F.col('updated_at').isNull(), 'WRONG_UPDATED_AT')
                .when(((F.col('amount').isNull()) | (F.col('amount')<=0)), 'WRONG_AMOUNT')
                .when((F.col('event_id_clean').isNull()), 'WRONG_EVENT_ID')
                .otherwise('CORRECT_ROW')))

valid_rows = (df_clean.filter(F.col('rejected_reason')=='CORRECT_ROW'))
rejected_rows = (df_clean.filter(F.col('rejected_reason')!='CORRECT_ROW'))

w = (Window.partitionBy(F.col('payment_id_clean')).orderBy(F.col('updated_at').desc(), F.col('event_id_clean').cast('int').desc()))
newest_rows = (valid_rows.withColumn('rn', F.row_number().over(w)).drop('rejected_reason'))
older_rows = (newest_rows.filter(F.col('rn')>1).drop('rn'))
newest_rows = (newest_rows.filter(F.col('rn')==1).drop('rn'))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ad_payments_exam.default.payments_silver (
# MAGIC     payment_id_clean STRING NOT NULL,
# MAGIC     order_id_clean STRING NOT NULL,
# MAGIC     amount DECIMAL(12,2) NOT NULL,
# MAGIC     status STRING NOT NULL,
# MAGIC     updated_at TIMESTAMP NOT NULL,
# MAGIC     event_id_clean INTEGER NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ad_payments_exam.default.payments_rejected (
# MAGIC     payment_id STRING,
# MAGIC     order_id STRING,
# MAGIC     amount_raw STRING,
# MAGIC     status_raw STRING,
# MAGIC     updated_at_raw STRING,
# MAGIC     event_id STRING,
# MAGIC     rejected_reason STRING,
# MAGIC     source_file STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

silver_source = newest_rows.select(
    F.col('payment_id_clean'),
    F.col('order_id_clean'),
    F.col('amount'),
    F.col('status'),
    F.col('updated_at'),
    F.col('event_id_clean'))

rejected_source = rejected_rows.select(
    F.col('payment_id'),
    F.col('order_id'),
    F.col('amount_raw'),
    F.col('status_raw'),
    F.col('updated_at_raw'),
    F.col('event_id'),
    F.col('rejected_reason'),
    F.col('source_file'))

# COMMAND ----------

silver_source.createOrReplaceTempView("silver_source")
existing = spark.table('ad_payments_exam.default.payments_rejected')
new_rejected = rejected_source.subtract(existing)
new_rejected.write.format('delta').mode('append').saveAsTable('ad_payments_exam.default.payments_rejected')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO ad_payments_exam.default.payments_silver AS t
# MAGIC USING silver_source AS s
# MAGIC ON s.payment_id_clean = t.payment_id_clean
# MAGIC
# MAGIC WHEN MATCHED AND (s.updated_at > t.updated_at OR (s.updated_at = t.updated_at AND s.event_id_clean > t.event_id_clean)) THEN
# MAGIC     UPDATE SET 
# MAGIC         t.order_id_clean = s.order_id_clean,
# MAGIC         t.amount = s.amount,
# MAGIC         t.status = s.status,
# MAGIC         t.updated_at = s.updated_at,
# MAGIC         t.event_id_clean = s.event_id_clean
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (payment_id_clean, order_id_clean, amount, status, updated_at, event_id_clean)
# MAGIC     VALUES (s.payment_id_clean, s.order_id_clean, s.amount, s.status, s.updated_at, s.event_id_clean);

# COMMAND ----------

print("Wszystkie:", df.count())
print("Valid:", valid_rows.count())
print("Rejected:", rejected_rows.count())
print("Po deduplikacji:", newest_rows.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM ad_payments_exam.default.payments_rejected;