# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "37815704-37ad-4e76-9a49-9cc0e0addcef",
# META       "default_lakehouse_name": "test_MUCC",
# META       "default_lakehouse_workspace_id": "1d8492a4-6758-4c2d-a97a-1251e07e8443",
# META       "known_lakehouses": [
# META         {
# META           "id": "6940ad95-5b73-4466-9eed-62a0b1c52266"
# META         },
# META         {
# META           "id": "37815704-37ad-4e76-9a49-9cc0e0addcef"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

abfss://tahirokawa_WS@onelake.dfs.fabric.microsoft.com/Lakehouse_tahirokawa_test.Lakehouse/Tables/customer_master

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# 1. ソース読み込み（Lakehouse_tahirokawa_test）
# ============================================================

src_base = (
    "abfss://tahirokawa_WS@onelake.dfs.fabric.microsoft.com/"
    "Lakehouse_tahirokawa_test.Lakehouse/Tables/"
)

df_sales    = spark.read.format("delta").load(src_base + "sales_fact")
df_item     = spark.read.format("delta").load(src_base + "item_master")
df_factory  = spark.read.format("delta").load(src_base + "factory_master")
df_customer = spark.read.format("delta").load(src_base + "customer_master")


# ============================================================
# 2. prefix 付与（衝突ゼロ）
# ============================================================

def add_prefix(df, prefix):
    return df.select([df[col].alias(f"{prefix}_{col}") for col in df.columns])

df_s = add_prefix(df_sales,    "sales")
df_i = add_prefix(df_item,     "item")
df_f = add_prefix(df_factory,  "factory")
df_c = add_prefix(df_customer, "customer")


# ============================================================
# 3. JOIN（prefix同士で安全）
# ============================================================

df_joined = (
    df_s
      .join(df_i, df_s["sales_item_code"] == df_i["item_item_code"], how="left")
      .join(df_f, df_s["sales_factory_code"] == df_f["factory_factory_code"], how="left")
      .join(df_c, df_s["sales_customer_code"] == df_c["customer_customer_code"], how="left")
)


# ============================================================
# 4. 不要なカラム（マスタの登録/更新日時・重複キー）を完全削除
# ============================================================

drop_cols = [
    # マスタ側 key 列の重複
    "item_item_code",
    "factory_factory_code",
    "customer_customer_code",

    # マスタ側 created / updated（不要）
    "item_created_at",
    "item_updated_at",
    "factory_created_at",
    "factory_updated_at",
    "customer_created_at",
    "customer_updated_at",

    # sales 側の created / updated（不要）
    "sales_created_at",
    "sales_updated_at"
]

df_clean = df_joined.drop(*drop_cols)


# ============================================================
# 5. 日本語の論理名への rename（日時系なし）
# ============================================================

rename_map = {
    # --- sales_fact ---
    "sales_sales_date": "売上日",
    "sales_lot_no": "ロット番号",
    "sales_contract_no": "契約番号",
    "sales_shipping_no": "出荷番号",
    "sales_quantity": "数量",
    "sales_amount": "売上金額",
    "sales_cost": "原価",
    "sales_profit": "利益",
    "sales_item_code": "品目コード",
    "sales_factory_code": "工場コード",
    "sales_customer_code": "顧客コード",

    # --- item_master ---
    "item_item_name": "品目名",
    "item_category": "品目カテゴリ",
    "item_unit": "単位",
    "item_unit_price": "単価",

    # --- factory_master ---
    "factory_factory_name": "工場名",
    "factory_region": "工場地域",

    # --- customer_master ---
    "customer_customer_name": "顧客名",
    "customer_industry": "業種",
    "customer_rank": "顧客ランク",
    "customer_region": "顧客地域"
}

df_final = df_clean
for before, after in rename_map.items():
    if before in df_final.columns:
        df_final = df_final.withColumnRenamed(before, after)


# ============================================================
# 6. test_MUCC の Tables に正式テーブルとして保存
# ============================================================

df_final.createOrReplaceTempView("tmp_sales_all")

spark.sql("DROP TABLE IF EXISTS sales_all")

spark.sql("""
    CREATE TABLE sales_all
    USING DELTA
    AS SELECT * FROM tmp_sales_all
""")

print("✔ 完了：test_MUCC の Tables に 'sales_all' を作成しました！")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM test_MUCC.sales_all LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM test_MUCC.sales_all LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # 🏭 製造業サンプルデータ構成（生成テーブル一覧）
# 
# 本ノートブックでは、製造業（コンクリ材料・建材業界）を想定した  
# **マスタ3種 + トランザクション1種** をランダム生成し、  
# Fabric Lakehouse（`/lakehouse/default/Tables/`）に保存します。
# 
# ---
# 
# ## 📦 1. 品目マスタ（item_master）
# 
# コンクリ材料・建材業界で実際に使われる品目名を採用。
# 
# | 列名 | データ型 | 説明 |
# |------|----------|------|
# | item_code | string | 品目コード |
# | item_name | string | 品目名（セメント・骨材・混和材・製品など） |
# | category | string | 原料 / 混和材 / 製品 / 補修材 |
# | unit | string | 単位（kg / L / 個 / 本） |
# | unit_price | int | 単価 |
# 
# ---
# 
# ## 🏭 2. 工場マスタ（factory_master）
# 
# 日本の地域名をベースにした工場マスタ。
# 
# | 列名 | データ型 | 説明 |
# |------|----------|------|
# | factory_code | string | 工場コード（F01〜F10） |
# | factory_name | string | 工場名（北海道工場など） |
# | region | string | 地域（北海道 / 関東 / 東海 など） |
# 
# ---
# 
# ## 🧭 3. 顧客マスタ（customer_master）
# 
# 法人顧客を想定したマスタ。
# 
# | 列名 | データ型 | 説明 |
# |------|----------|------|
# | customer_code | string | 顧客コード（C001〜C010） |
# | customer_name | string | 顧客名（架空の法人名） |
# | industry | string | 業種（製造業 / 商社 / 建設 / 物流 など） |
# | region | string | 地域（関東 / 近畿 / 九州 など） |
# | rank | string | 顧客ランク（A / B / C） |
# 
# ---
# 
# ## 📊 4. 売上トランザクション（sales_fact）
# 
# 4〜6月の3ヶ月間で約3000件をランダム生成。  
# 品目カテゴリ・工場・顧客ランクに偏りを持たせたリアルなデータ。
# 
# | 列名 | データ型 | 説明 |
# |------|----------|------|
# | sales_date | date | 売上日（2024/4〜6） |
# | item_code | string | 品目コード |
# | factory_code | string | 工場コード |
# | customer_code | string | 顧客コード |
# | lot_no | string | ロット番号（LOT-YYYYMMDD-XXXX） |
# | quantity | int | 数量（カテゴリ別に偏りあり） |
# | amount | int | 売上金額（unit_price × quantity） |
# | cost | int | 原価（単価の50〜90% × 数量） |
# | profit | int | 利益（amount − cost） |
# 
# ---


# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import random

spark = SparkSession.builder.getOrCreate()

# ============================================================
# 1. 工場マスタ
# ============================================================
factory_data = [
    ("F01", "北海道工場", "北海道"),
    ("F02", "東北工場", "東北"),
    ("F03", "関東工場", "関東"),
    ("F04", "甲信越工場", "甲信越"),
    ("F05", "北陸工場", "北陸"),
    ("F06", "東海工場", "東海"),
    ("F07", "近畿工場", "近畿"),
    ("F08", "中国工場", "中国"),
    ("F09", "四国工場", "四国"),
    ("F10", "九州工場", "九州")
]

factory_schema = StructType([
    StructField("factory_code", StringType()),
    StructField("factory_name", StringType()),
    StructField("region", StringType())
])

factory_df = spark.createDataFrame(factory_data, factory_schema)

factory_weights = {
    "F03": 0.25,
    "F06": 0.15,
    "F07": 0.15,
    "F01": 0.05,
    "F02": 0.05,
    "F04": 0.05,
    "F05": 0.05,
    "F08": 0.10,
    "F09": 0.10,
    "F10": 0.05
}

# ============================================================
# 2. 顧客マスタ
# ============================================================
customer_data = [
    ("C001", "東日本精工株式会社", "製造業", "関東", "A"),
    ("C002", "西日本部材工業株式会社", "製造業", "近畿", "A"),
    ("C003", "中央商事株式会社", "商社", "中部", "B"),
    ("C004", "北海道テクノロジー株式会社", "製造業", "北海道", "B"),
    ("C005", "九州ロジスティクス株式会社", "物流", "九州", "C"),
    ("C006", "東海マテリアル株式会社", "素材", "東海", "A"),
    ("C007", "関西建設資材株式会社", "建設", "近畿", "B"),
    ("C008", "四国パーツ販売株式会社", "卸売", "四国", "C"),
    ("C009", "東北エンジニアリング株式会社", "製造業", "東北", "B"),
    ("C010", "甲信越サプライ株式会社", "商社", "甲信越", "C")
]

customer_schema = StructType([
    StructField("customer_code", StringType()),
    StructField("customer_name", StringType()),
    StructField("industry", StringType()),
    StructField("region", StringType()),
    StructField("rank", StringType())
])

customer_df = spark.createDataFrame(customer_data, customer_schema)

customer_weights = {"A": 0.5, "B": 0.3, "C": 0.2}

# ============================================================
# 3. 品目マスタ（コンクリ材料）
# ============================================================
item_data = [
    ("C001", "普通ポルトランドセメント", "原料", "kg", 12),
    ("C002", "高炉セメントB種", "原料", "kg", 10),
    ("C003", "早強ポルトランドセメント", "原料", "kg", 15),
    ("A001", "川砂（細骨材）", "原料", "kg", 3),
    ("A002", "砕砂（細骨材）", "原料", "kg", 2),
    ("A003", "砕石 20-5", "原料", "kg", 4),
    ("A004", "再生砕石 RC-40", "原料", "kg", 2),
    ("M001", "フライアッシュ（FA）", "混和材", "kg", 8),
    ("M002", "高性能AE減水剤", "混和材", "L", 120),
    ("M003", "AE剤", "混和材", "L", 90),
    ("M004", "膨張材", "混和材", "kg", 20),
    ("P001", "U字溝 300", "製品", "個", 1500),
    ("P002", "L型擁壁 1500", "製品", "個", 8000),
    ("P003", "ヒューム管 300", "製品", "本", 5000),
    ("P004", "インターロッキングブロック", "製品", "個", 200),
    ("R001", "無収縮モルタル", "補修材", "kg", 25),
    ("R002", "左官用モルタル", "補修材", "kg", 18),
    ("R003", "コンクリート養生剤", "補修材", "L", 150)
]

item_schema = StructType([
    StructField("item_code", StringType()),
    StructField("item_name", StringType()),
    StructField("category", StringType()),
    StructField("unit", StringType()),
    StructField("unit_price", IntegerType())
])

item_df = spark.createDataFrame(item_data, item_schema)

item_weights = {
    "製品": 0.40,
    "原料": 0.30,
    "混和材": 0.20,
    "補修材": 0.10
}

# ============================================================
# 4. 日付ディメンション
# ============================================================
date_df = (
    spark.sql("""
        SELECT explode(sequence(to_date('2024-04-01'), to_date('2024-06-30'), interval 1 day)) AS date
    """)
    .withColumn("date_key", date_format("date", "yyyyMMdd"))
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .withColumn("day", dayofmonth("date"))
    .withColumn("weekday", date_format("date", "E"))
    .withColumn("weekofyear", weekofyear("date"))
    .withColumn("quarter", quarter("date"))
)

# ============================================================
# 5. トランザクション生成（3000件）
# ============================================================

n = 3000

def weighted_choice(weight_dict):
    keys = list(weight_dict.keys())
    weights = list(weight_dict.values())
    return random.choices(keys, weights=weights, k=1)[0]

rows = []
for i in range(n):

    sales_date = date_df.orderBy(rand()).limit(1).collect()[0]["date"]

    factory_code = weighted_choice(factory_weights)

    rank = weighted_choice(customer_weights)
    customer_row = customer_df.filter(col("rank") == rank).orderBy(rand()).limit(1).collect()[0]
    customer_code = customer_row["customer_code"]

    category = weighted_choice(item_weights)
    item_row = item_df.filter(col("category") == category).orderBy(rand()).limit(1).collect()[0]
    item_code = item_row["item_code"]
    unit_price = item_row["unit_price"]

    if category == "原料":
        quantity = random.randint(200, 2000)
    elif category == "製品":
        quantity = random.randint(1, 10)
    else:
        quantity = random.randint(10, 200)

    amount = unit_price * quantity
    cost_unit = int(unit_price * random.uniform(0.5, 0.9))
    cost = cost_unit * quantity
    profit = amount - cost

    lot_no = f"LOT-{sales_date.strftime('%Y%m%d')}-{i:04d}"

    rows.append((sales_date, item_code, factory_code, customer_code, lot_no, quantity, amount, cost, profit))

fact_schema = StructType([
    StructField("sales_date", DateType()),
    StructField("item_code", StringType()),
    StructField("factory_code", StringType()),
    StructField("customer_code", StringType()),
    StructField("lot_no", StringType()),
    StructField("quantity", IntegerType()),
    StructField("amount", IntegerType()),
    StructField("cost", IntegerType()),
    StructField("profit", IntegerType())
])

sales_df = spark.createDataFrame(rows, fact_schema)

# ============================================================
# 6. Lakehouse 保存（フォルダ削除 → 保存）
# ============================================================

paths = [
    "/lakehouse/default/Tables/item_master",
    "/lakehouse/default/Tables/factory_master",
    "/lakehouse/default/Tables/customer_master",
    "/lakehouse/default/Tables/sales_fact",
    "/lakehouse/default/Tables/date_dim"
]

# 既存フォルダ削除
for p in paths:
    if mssparkutils.fs.exists(p):
        mssparkutils.fs.rm(p, recurse=True)

# 保存
item_df.write.format("delta").mode("overwrite").save(paths[0])
factory_df.write.format("delta").mode("overwrite").save(paths[1])
customer_df.write.format("delta").mode("overwrite").save(paths[2])
sales_df.write.format("delta").mode("overwrite").save(paths[3])
date_df.write.format("delta").mode("overwrite").save(paths[4])

print("Lakehouse への保存が完了しました")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
