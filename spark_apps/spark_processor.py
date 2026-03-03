"""spark_processor.py """
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests
from datetime import datetime
import pandas as pd 


# ============================================
# 1. НАСТРОЙКИ
# ============================================

KAFKA_SERVERS = "kafka:9092"
KAFKA_TOPIC = "marketplace-data"
CLICKHOUSE_URL = "http://default:sparkpass@clickhouse:8123"
CLICKHOUSE_DB = "marketplace"

# ============================================
# 2. СХЕМА ДАННЫХ (из вашего генератора)
# ============================================

schema = StructType([
    StructField("record_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price_rub", IntegerType(), True),
    StructField("stock", IntegerType(), True),
    StructField("seller_rating", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("marketplace", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("user_region", StringType(), True),
    StructField("data_quality", StringType(), True)
])

# ============================================
# 3. ИНИЦИАЛИЗАЦИЯ CLICKHOUSE
# ============================================

def init_clickhouse():
    """Создает таблицы в ClickHouse если их нет"""
    print("📦 Инициализация ClickHouse...")
    print(f"🔍 CLICKHOUSE_URL = {CLICKHOUSE_URL}")
    
    # Сначала проверим подключение
    try:
        r = requests.get(CLICKHOUSE_URL, timeout=5)
        if r.status_code != 200:
            print(f"⚠️ ClickHouse ответил кодом {r.status_code}")
    except Exception as e:
        print(f"❌ Не могу подключиться к ClickHouse: {e}")
        return False
    
    # Создаем БД если нет
    try:
        response = requests.post(CLICKHOUSE_URL, data=f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")
        if response.status_code == 200:
            print(f"  ✅ База данных {CLICKHOUSE_DB} создана/существует")
        else:
            print(f"  ⚠️ Ошибка создания БД: {response.text}")
    except Exception as e:
        print(f"  ❌ Ошибка подключения при создании БД: {e}")
        return False
    
    # Таблица для минутной статистики
    create_minute_stats = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.minute_stats (
        event_time DateTime,
        category String,
        marketplace String,
        transactions UInt32,
        revenue Float64,
        avg_price Float64,
        anomalies_count UInt32
    ) ENGINE = MergeTree()
    ORDER BY (event_time, category)
    """
    
    # Таблица для аномалий
    create_anomalies = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.anomalies (
        timestamp DateTime,
        anomaly_type String,
        product_name String,
        price Float64,
        category String
    ) ENGINE = MergeTree()
    ORDER BY (timestamp)
    """
    
    # Таблица для часовых трендов
    create_hourly_trends = f"""
    CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.hourly_trends (
        date Date,
        hour UInt8,
        category String,
        avg_price Float64,
        total_revenue Float64,
        transactions_count UInt32,
        unique_sessions UInt32
    ) ENGINE = MergeTree()
    ORDER BY (date, hour, category)
    """
    
    # Создаем таблицы
    tables_created = 0
    for query in [create_minute_stats, create_anomalies, create_hourly_trends]:
        try:
            print(f"  ⏳ Выполняю: {query[:50]}...")
            response = requests.post(CLICKHOUSE_URL, data=query)
            if response.status_code == 200:
                print(f"  ✅ Таблица создана")
                tables_created += 1
            else:
                print(f"  ⚠️ Ошибка: {response.text}")
        except Exception as e:
            print(f"  ❌ Ошибка подключения: {e}")
            return False
    
    # Проверим, что таблицы действительно создались
    try:
        check_query = f"SHOW TABLES FROM {CLICKHOUSE_DB}"
        r = requests.post(CLICKHOUSE_URL, data=check_query)
        if r.status_code == 200:
            tables = r.text.strip().split('\n')
            print(f"  📋 Таблицы в БД: {tables}")
    except Exception as e:
        print(f"  ⚠️ Не удалось проверить таблицы: {e}")
    
    print(f"✅ ClickHouse готов, создано таблиц: {tables_created}\n")
    return tables_created == 3

# ============================================
# 4. СОХРАНЕНИЕ В CLICKHOUSE
# ============================================

def save_to_clickhouse(df, table_name, batch_id):
    print(f"\n{'='*50}")
    print(f"💾 БАТЧ #{batch_id} в таблицу {table_name}")
    print(f"📊 Строк в батче: {df.count()}")
    
    if df.isEmpty():
        print("⚠️ Батч пуст, пропускаем")
        return
    
    # ===== ОЧИСТКА ДАННЫХ В SPARK =====
    from pyspark.sql.functions import col, regexp_replace, when, lit
    
    # Определяем строковые колонки
    string_columns = [field.name for field in df.schema.fields if field.dataType.typeName() == 'string']
    
    # Очищаем каждую строковую колонку от битых символов
    for column in string_columns:
        df = df.withColumn(
            column,
            when(
                col(column).isNotNull(),
                regexp_replace(col(column), "[^\\x20-\\x7Eа-яА-ЯёЁ0-9\\s\\.,!?-]", "")
            ).otherwise(lit(None))
        )
    
    # Для числовых колонок заменяем NULL на 0
    numeric_columns = [field.name for field in df.schema.fields 
                      if field.dataType.typeName() in ['integer', 'long', 'double', 'float']]
    for column in numeric_columns:
        df = df.withColumn(column, when(col(column).isNull(), lit(0)).otherwise(col(column)))
    
    print("🧹 Данные очищены от битых символов в Spark")
    print("📋 Пример очищенных данных:")
    df.show(2, truncate=False)
    
    try:
        # Конвертируем в pandas
        pandas_df = df.toPandas()
        print(f"✅ Pandas DF создан, строк: {len(pandas_df)}")
        
        # Подготовка значений для SQL
        values_list = []
        for _, row in pandas_df.iterrows():
            row_values = []
            for v in row.values:
                if pd.isna(v) or v is None:
                    row_values.append("NULL")
                elif isinstance(v, (datetime, pd.Timestamp)):
                    row_values.append(f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'")
                elif isinstance(v, str):
                    # Экранируем одинарные кавычки
                    escaped = v.replace("'", "\\'")
                    row_values.append(f"'{escaped}'")
                elif isinstance(v, (int, float)):
                    row_values.append(str(v))
                else:
                    row_values.append(f"'{str(v)}'")
            
            values_list.append(f"({', '.join(row_values)})")
        
        # Вставка чанками
        chunk_size = 1000
        columns = ', '.join([f"`{col}`" for col in pandas_df.columns])
        
        for i in range(0, len(values_list), chunk_size):
            chunk_values = values_list[i:i+chunk_size]
            chunk_str = ', '.join(chunk_values)
            
            query = f"INSERT INTO {CLICKHOUSE_DB}.{table_name} ({columns}) VALUES {chunk_str}"
            
            try:
                r = requests.post(CLICKHOUSE_URL, data=query)
                if r.status_code == 200:
                    print(f"✅ Вставлен чанк {i//chunk_size + 1}: {len(chunk_values)} строк")
                else:
                    print(f"❌ Ошибка вставки чанка: {r.text}")
                    # Диагностика проблемных строк
                    for j, single_values in enumerate(chunk_values):
                        single_query = f"INSERT INTO {CLICKHOUSE_DB}.{table_name} ({columns}) VALUES {single_values}"
                        r2 = requests.post(CLICKHOUSE_URL, data=single_query)
                        if r2.status_code != 200:
                            print(f"  ⚠️ Проблемная строка {i+j}: {r2.text}")
                            # Показываем саму проблемную строку
                            print(f"     Данные: {single_values[:200]}...")
            except Exception as e:
                print(f"❌ Исключение при вставке чанка: {e}")
        
        print(f"✅ Батч #{batch_id} обработан, вставлено строк: {len(pandas_df)}")
        
    except Exception as e:
        print(f"❌ Критическая ошибка в save_to_clickhouse: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"{'='*50}\n")
# ============================================
# 5. ОСНОВНАЯ ОБРАБОТКА
# ============================================

def main():
    print("=" * 70)
    print("🚀 SPARK STREAMING ПРОЦЕССОР")
    print("=" * 70)
    
    # 1. Инициализация ClickHouse
    if not init_clickhouse():
        return
    
    # 2. Создаем Spark сессию
    spark = SparkSession.builder \
        .appName("MarketplaceProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n📥 Чтение из Kafka...")
    
    # 3. Читаем поток из Kafka
    stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    # 4. Парсим JSON
    parsed = stream.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # 5. РАЗДЕЛЯЕМ ПОТОКИ
    normal_data = parsed.filter(col("data_quality") == "normal")
    anomalies = parsed.filter(col("data_quality") != "normal")
    
    print("\n📊 ЗАПУСК АНАЛИТИКИ:\n")
    
    # ========================================
    # ПОТОК 1: Минутная статистика (для онлайн графиков)
    # ========================================
    minute_stats = normal_data \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy(
            window("timestamp", "1 minute"),
            "category",
            "marketplace"
        ) \
        .agg(
            count("*").alias("transactions"),
            sum("price_rub").alias("revenue"),
            avg("price_rub").alias("avg_price")
        ) \
        .select(
            col("window.start").alias("event_time"),
            "category",
            "marketplace",
            "transactions",
            "revenue",
            "avg_price"
        )
    
    # ========================================
    # ПОТОК 2: Аномалии (для мониторинга качества)
    # ========================================
    anomaly_stream = anomalies.select(
        col("timestamp"),
        col("data_quality").alias("anomaly_type"),
        col("product_name"),
        col("price_rub").alias("price"),
        col("category")
    )
    
    # ========================================
    # ПОТОК 3: Часовые тренды (для аналитики)
    # ========================================
    hourly_trends = normal_data \
        .withWatermark("timestamp", "2 hours") \
        .groupBy(
            window("timestamp", "1 hour"),
            "category"
        ) \
        .agg(
            avg("price_rub").alias("avg_price"),
            sum("price_rub").alias("total_revenue"),
            count("*").alias("transactions_count"),
            approx_count_distinct("session_id").alias("unique_sessions")
        ) \
        .select(
            to_date(col("window.start")).alias("date"),
            hour(col("window.start")).alias("hour"),
            "category",
            "avg_price",
            "total_revenue",
            "transactions_count",
            "unique_sessions"
        )
    
    # ========================================
    # ЗАПУСК ВСЕХ ПОТОКОВ
    # ========================================
    
    # Поток 1: Минутная статистика в ClickHouse
    query1 = minute_stats.writeStream \
        .foreachBatch(lambda df, id: save_to_clickhouse(df, "minute_stats", id)) \
        .outputMode("append") \
        .trigger(processingTime="30 seconds") \
        .start()
    print("  ✅ Минутная статистика → ClickHouse")
    
    # Поток 2: Аномалии в ClickHouse
    query2 = anomaly_stream.writeStream \
        .foreachBatch(lambda df, id: save_to_clickhouse(df, "anomalies", id)) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    print("  ✅ Аномалии → ClickHouse")
    
    # Поток 3: Часовые тренды в ClickHouse
    query3 = hourly_trends.writeStream \
        .foreachBatch(lambda df, id: save_to_clickhouse(df, "hourly_trends", id)) \
        .outputMode("append") \
        .trigger(processingTime="5 minutes") \
        .start()
    print("  ✅ Часовые тренды → ClickHouse")
    
    # Поток 4: Онлайн статистика в консоль (для отладки)
    query4 = minute_stats.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="1 minute") \
        .start()
    print("  ✅ Онлайн статистика → Console\n")
    
    print("=" * 70)
    print("✅ ВСЕ ПОТОКИ ЗАПУЩЕНЫ")
    print("📊 Данные сохраняются в ClickHouse")
    print("⏹️  Для остановки: Ctrl+C")
    print("=" * 70)
   
    # Ждем завершения
    query1.awaitTermination()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Остановка...")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()