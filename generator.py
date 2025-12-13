"""
marketplace_generator.py
Личный генератор данных маркетплейса.
Сделано для тестов, а не для красоты кода.
"""

import random
import json
from datetime import datetime, timedelta
import asyncio
from typing import Dict, Any, Optional, Union

# ------------------------------------------------------------
# 1. КАТАЛОГ ТОВАРОВ (можно менять под свои нужды)
# ------------------------------------------------------------
PRODUCT_CATALOG = [
    {"id": "PH_001", "name": "Смартфон", "category": "Электроника", 
     "min_price": 3000, "max_price": 10000, "typical_stock": (5, 50)},
    {"id": "TB_001", "name": "Планшет", "category": "Электроника", 
     "min_price": 5000, "max_price": 15000, "typical_stock": (3, 30)},
    {"id": "NB_001", "name": "Ноутбук", "category": "Электроника", 
     "min_price": 20000, "max_price": 50000, "typical_stock": (1, 20)},
    {"id": "MT_001", "name": "Матрёшка", "category": "Сувениры", 
     "min_price": 500, "max_price": 3000, "typical_stock": (10, 100)},
    {"id": "RC_001", "name": "Расчёска", "category": "Бытовое", 
     "min_price": 100, "max_price": 500, "typical_stock": (50, 200)},
    {"id": "SH_001", "name": "Шампунь", "category": "Косметика", 
     "min_price": 200, "max_price": 800, "typical_stock": (20, 150)},
    {"id": "HD_001", "name": "Наушники", "category": "Электроника", 
     "min_price": 1000, "max_price": 5000, "typical_stock": (5, 60)},
    {"id": "BK_001", "name": "Книга", "category": "Книги", 
     "min_price": 300, "max_price": 1500, "typical_stock": (15, 80)},
    {"id": "TS_001", "name": "Футболка", "category": "Одежда", 
     "min_price": 500, "max_price": 2000, "typical_stock": (25, 120)},
    {"id": "CF_001", "name": "Кофе", "category": "Продукты", 
     "min_price": 400, "max_price": 1200, "typical_stock": (30, 200)}
]

MARKETPLACES = ["Wildberries", "Ozon", "Яндекс.Маркет", "СберМегаМаркет", "AliExpress"]

# ------------------------------------------------------------
# 2. ТИПЫ АНОМАЛИЙ (можно отключить, закомментировав)
# ------------------------------------------------------------
ANOMALY_PROFILE = {
    "missing_fields": 0.03,       # 3% - пропущены поля
    "extra_quotes": 0.02,         # 2% - лишние кавычки
    "negative_price": 0.01,       # 1% - отрицательная цена
    "huge_price": 0.005,          # 0.5% - цена в 1000 раз выше
    "future_timestamp": 0.01,     # 1% - дата из будущего
    "malformed_json": 0.015,      # 1.5% - испорченный JSON
    "duplicate": 0.02,            # 2% - дубликат
    "null_values": 0.01,          # 1% - все поля NULL
    "sql_injection": 0.005,       # 0.5% - SQL-инъекция
    "binary_garbage": 0.003,      # 0.3% - бинарный мусор
    "encoding_problem": 0.01,     # 1% - проблемы кодировки
    "empty_record": 0.005,        # 0.5% - пустая запись
    "normal": 0.85                # 85% - нормальные данные
}

# ------------------------------------------------------------
# 3. ГЕНЕРАТОР РЕАЛИСТИЧНЫХ ДАННЫХ
# ------------------------------------------------------------
def generate_healthy_record(counter: int) -> Dict[str, Any]:
    """Генерация НОРМАЛЬНОЙ записи о товаре"""
    product = random.choice(PRODUCT_CATALOG)
    
    # Цена с учётом времени суток (ночью дешевле)
    base_price = random.randint(product["min_price"], product["max_price"])
    
    hour = datetime.now().hour
    if 0 <= hour < 6:  # Ночью
        price_mod = random.uniform(0.95, 1.0)
    elif 20 <= hour <= 23:  # Вечером
        price_mod = random.uniform(1.0, 1.05)
    else:
        price_mod = random.uniform(0.98, 1.02)
    
    price = int(base_price * price_mod)
    
    # Остаток
    min_stock, max_stock = product["typical_stock"]
    stock = random.randint(min_stock, max_stock)
    
    # Популярные товары быстрее заканчиваются
    if product["category"] == "Электроника":
        stock = max(1, stock - random.randint(0, 10))
    
    # Рейтинг продавца (большинство высокие, но есть и плохие)
    if random.random() < 0.1:  # 10% продавцов с низким рейтингом
        rating = round(random.uniform(2.0, 3.5), 1)
    else:
        rating = round(random.uniform(4.0, 5.0), 1)
    
    # Временная метка с небольшим "дрейфом" (±2 минуты)
    time_offset = random.randint(-120, 120)
    timestamp = (datetime.now() + timedelta(seconds=time_offset)).isoformat()
    
    return {
        "record_id": f"rec_{counter:08d}",
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "price_rub": price,
        "stock": stock,
        "seller_rating": rating,
        "timestamp": timestamp,
        "marketplace": random.choice(MARKETPLACES),
        "session_id": f"sess_{random.randint(10000, 99999)}",
        "user_region": random.choice(["Москва", "СПб", "Новосибирск", "Екатеринбург", "Казань"]),
        "data_quality": "normal",
        "_metadata": {
            "generation_time": datetime.now().isoformat(),
            "version": "1.0"
        }
    }

# ------------------------------------------------------------
# 4. ГЕНЕРАТОР АНОМАЛИЙ (можно закомментировать)
# ------------------------------------------------------------
def inject_anomaly(record: Dict[str, Any], anomaly_type: str) -> Union[Dict[str, Any], str, bytes]:
    """Внедряет аномалию в запись"""
    
    if anomaly_type == "missing_fields":
        # Удаляем случайные поля (кроме record_id и data_quality)
        if record:
            fields = list(record.keys())
            fields = [f for f in fields if f not in ["record_id", "data_quality"]]
            to_remove = random.sample(fields, min(3, len(fields)))
            for field in to_remove:
                record.pop(field, None)
            record["data_quality"] = "missing_fields"
        return record
    
    elif anomaly_type == "extra_quotes":
        # Лишние кавычки
        if record:
            for key in ["product_name", "category", "user_region"]:
                if key in record and record[key]:
                    if random.random() < 0.5:
                        record[key] = f'"{record[key]}"'
                    else:
                        record[key] = f'{record[key]}""'
            record["data_quality"] = "extra_quotes"
        return record
    
    elif anomaly_type == "negative_price":
        if record:
            record["price_rub"] = -abs(record["price_rub"])
            record["data_quality"] = "negative_price"
        return record
    
    elif anomaly_type == "huge_price":
        if record:
            record["price_rub"] = record["price_rub"] * random.randint(100, 10000)
            record["data_quality"] = "huge_price"
        return record
    
    elif anomaly_type == "future_timestamp":
        if record:
            days_in_future = random.randint(1, 365 * 10)
            future_date = datetime.now() + timedelta(days=days_in_future)
            record["timestamp"] = future_date.isoformat()
            record["data_quality"] = "future_timestamp"
        return record
    
    elif anomaly_type == "malformed_json":
        # Испорченный JSON
        malformed = '{"id": "bad_json", "product": "Тест", "price": 100,}'  # Лишняя запятая
        if random.random() < 0.3:
            malformed = '{id: "no_quotes", product: "Тест"}'  # Нет кавычек у ключей
        return malformed
    
    elif anomaly_type == "null_values":
        if record:
            for key in record:
                if key != "data_quality":
                    record[key] = None
            record["data_quality"] = "null_values"
        return record
    
    elif anomaly_type == "sql_injection":
        if record:
            injections = [
                "'; DROP TABLE products; --",
                "' OR '1'='1",
                "'; SELECT * FROM users; --",
                "<script>alert('xss')</script>"
            ]
            record["product_name"] = f"Товар{random.choice(injections)}"
            record["data_quality"] = "sql_injection"
        return record
    
    elif anomaly_type == "binary_garbage":
        # Бинарные данные
        garbage = bytes([random.randint(0, 255) for _ in range(50)])
        return garbage
    
    elif anomaly_type == "encoding_problem":
        if record:
            # Добавляем невалидные UTF-8 символы
            record["product_name"] = "Товар" + "".join([
                chr(random.randint(0xD800, 0xDFFF))  # Невалидные суррогаты
                if random.random() < 0.3 else 
                chr(random.randint(0x00, 0x1F))      # Управляющие символы
                for _ in range(5)
            ])
            record["data_quality"] = "encoding_problem"
        return record
    
    elif anomaly_type == "empty_record":
        return {}
    
    return record

# ------------------------------------------------------------
# 5. ОСНОВНОЙ ГЕНЕРАТОР
# ------------------------------------------------------------
class UnifiedDataGenerator:
    """Генератор: нормальные данные + аномалии"""
    
    def __init__(self, anomaly_profile=None):
        self.counter = 0
        self.last_good_record = None
        self.anomaly_log = []
        self.anomaly_profile = anomaly_profile or ANOMALY_PROFILE
        
    def _choose_anomaly_type(self) -> str:
        """Выбирает тип аномалии согласно профилю"""
        r = random.random()
        cumulative = 0.0
        
        for anomaly_type, probability in self.anomaly_profile.items():
            cumulative += probability
            if r <= cumulative:
                return anomaly_type
        return "normal"
    
    async def generate_record(self) -> Optional[Union[Dict[str, Any], str, bytes]]:
        """Генерирует одну запись (возможно с аномалией)"""
        
        self.counter += 1
        
        # Каждые 1000 строк - пропуск данных (имитация сбоя)
        if self.counter % 1000 == 0:
            self.anomaly_log.append({
                "counter": self.counter,
                "type": "data_gap",
                "timestamp": datetime.now().isoformat()
            })
            return None
        
        # Выбираем тип записи
        anomaly_type = self._choose_anomaly_type()
        
        # Генерируем основу
        if anomaly_type == "normal" or anomaly_type in ["duplicate", "empty_record"]:
            base_record = generate_healthy_record(self.counter)
        else:
            base_record = generate_healthy_record(self.counter)
        
        # Обработка дубликатов
        if anomaly_type == "duplicate":
            if self.last_good_record:
                record = self.last_good_record.copy()
                record["record_id"] = f"{record['record_id']}_dup"
                record["data_quality"] = "duplicate"
                return record
            else:
                anomaly_type = "normal"
        
        # Внедряем аномалию (если не normal)
        if anomaly_type != "normal":
            record = inject_anomaly(base_record, anomaly_type)
            
            # Логируем аномалию
            self.anomaly_log.append({
                "counter": self.counter,
                "type": anomaly_type,
                "record_id": base_record.get("record_id", "unknown"),
                "timestamp": datetime.now().isoformat()
            })
        else:
            record = base_record
            self.last_good_record = record.copy()
        
        return record
    
    async def generate_stream(self, records_to_generate: int = 10000):
        """Генерирует поток записей"""
        generated = 0
        
        while generated < records_to_generate:
            # Реалистичная задержка (в среднем 20 записей/сек)
            delay = random.expovariate(50.0)
            await asyncio.sleep(delay)
            
            record = await self.generate_record()
            
            if record is not None:
                yield record
                generated += 1
            
            # 0.2% шанс на длинную паузу (сбой сети)
            if random.random() < 0.002:
                long_delay = random.uniform(2.0, 10.0)
                await asyncio.sleep(long_delay)
    
    def save_to_files(self, filename_base: str = "data"):
        """Сохраняет сгенерированные данные в файлы"""
        # Лог аномалий
        with open(f"{filename_base}_anomalies.jsonl", "w", encoding="utf-8") as f:
            for anomaly in self.anomaly_log:
                f.write(json.dumps(anomaly, ensure_ascii=False) + "\n")
        
        print(f"Сохранено {len(self.anomaly_log)} записей об аномалиях")

# ------------------------------------------------------------
# 6. ПРОСТЫЕ ХЕЛПЕРЫ ДЛЯ ЭКСПОРТА
# ------------------------------------------------------------
async def export_to_jsonl(filename: str, count: int = 1000):
    """Экспорт в JSON Lines файл"""
    generator = UnifiedDataGenerator()
    
    with open(filename, "w", encoding="utf-8") as f:
        async for record in generator.generate_stream(count):
            if isinstance(record, dict):
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
            elif isinstance(record, str):
                f.write(record + "\n")
    
    print(f"Экспортировано {count} записей в {filename}")

async def export_to_parquet(filename: str, count: int = 1000):
    """Экспорт в Parquet (если установлен pyarrow)"""
    try:
        import pandas as pd
        import pyarrow.parquet as pq
        
        records = []
        generator = UnifiedDataGenerator()
        
        async for record in generator.generate_stream(count):
            if isinstance(record, dict):
                records.append(record)
        
        df = pd.DataFrame(records)
        df.to_parquet(filename, engine='pyarrow')
        print(f"Экспортировано {len(df)} записей в {filename}")
        
    except ImportError:
        print("Для экспорта в Parquet установите: pip install pandas pyarrow")

# ------------------------------------------------------------
# 7. ПРОСТОЙ CLI
# ------------------------------------------------------------
async def main_cli():
    """Простой интерфейс командной строки"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Генератор данных маркетплейса")
    parser.add_argument("--count", type=int, default=1000, help="Количество записей")
    parser.add_argument("--format", choices=["jsonl", "parquet", "csv"], default="jsonl", help="Формат вывода")
    parser.add_argument("--output", type=str, default="data", help="Имя выходного файла")
    parser.add_argument("--no-anomalies", action="store_true", help="Без аномалий")
    
    args = parser.parse_args()
    
    # Если без аномалий - меняем профиль
    anomaly_profile = None
    if args.no_anomalies:
        anomaly_profile = {"normal": 1.0}
    
    generator = UnifiedDataGenerator(anomaly_profile)
    
    if args.format == "jsonl":
        filename = f"{args.output}.jsonl"
        with open(filename, "w", encoding="utf-8") as f:
            generated = 0
            async for record in generator.generate_stream(args.count):
                if isinstance(record, dict):
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                elif isinstance(record, str):
                    f.write(record + "\n")
                generated += 1
                if generated % 1000 == 0:
                    print(f"Генерирую... {genered}/{args.count}")
        
        print(f"✅ Готово: {filename}")
    
    elif args.format == "csv":
        filename = f"{args.output}.csv"
        import csv
        
        with open(filename, "w", encoding="utf-8", newline="") as f:
            writer = None
            generated = 0
            
            async for record in generator.generate_stream(args.count):
                if isinstance(record, dict):
                    if writer is None:
                        writer = csv.DictWriter(f, fieldnames=record.keys())
                        writer.writeheader()
                    writer.writerow({k: str(v) for k, v in record.items()})
                
                generated += 1
                if generated % 1000 == 0:
                    print(f"Генерирую... {genered}/{args.count}")
        
        print(f"✅ Готово: {filename}")
    
    elif args.format == "parquet":
        filename = f"{args.output}.parquet"
        await export_to_parquet(filename, args.count)
    
    # Сохраняем логи аномалий
    if generator.anomaly_log:
        generator.save_to_files(args.output)

# ------------------------------------------------------------
# 8. ЗАПУСК
# ------------------------------------------------------------
if __name__ == "__main__":
    # Простой пример использования
    import sys
    
    if len(sys.argv) > 1:
        asyncio.run(main_cli())
    else:
        # Если запустили без аргументов - пример
        async def example():
            print("🎲 Генерация 10 записей для примера...")
            generator = UnifiedDataGenerator()
            
            async for record in generator.generate_stream(10):
                if isinstance(record, dict):
                    print(json.dumps(record, indent=2, ensure_ascii=False))
                elif record is not None:
                    print(f"[Аномалия]: {type(record).__name__}")
            
            print("\n📝 Использование:")
            print("  python generator.py --count 10000 --format jsonl --output my_data")
            print("  python generator.py --count 5000 --format csv --no-anomalies")
        
        asyncio.run(example())
