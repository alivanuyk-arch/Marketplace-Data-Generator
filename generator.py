import random
import json
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Union, Callable
import struct
from aioitertools import enumerate as aenumerate

# ------------------------------------------------------------
# 1. ПОЛНЫЙ КОНФИГ (все параметры)
# ------------------------------------------------------------

PRODUCT_CATALOG = [
    {
        "id": "PH_001",
        "name": "Смартфон",
        "category": "Электроника",
        "price_range": (3000, 10000),
        "stock_range": (5, 50)
    },
    {
        "id": "TB_001", 
        "name": "Планшет",
        "category": "Электроника",
        "price_range": (5000, 15000),
        "stock_range": (3, 30)
    }
]

MARKETPLACES = ["Wildberries", "Ozon", "Яндекс.Маркет", "СберМегаМаркет"]

# ВСЕ аномалии как в оригинале
ANOMALY_CONFIG = {
    "normal": (0.85, "generate_normal"),
    "missing_fields": (0.03, "anomaly_missing_fields"),
    "extra_quotes": (0.02, "anomaly_extra_quotes"),
    "negative_price": (0.01, "anomaly_negative_price"),
    "huge_price": (0.005, "anomaly_huge_price"),
    "future_timestamp": (0.01, "anomaly_future_timestamp"),
    "malformed_json": (0.015, "anomaly_malformed_json"),
    "duplicate": (0.02, "anomaly_duplicate"),
    "null_values": (0.01, "anomaly_null_values"),
    "sql_injection": (0.005, "anomaly_sql_injection"),
    "binary_garbage": (0.003, "anomaly_binary_garbage"),
    "encoding_problem": (0.01, "anomaly_encoding_problem"),
    "empty_record": (0.005, "anomaly_empty_record")
}

# ------------------------------------------------------------
# 2. БАЗОВЫЕ ГЕНЕРАТОРЫ (с полной логикой)
# ------------------------------------------------------------

def _generate_price_with_time_logic(product: Dict) -> int:
    """Цена с учетом времени суток (как в оригинале)"""
    min_price, max_price = product["price_range"]
    base_price = random.randint(min_price, max_price)
    
    hour = datetime.now().hour
    if 0 <= hour < 6:      # Ночью дешевле
        price_mod = random.uniform(0.95, 1.0)
    elif 20 <= hour <= 23: # Вечером дороже
        price_mod = random.uniform(1.0, 1.05)
    else:                  # Днем нормально
        price_mod = random.uniform(0.98, 1.02)
    
    return int(base_price * price_mod)

def _generate_stock_with_popularity(product: Dict) -> int:
    """Остаток с учетом популярности товара"""
    min_stock, max_stock = product["stock_range"]
    stock = random.randint(min_stock, max_stock)
    
    # Популярные товары быстрее заканчиваются
    if product["category"] == "Электроника":
        stock = max(1, stock - random.randint(0, 10))
    
    return stock

def _generate_seller_rating() -> float:
    """Рейтинг продавца (10% с низким рейтингом)"""
    if random.random() < 0.1:  # 10% продавцов с низким рейтингом
        return round(random.uniform(2.0, 3.5), 1)
    return round(random.uniform(4.0, 5.0), 1)

async def generate_product_component() -> Dict[str, Any]:
    """Генерация данных товара (async с задержкой)"""
    await asyncio.sleep(0.001)  # Имитация async-операции
    
    product = random.choice(PRODUCT_CATALOG)
    
    return {
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "price_rub": _generate_price_with_time_logic(product),
        "stock": _generate_stock_with_popularity(product)
    }

async def generate_marketplace_component() -> Dict[str, Any]:
    """Генерация данных площадки"""
    await asyncio.sleep(0.001)
    
    return {
        "marketplace": random.choice(MARKETPLACES),
        "seller_rating": _generate_seller_rating(),
        "user_region": random.choice(["Москва", "СПб", "Новосибирск"]),
        "session_id": f"sess_{random.randint(10000, 99999)}"
    }

def generate_base_metadata(counter: int) -> Dict[str, Any]:
    """Базовая метаинформация записи"""
    # Временная метка с "дрейфом" ±2 минуты
    time_offset = random.randint(-120, 120)
    timestamp = (datetime.now() + timedelta(seconds=time_offset)).isoformat()
    
    return {
        "record_id": f"rec_{counter:08d}",
        "timestamp": timestamp,
        "_metadata": {
            "generation_time": datetime.now().isoformat(),
            "version": "1.0"
        }
    }

# ------------------------------------------------------------
# 3. ГЕНЕРАТОРЫ АНОМАЛИЙ (ВСЕ как в оригинале)
# ------------------------------------------------------------

async def generate_normal(counter: int, last_normal_record: Dict = None) -> Dict[str, Any]:
    """Нормальная запись (полная логика)"""
    record = generate_base_metadata(counter)
    record.update(await generate_product_component())
    record.update(await generate_marketplace_component())
    record["data_quality"] = "normal"
    return record

async def anomaly_missing_fields(counter: int, last_normal_record: Dict) -> Dict[str, Any]:
    """Пропущенные поля (удаляем 1-3 случайных поля)"""
    record = await generate_normal(counter, last_normal_record)
    
    fields = [k for k in record.keys() 
              if k not in ["record_id", "data_quality", "_metadata"]]
    
    if fields:
        to_remove = random.sample(fields, min(3, len(fields)))
        for field in to_remove:
            record.pop(field, None)
    
    record["data_quality"] = "missing_fields"
    return record

async def anomaly_extra_quotes(counter: int, last_normal_record: Dict) -> Dict[str, Any]:
    """Лишние кавычки в строковых полях"""
    record = await generate_normal(counter, last_normal_record)
    
    for key in ["product_name", "category", "user_region"]:
        if key in record and record[key]:
            if random.random() < 0.5:
                record[key] = f'"{record[key]}"'  # Кавычки вокруг
            else:
                record[key] = f'{record[key]}""'  # Двойные кавычки в конце
    
    record["data_quality"] = "extra_quotes"
    return record

async def anomaly_negative_price(counter: int, last_normal_record: Dict) -> Dict[str, Any]:
    """Отрицательная цена"""
    record = await generate_normal(counter, last_normal_record)
    
    if "price_rub" in record:
        record["price_rub"] = -abs(record["price_rub"])
    
    record["data_quality"] = "negative_price"
    return record

async def anomaly_huge_price(counter: int, last_normal_record: Dict) -> Dict[str, Any]:
    """Цена в 100-10000 раз выше"""
    record = await generate_normal(counter, last_normal_record)
    
    if "price_rub" in record:
        record["price_rub"] = record["price_rub"] * random.randint(100, 10000)
    
    record["data_quality"] = "huge_price"
    return record

async def anomaly_future_timestamp(counter: int, last_normal_record: Dict) -> Dict[str, Any]:
    """Дата из будущего"""
    record = await generate_normal(counter, last_normal_record)
    
    days_in_future = random.randint(1, 365 * 10)  # До 10 лет в будущее
    future_date = datetime.now() + timedelta(days=days_in_future)
    record["timestamp"] = future_date.isoformat()
    
    record["data_quality"] = "future_timestamp"
    return record

async def anomaly_malformed_json(counter: int, last_normal_record: Dict) -> str:
    """Битый JSON (возвращает строку!)"""
    errors = [
        '{"id": "bad_json", "product": "Тест", "price": 100,}',  # Лишняя запятая
        '{id: "no_quotes", product: "Тест"}',  # Нет кавычек у ключей
        '{"id": "test" "price": 100}',  # Нет запятой
        '{"id": "test"'  # Незакрытый JSON
    ]
    return random.choice(errors)

async def anomaly_duplicate(counter: int, last_normal_record: Dict) -> Optional[Dict[str, Any]]:
    """Дубликат последней нормальной записи"""
    if last_normal_record:
        record = last_normal_record.copy()
        record["record_id"] = f"{record['record_id']}_dup"
        record["data_quality"] = "duplicate"
        return record
    return None  # Если нет последней нормальной

async def anomaly_null_values(counter: int, last_normal_record: Dict) -> Dict[str, Any]:
    """Все поля = None"""
    record = generate_base_metadata(counter)
    
    # Все основные поля = None
    fields = ["product_id", "product_name", "category", "price_rub", 
              "stock", "seller_rating", "marketplace", "user_region"]
    
    for field in fields:
        record[field] = None
    
    record["data_quality"] = "null_values"
    return record

async def anomaly_sql_injection(counter: int, last_normal_record: Dict) -> Dict[str, Any]:
    """SQL-инъекция в строковые поля"""
    record = await generate_normal(counter, last_normal_record)
    
    injections = [
        "'; DROP TABLE products; --",
        "' OR '1'='1",
        "'; SELECT * FROM users; --",
        "<script>alert('xss')</script>"
    ]
    
    # Вставляем в случайное строковое поле
    text_fields = [k for k, v in record.items() 
                  if isinstance(v, str) and k not in ["record_id", "data_quality", "_metadata"]]
    
    if text_fields:
        field = random.choice(text_fields)
        record[field] += random.choice(injections)
    
    record["data_quality"] = "sql_injection"
    return record

async def anomaly_binary_garbage(counter: int, last_normal_record: Dict) -> bytes:
    """Бинарный мусор (возвращает bytes!)"""
    # Генерируем случайные байты
    return bytes([random.randint(0, 255) for _ in range(50)])

async def anomaly_encoding_problem(counter: int, last_normal_record: Dict) -> Dict[str, Any]:
    """Проблемы с кодировкой UTF-8"""
    record = await generate_normal(counter, last_normal_record)
    
    # Добавляем невалидные UTF-8 символы
    problem_chars = []
    for _ in range(5):
        if random.random() < 0.3:
            # Невалидные суррогаты
            problem_chars.append(chr(random.randint(0xD800, 0xDFFF)))
        else:
            # Управляющие символы
            problem_chars.append(chr(random.randint(0x00, 0x1F)))
    
    # Вставляем в строковое поле
    if "product_name" in record:
        record["product_name"] = "Товар" + "".join(problem_chars)
    
    record["data_quality"] = "encoding_problem"
    return record

async def anomaly_empty_record(counter: int, last_normal_record: Dict) -> Dict[str, Any]:
    """Пустая запись"""
    return {}

# ------------------------------------------------------------
# 4. ДИСПЕТЧЕР С ПОЛНОЙ ЛОГИКОЙ (включая сбои)
# ------------------------------------------------------------

class MarketplaceDataGenerator:
    """Полнофункциональный генератор (как оригинал, но с чистой архитектурой)"""
    
    def __init__(self, anomaly_config=None):
        self.counter = 0
        self.last_normal_record = None
        self.anomaly_log = []
        self.config = anomaly_config or ANOMALY_CONFIG
        
        # Регистрируем ВСЕ генераторы
        self.generators = {
            "generate_normal": generate_normal,
            "anomaly_missing_fields": anomaly_missing_fields,
            "anomaly_extra_quotes": anomaly_extra_quotes,
            "anomaly_negative_price": anomaly_negative_price,
            "anomaly_huge_price": anomaly_huge_price,
            "anomaly_future_timestamp": anomaly_future_timestamp,
            "anomaly_malformed_json": anomaly_malformed_json,
            "anomaly_duplicate": anomaly_duplicate,
            "anomaly_null_values": anomaly_null_values,
            "anomaly_sql_injection": anomaly_sql_injection,
            "anomaly_binary_garbage": anomaly_binary_garbage,
            "anomaly_encoding_problem": anomaly_encoding_problem,
            "anomaly_empty_record": anomaly_empty_record
        }
    
    def _choose_generator(self) -> tuple:
        """Выбор типа записи по вероятностям"""
        r = random.random()
        cumulative = 0.0
        
        for anomaly_type, (prob, generator_name) in self.config.items():
            cumulative += prob
            if r <= cumulative:
                return anomaly_type, self.generators[generator_name]
        
        return "normal", generate_normal
    
    async def generate_record(self) -> Optional[Union[Dict[str, Any], str, bytes]]:
        """Генерация одной записи с имитацией сбоев"""
        self.counter += 1
        
        # Имитация сбоя (каждые 1000 записей) - как в оригинале
        if self.counter % 1000 == 0:
            self.anomaly_log.append({
                "counter": self.counter,
                "type": "data_gap",
                "timestamp": datetime.now().isoformat()
            })
            return None
        
        # Выбор типа записи
        anomaly_type, generator_func = self._choose_generator()
        
        # Генерация
        try:
            record = await generator_func(self.counter, self.last_normal_record)
            
            # Для дубликата может вернуться None
            if record is None and anomaly_type == "duplicate":
                # Если нет последней нормальной - генерируем нормальную
                record = await generate_normal(self.counter, self.last_normal_record)
                anomaly_type = "normal"
            
            # Сохраняем последнюю нормальную для дубликатов
            if anomaly_type == "normal" and isinstance(record, dict):
                self.last_normal_record = record.copy()
            
            # Логируем аномалии (кроме normal)
            if anomaly_type != "normal" and record is not None:
                record_id = record.get("record_id", "unknown") if isinstance(record, dict) else "non_dict"
                self.anomaly_log.append({
                    "counter": self.counter,
                    "type": anomaly_type,
                    "record_id": record_id,
                    "timestamp": datetime.now().isoformat()
                })
            
            return record
            
        except Exception as e:
            # Логируем ошибки генерации
            self.anomaly_log.append({
                "counter": self.counter,
                "type": "generation_error",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
            return None
    
    async def generate_stream(self, records_to_generate: int = 10000):
        """Поток данных с реалистичными задержками и сбоями"""
        generated = 0
        
        while generated < records_to_generate:
            # Реалистичная задержка (в среднем 20 записей/сек) как в оригинале
            delay = random.expovariate(50.0)
            await asyncio.sleep(delay)
            
            # 0.2% шанс на длинную паузу (имитация сетевого сбоя)
            if random.random() < 0.002:
                long_delay = random.uniform(2.0, 10.0)
                await asyncio.sleep(long_delay)
            
            # Генерация записи
            record = await self.generate_record()
            
            if record is not None:
                yield record
                generated += 1  
    
    def save_anomaly_log(self, filename: str = "anomalies.jsonl"):
        """Сохранение лога аномалий"""
        with open(filename, "w", encoding="utf-8") as f:
            for anomaly in self.anomaly_log:
                f.write(json.dumps(anomaly, ensure_ascii=False) + "\n")
        print(f"Сохранено {len(self.anomaly_log)} записей об аномалиях в {filename}")

