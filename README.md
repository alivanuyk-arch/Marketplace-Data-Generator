🛒 Marketplace Data Generator
Генератор потоковых данных маркетплейса с отправкой в Kafka и обработкой в Spark + ClickHouse.

🎯 История создания (зачем всё это)
Всё началось с желания освоить новые технологии работы с потоковыми данными:

🔍 Что искал
Реалистичные потоковые данные для тренировки

Датасеты с разными сценариями (нормальные данные + аномалии)

Возможность тестировать ETL-пайплайны

🤔 С чем столкнулся
Доступ к данным — либо капчи, либо сложная юридическая волокита

"Серый" вход — неофициальные API, которые могут закрыться в любой момент

Статичные датасеты — CSV-файлы, а хотелось живой поток

Отсутствие аномалий — все датасеты "стерильные", без ошибок

💡 Решение
"А что, я же хотел стать профи, но я программист — сделаю сам! "

🏗️ Архитектура
text
[Генератор] → [Kafka] → [Spark] → [ClickHouse]
     ↑           ↑          ↑            ↑
  Данные      Топик      Очистка      Хранилище
  с мусором   "pipe"     Агрегация    (с характером)
Компоненты:
Генератор — создаёт поток данных с 13 типами аномалий

Kafka — транспортная шина (просто "труба")

Spark — разделение потоков, очистка, агрегация

ClickHouse — аналитическое хранилище (прошёл огонь и воду)

🚀 Быстрый старт
bash
# 1. Клонировать репозиторий
git clone https://github.com/yourname/marketplace-generator
cd marketplace-generator

# 2. Запустить инфраструктуру
docker-compose up -d

# 3. Запустить генератор
python main.py

# 4. Наблюдать магию
docker logs spark --tail 50
⚡ Производительность
Реальная скорость работы (проверено на практике)
Генератор: ~12 записей/сек (оптимально для учебного проекта)

Kafka: принимает поток без задержек

Spark Streaming: обрабатывает в реальном времени

ClickHouse: успешно сохраняет ~700-1000 записей в минуту

Почему именно такая скорость?
Генератор намеренно создаёт "рваный" поток для всестороннего тестирования:

python
# Механизмы создания нестабильного потока:
delay = random.expovariate(50.0)  # Случайные задержки
await asyncio.sleep(delay)        # Базовая пауза

# 0.2% шанс на ДЛИННУЮ паузу (2-10 сек)
if random.random() < 0.002:       # Имитация сетевых сбоев
    long_delay = random.uniform(2.0, 10.0)
    await asyncio.sleep(long_delay)

# Каждые 1000 записей - пропуск (возвращает None)
if self.counter % 1000 == 0:      # Проверка устойчивости
    return None
Зачем такие сложности?
✅ Тестирование устойчивости Kafka при нестабильном потоке

✅ Проверка таймаутов и реконнектов в Spark Streaming

✅ Эмуляция реальных условий с переменной нагрузкой

✅ Выявление узких мест в пайплайне

📊 Типы аномалий (13 видов)
Тип	Описание	Частота
missing_fields	Пропущенные поля	3%
extra_quotes	Лишние кавычки в строковых полях	2%
duplicate	Дубликат последней нормальной записи	2%
encoding_problem	Невалидные UTF-8 символы	1%
future_timestamp	Дата из будущего (до 10 лет)	1%
negative_price	Отрицательная цена	1%
null_values	Все поля = None	1%
sql_injection	SQL-инъекции в строковых полях	0.5%
huge_price	Цена в 100-10000 раз выше	0.5%
malformed_json	Битый JSON (возвращает строку)	1.5%
binary_garbage	Бинарный мусор (возвращает bytes)	0.3%
empty_record	Пустой словарь	0.5%
data_gap	Пропуск записи (каждые 1000)	0.1%
🏆 Главные победы
🥇 Permission denied в ClickHouse (2 дня ада)
Проблема:

text
std::exception. Code: 1001, filesystem error: in rename: Permission denied
Решение:

yaml
# Замена bind mount на volume (это был ключ!)
volumes:
  - clickhouse_data:/var/lib/clickhouse  # ✅ РАБОТАЕТ!
  # вместо ./clickhouse-data:/var/lib/clickhouse  # ❌ НЕ РАБОТАЕТ
Урок: Docker на Windows + bind mount + ClickHouse = гремучая смесь

🥈 Kafka — создание топика
yaml
# Добавить в environment Kafka:
KAFKA_CREATE_TOPICS: "marketplace-data:3:1"  # 3 партиции
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
📋 Полезные команды
Проверка данных в ClickHouse
bash
# Сколько всего записей?
docker exec clickhouse clickhouse-client --password sparkpass --query "SELECT COUNT(*) FROM marketplace.anomalies"

# Типы аномалий
docker exec clickhouse clickhouse-client --password sparkpass --query "SELECT anomaly_type, COUNT(*) FROM marketplace.anomalies GROUP BY anomaly_type ORDER BY COUNT(*) DESC" --format Pretty

# Последние записи
docker exec clickhouse clickhouse-client --password sparkpass --query "SELECT * FROM marketplace.anomalies ORDER BY timestamp DESC LIMIT 5" --format Pretty

# Минутная статистика
docker exec clickhouse clickhouse-client --password sparkpass --query "SELECT * FROM marketplace.minute_stats ORDER BY event_time DESC LIMIT 5" --format Pretty
Мониторинг скорости
bash
# Скрипт для наблюдения в реальном времени
while($true) {
    $time = Get-Date -Format "HH:mm:ss"
    $count = docker exec clickhouse clickhouse-client --password sparkpass --query "SELECT COUNT(*) FROM marketplace.anomalies" 2>$null
    Write-Host "[$time] 📊 anomalies: $count"
    Start-Sleep -Seconds 5
}
Логи компонентов
bash
# Генератор
docker logs generator --tail 50

# Spark
docker logs spark --tail 50

# Kafka
docker logs kafka --tail 50

# ClickHouse
docker logs clickhouse --tail 50
🔧 Структура проекта
text
Marketplace-Data-Generator/
├── 📄 docker-compose.yml      # Оркестрация всех сервисов
├── 📄 Dockerfile              # Для генератора
├── 📄 Dockerfile.spark        # Для Spark (с Python)
├── 📄 generator.py            # Полный генератор данных
├── 📄 main.py                 # Точка входа (генератор + продюсер)
├── 📄 Marketplace_Producer.py # Kafka продюсер
├── 📄 requirements.txt        # Зависимости Python
├── 📄 README.md               # Этот файл
├── 📁 spark_apps/             
│   └── 📄 spark_processor.py  # Обработчик Spark
└── 📁 clickhouse-data/        # Данные (игнорируется git)
🛠 Технологии
Python 3.11+ — генератор данных

Apache Kafka 4.0.0 — транспортная шина

Apache Spark 3.5.5 — потоковая обработка

ClickHouse latest — аналитическое хранилище

Docker + Docker Compose — контейнеризация

📈 Результаты тестирования
За время разработки (2 дня непрерывной борьбы):

✅ ~2400+ записей в таблице anomalies

✅ 13 типов аномалий успешно детектируются

✅ 9 типов уже в базе (остальные добиваем)

✅ Минутная статистика обновляется

✅ Система стабильна под нагрузкой ~12 записей/сек

💡 Выводы
ClickHouse — мощный инструмент для аналитики, но:

🔴 Требует правильной настройки в Docker

🔴 Не прощает ошибок с файловой системой

✅ После правильной настройки работает как часы

2 дня мучений — бесценный опыт! 💪

📄 Лицензия
MIT — берите, пробуйте, мучайтесь, прокачивайтесь!