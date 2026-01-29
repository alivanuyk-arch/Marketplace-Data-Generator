import asyncio
import sys
from gen import MarketplaceDataGenerator
from Marketplace_Producer import KafkaMarketplaceProducer

async def main():
    print("=" * 60)
    print("🧪 ТЕСТИРОВАНИЕ KAFKA ПРОДЮСЕРА")
    print("=" * 60)
    
    # 1. Создаем генератор данных
    print("\n1️⃣  СОЗДАНИЕ ГЕНЕРАТОРА ДАННЫХ")
    print("   Создаю MarketplaceDataGenerator...")
    generator = MarketplaceDataGenerator()
    print("   ✅ Генератор создан")
    
    # 2. Создаем продюсер Kafka
    print("\n2️⃣  ПОДКЛЮЧЕНИЕ К KAFKA")
    print("   Пытаюсь подключиться к localhost:9092...")
    
    try:
        producer = KafkaMarketplaceProducer('localhost:9092')
        print("   ✅ Подключение установлено")
    except Exception as e:
        print(f"   ❌ Не удалось подключиться к Kafka: {e}")
        print("\n   🔧 УСТРАНЕНИЕ ПРОБЛЕМ:")
        print("   1. Убедитесь, что Docker запущен")
        print("   2. Запустите Kafka: docker-compose up -d")
        print("   3. Подождите 30 секунд для запуска Kafka")
        return
    
    # 3. Отправляем небольшое количество записей
    print("\n3️⃣  ОТПРАВКА ДАННЫХ")
    print("   Начинаю отправку 20 записей для теста...")
    
    try:
        await producer.stream_to_kafka(
            generator=generator,
            topic='marketplace-data',
            total_records=1000  # Начнем с малого
        )
    except KeyboardInterrupt:
        print("\n   ⏹️  Остановлено пользователем")
    except Exception as e:
        print(f"\n   ❌ Неожиданная ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 4. Завершаем работу
        print("\n4️⃣  ЗАВЕРШЕНИЕ")
        producer.close()
        print("   ✅ Тест завершен")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🚫 Программа прервана пользователем")
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")