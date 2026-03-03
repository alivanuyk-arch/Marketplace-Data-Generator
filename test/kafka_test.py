import asyncio
import sys
import time
from generator import MarketplaceDataGenerator
from Marketplace_Producer import KafkaMarketplaceProducer

async def main():
    print("=" * 70)
    print("🧪 ТЕСТИРОВАНИЕ KAFKA ПРОДЮСЕРА")
    print("=" * 70)
    
    # 1. Создаем генератор данных
    print("\n1️⃣  СОЗДАНИЕ ГЕНЕРАТОРА ДАННЫХ")
    print("   Создаю MarketplaceDataGenerator...")
    generator = MarketplaceDataGenerator()
    print(f"   ✅ Генератор создан")
    print(f"   📊 Категории: {', '.join(generator.categories)}")
    
    # 2. Создаем продюсер Kafka
    print("\n2️⃣  ПОДКЛЮЧЕНИЕ К KAFKA")
    print("   Пытаюсь подключиться к kafka:9092...")
    
    # Проверяем, запущен ли Docker и Kafka
    import socket
    def check_port(host, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    
    # Пробуем разные варианты подключения
    kafka_servers = None
    for server in ['localhost:9092', 'kafka:9092', '127.0.0.1:9092']:
        host, port = server.split(':')
        if check_port(host, int(port)):
            kafka_servers = server
            print(f"   ✅ Kafka доступна на {server}")
            break
    
    if not kafka_servers:
        print("   ❌ Kafka не доступна ни по одному адресу")
        print("\n   🔧 УСТРАНЕНИЕ ПРОБЛЕМ:")
        print("   1. Убедитесь, что Docker запущен: docker ps")
        print("   2. Запустите Kafka: docker-compose up -d kafka")
        print("   3. Подождите 20 секунд: Start-Sleep -Seconds 20")
        print("   4. Проверьте статус: docker ps | findstr kafka")
        return
    
    try:
        producer = KafkaMarketplaceProducer(kafka_servers)
        print("   ✅ Продюсер создан")
    except Exception as e:
        print(f"   ❌ Не удалось создать продюсер: {e}")
        return
    
    # 3. Проверяем, создан ли топик
    print("\n3️⃣  ПРОВЕРКА ТОПИКА")
    try:
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=kafka_servers)
        topics = admin.list_topics()
        if 'marketplace-data' in topics:
            print("   ✅ Топик 'marketplace-data' существует")
        else:
            print("   ⚠️ Топик 'marketplace-data' не найден")
            print("   🔧 Создайте его: docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic marketplace-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1")
    except Exception as e:
        print(f"   ⚠️ Не удалось проверить топик: {e}")
    
    # 4. Отправляем записи
    print("\n4️⃣  ОТПРАВКА ДАННЫХ")
    test_count = 20  # Небольшое количество для теста
    print(f"   Начинаю отправку {test_count} записей для теста...")
    
    try:
        start_time = time.time()
        await producer.stream_to_kafka(
            generator=generator,
            topic='marketplace-data',
            total_records=test_count
        )
        elapsed = time.time() - start_time
        print(f"\n   ⏱️  Время отправки: {elapsed:.2f} сек")
        
        # 5. Проверяем результат
        print("\n5️⃣  ПРОВЕРКА РЕЗУЛЬТАТА")
        print(f"   ✅ Отправлено записей: {producer.sent_count}")
        print(f"   ❌ Ошибок: {producer.error_count}")
        
        if producer.sent_count == test_count:
            print("   ✅ ВСЕ ЗАПИСИ УСПЕШНО ОТПРАВЛЕНЫ!")
        else:
            print(f"   ⚠️ Отправлено только {producer.sent_count} из {test_count}")
            
    except KeyboardInterrupt:
        print("\n   ⏹️  Остановлено пользователем")
    except Exception as e:
        print(f"\n   ❌ Неожиданная ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # 6. Завершаем работу
        print("\n6️⃣  ЗАВЕРШЕНИЕ")
        producer.close()
        print("   ✅ Тест завершен")
        print("\n💡 Теперь можно проверить данные в Kafka:")
        print("   docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \\")
        print("     --bootstrap-server localhost:9092 \\")
        print("     --topic marketplace-data \\")
        print("     --from-beginning --max-messages 5")
        print("=" * 70)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🚫 Программа прервана пользователем")
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()