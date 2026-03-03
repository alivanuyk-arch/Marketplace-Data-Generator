#вход

import asyncio
import argparse
import signal
import sys
import os
import time
from datetime import datetime
from Marketplace_Producer import KafkaMarketplaceProducer
from generator import MarketplaceDataGenerator


class DataPipeline:
    def __init__(self, bootstrap_servers=None, topic='marketplace-data'):
        # Приоритет: аргумент > переменная окружения > значение по умолчанию
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('KAFKA_SERVERS', 'kafka:9092')
        
        self.bootstrap_servers = bootstrap_servers
        self.generator = MarketplaceDataGenerator()
        self.producer = None
        self.topic = topic
        self.total_sent = 0
        self.start_time = None
        self.running = True
        
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
    
    def stop(self, *args):
        print("\n\n🛑 Получен сигнал остановки...")
        self.running = False
    
    async def connect_kafka_with_retry(self, max_retries=10):
        """Подключение к Kafka с повторными попытками"""
        for attempt in range(max_retries):
            try:
                print(f"🔧 Попытка {attempt+1}/{max_retries} подключения к Kafka...")
                self.producer = KafkaMarketplaceProducer(self.bootstrap_servers)
                print(f"✅ Подключено к Kafka на {self.bootstrap_servers}")
                return True
            except Exception as e:
                print(f"⚠️ Ошибка: {e}")
                if attempt < max_retries - 1:
                    wait_time = 3 * (attempt + 1)
                    print(f"⏳ Жду {wait_time} сек...")
                    await asyncio.sleep(wait_time)
        return False
    
    async def run(self, count: int = None):
        print("=" * 70)
        print("🚀 DATA PIPELINE: Генератор → Kafka")
        print("=" * 70)
        print(f"📤 Брокер: {self.bootstrap_servers}")
        print(f"📤 Топик: {self.topic}")
        print(f"📊 Режим: {'Бесконечный' if count is None else f'{count} записей'}")
        print("=" * 70)
        
        # Подключаемся к Kafka
        if not await self.connect_kafka_with_retry():
            print("❌ Не удалось подключиться к Kafka")
            return
        
        self.start_time = datetime.now()
        
        try:
            if count is None:
                while self.running:
                    async for record in self.generator.generate_stream(1000):
                        if not self.running:
                            break
                        await self._process_record(record)
                        
                        if self.total_sent % 100 == 0:
                            await self._print_stats()
            else:
                async for record in self.generator.generate_stream(count):
                    if not self.running:
                        break
                    await self._process_record(record)
                    
                    if self.total_sent % 100 == 0:
                        await self._print_stats()
        
        finally:
            await self._shutdown()
    
    async def _process_record(self, record):
        try:
            key = None
            if isinstance(record, dict):
                key = record.get('marketplace') or record.get('product_id')
            
            await self.producer.send_record(self.topic, record, key)
            self.total_sent += 1
            
        except Exception as e:
            print(f"⚠️ Ошибка обработки записи {self.total_sent}: {e}")
    
    async def _print_stats(self):
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.total_sent / elapsed if elapsed > 0 else 0
        print(f"📊 Статус: {self.total_sent} записей | {rate:.1f} записей/сек")
    
    async def _shutdown(self):
        print("\n🔄 Завершение работы...")
        if self.producer:
            self.producer.close()
        
        elapsed = (datetime.now() - self.start_time).total_seconds()
        print("\n" + "=" * 70)
        print("📈 ФИНАЛЬНАЯ СТАТИСТИКА")
        print(f"   Всего отправлено: {self.total_sent}")
        print(f"   Время работы: {elapsed:.1f} сек")
        print("=" * 70)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=None)
    parser.add_argument('--topic', type=str, default='marketplace-data')
    parser.add_argument('--bootstrap', type=str, default=None)
    
    args = parser.parse_args()
    
    pipeline = DataPipeline(
        bootstrap_servers=args.bootstrap,
        topic=args.topic
    )
    
    try:
        asyncio.run(pipeline.run(count=args.count))
    except KeyboardInterrupt:
        print("\n\n👋 Программа прервана")
    except Exception as e:
        print(f"\n💥 Ошибка: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())