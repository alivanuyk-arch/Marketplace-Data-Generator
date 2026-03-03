
#Marketplace_Producer.py
import asyncio
from kafka import KafkaProducer
import json
from datetime import datetime
import traceback
import sys

class KafkaMarketplaceProducer:
    """Отправляет данные из генератора в Kafka"""
    
    def __init__(self, bootstrap_servers='localhost:9092'):
        print(f"🔧 Инициализация продюсера Kafka: {bootstrap_servers}")
        try:
            # Создаем продюсер
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                max_block_ms=10000  # Увеличиваем таймаут
            )
            print(f"✅ Продюсер Kafka создан успешно")
        except Exception as e:
            print(f"❌ ОШИБКА создания продюсера Kafka: {e}")
            print(f"   Убедитесь, что Kafka запущена на {bootstrap_servers}")
            raise
        
        self.sent_count = 0
        self.error_count = 0
        self.start_time = None
    
    def _clean_record(self, record):
        """Очищает запись от невалидных UTF-8 символов"""
        if not isinstance(record, dict):
            return record
        
        cleaned = {}
        for key, value in record.items():
            if isinstance(value, str):
                # Убираем невалидные символы
                cleaned[key] = value.encode('utf-8', 'ignore').decode('utf-8', 'ignore')
            elif isinstance(value, (dict, list)):
                # Рекурсивно очищаем вложенные структуры
                cleaned[key] = self._clean_record(value) if isinstance(value, dict) else value
            else:
                cleaned[key] = value
        
        return cleaned
    
    async def send_record(self, topic: str, record, key=None):
        """Асинхронная отправка записи в Kafka"""
        record_num = self.sent_count + 1
        
        try:
            print(f"  [{record_num}] Отправляю запись в Kafka...")
            print(f"     Тип записи: {type(record).__name__}")
            
            if isinstance(record, dict):
                print(f"     record_id: {record.get('record_id', 'N/A')}")
                print(f"     data_quality: {record.get('data_quality', 'N/A')}")
                
                # Очищаем запись от невалидных символов
                cleaned_record = self._clean_record(record)
                value = cleaned_record
                
            elif isinstance(record, str):
                print(f"     Длина строки: {len(record)} символов")
                # Убираем невалидные UTF-8 символы из строки
                cleaned_str = record.encode('utf-8', 'ignore').decode('utf-8', 'ignore')
                print(f"     После очистки: {len(cleaned_str)} символов")
                
                future = self.producer.send(
                    topic, 
                    key=key.encode('utf-8') if key else None,
                    value=cleaned_str.encode('utf-8')
                )
                future.get(timeout=10)
                self.sent_count += 1
                print(f"  [{record_num}] ✅ Строка отправлена")
                return
                
            elif isinstance(record, bytes):
                print(f"     Бинарные данные: {len(record)} байт")
                # Бинарные данные отправляем как есть
                future = self.producer.send(
                    topic, 
                    key=key.encode('utf-8') if key else None,
                    value=record
                )
                future.get(timeout=10)
                self.sent_count += 1
                print(f"  [{record_num}] ✅ Бинарные данные отправлены")
                return
            else:
                print(f"  [{record_num}] ⚠️ Неизвестный тип записи: {type(record)}")
                self.error_count += 1
                return
            
            # Отправляем JSON
            print(f"     Отправляю JSON в топик '{topic}'...")
            future = self.producer.send(
                topic,
                key=str(key).encode('utf-8') if key else None,
                value=value
            )
            
            # Асинхронное ожидание подтверждения
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: future.get(timeout=10)
            )
            
            self.sent_count += 1
            print(f"  [{record_num}] ✅ Успешно отправлено")
            
            if self.sent_count % 10 == 0:
                print(f"📊 Промежуточный итог: {self.sent_count} записей")
                
        except Exception as e:
            self.error_count += 1
            print(f"  [{record_num}] ❌ ОШИБКА отправки: {type(e).__name__}: {e}")
            print(f"     Трассировка ошибки:")
            traceback.print_exc(file=sys.stdout)
    
    async def stream_to_kafka(self, generator, topic: str, total_records: int = 100):
        """Потоковая отправка данных из генератора в Kafka"""
        print(f"\n{'='*60}")
        print(f"🚀 НАЧАЛО ОТПРАВКИ")
        print(f"   Топик: '{topic}'")
        print(f"   Количество записей: {total_records}")
        print(f"{'='*60}\n")
        
        self.start_time = datetime.now()
        records_sent = 0
        
        try:
            print(f"🔄 Получаю записи из генератора...")
            
            async for record in generator.generate_stream(total_records):
                print(f"\n--- Запись #{records_sent + 1} ---")
                
                # Создаем ключ для партиционирования
                key = None
                if isinstance(record, dict):
                    key = record.get('marketplace') or record.get('product_id')
                    print(f"   Ключ для партиционирования: {key}")
                
                # Отправляем запись
                await self.send_record(topic, record, key)
                records_sent += 1
                
                # Небольшая пауза между отправками
                await asyncio.sleep(0.01)
            
            print(f"\n{'='*60}")
            print(f"✅ ПОТОК ЗАВЕРШЕН")
            print(f"   Получено из генератора: {records_sent} записей")
            
        except Exception as e:
            print(f"\n{'='*60}")
            print(f"❌ КРИТИЧЕСКАЯ ОШИБКА В ПОТОКЕ")
            print(f"   Ошибка: {type(e).__name__}: {e}")
            traceback.print_exc(file=sys.stdout)
            print(f"   Успешно обработано до ошибки: {records_sent} записей")
        
        finally:
            # Завершаем работу продюсера
            print(f"\n🔄 Завершаю работу продюсера...")
            self.producer.flush()
            
            elapsed = (datetime.now() - self.start_time).total_seconds()
            print(f"\n{'='*60}")
            print(f"📊 ФИНАЛЬНАЯ СТАТИСТИКА")
            print(f"   Успешно отправлено: {self.sent_count}")
            print(f"   Ошибок отправки: {self.error_count}")
            print(f"   Всего времени: {elapsed:.2f} сек")
            if elapsed > 0:
                print(f"   Скорость: {self.sent_count/elapsed:.1f} записей/сек")
            print(f"{'='*60}")
    
    def close(self):
        """Закрыть продюсер"""
        print(f"\n🔒 Закрываю соединение с Kafka...")
        self.producer.close()
        print(f"✅ Соединение закрыто")