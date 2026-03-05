import asyncio
from kafka import KafkaProducer
import json
from datetime import datetime
import traceback
import sys
from logger import logger

class KafkaMarketplaceProducer:
       
    def __init__(self, bootstrap_servers='localhost:9092'):

        logger.info(f"Инициализация продюсера Kafka: {bootstrap_servers}")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                max_block_ms=10000 
            )
            logger.info(f"Продюсер Kafka создан успешно")
        except Exception as e:
            logger.error(f"ОШИБКА создания продюсера Kafka: {e}")
            logger.error(f"Убедитесь, что Kafka запущена на {bootstrap_servers}")
            raise
        
        self.sent_count = 0
        self.error_count = 0
        self.start_time = None
    
    def _clean_record(self, record):
        
        if not isinstance(record, dict):
            return record
        
        cleaned = {}
        for key, value in record.items():
            if isinstance(value, str):
                cleaned[key] = value.encode('utf-8', 'ignore').decode('utf-8', 'ignore')
            elif isinstance(value, (dict, list)):
                cleaned[key] = self._clean_record(value) if isinstance(value, dict) else value
            else:
                cleaned[key] = value
        
        return cleaned
    
    async def send_record(self, topic: str, record, key=None):
        
        record_num = self.sent_count + 1
        
        try:
            logger.info(f"[{record_num}] Отправка записи в Kafka")
            logger.info(f"Тип записи: {type(record).__name__}")
            
            if isinstance(record, dict):
                logger.info(f"record_id: {record.get('record_id', 'N/A')}")
                logger.info(f"data_quality: {record.get('data_quality', 'N/A')}")
                
                cleaned_record = self._clean_record(record)
                value = cleaned_record
                
            elif isinstance(record, str):

                logger.info(f"Длина строки: {len(record)} символов")
                
                cleaned_str = record.encode('utf-8', 'ignore').decode('utf-8', 'ignore')
                logger.info(f"После очистки: {len(cleaned_str)} символов")
                
                future = self.producer.send(
                    topic, 
                    key=key.encode('utf-8') if key else None,
                    value=cleaned_str.encode('utf-8')
                )
                future.get(timeout=10)
                self.sent_count += 1
                logger.info(f"[{record_num}] Строка отправлена")
                return
                
            elif isinstance(record, bytes):

                logger.info(f"Бинарные данные: {len(record)} байт")
                
                future = self.producer.send(
                    topic, 
                    key=key.encode('utf-8') if key else None,
                    value=record
                )
                future.get(timeout=10)
                self.sent_count += 1
                logger.info(f"[{record_num}] Бинарные данные отправлены")
                return
            else:
                logger.info(f"[{record_num}] Неизвестный тип записи: {type(record)}")
                self.error_count += 1
                return
            
            logger.info(f"JSON идет в топик '{topic}'...")
            future = self.producer.send(
                topic,
                key=str(key).encode('utf-8') if key else None,
                value=value
            )
                        
            await asyncio.get_event_loop().run_in_executor(
                None, lambda: future.get(timeout=10)
            )
            
            self.sent_count += 1
            logger.info(f"[{record_num}] Успешно отправлено")
            
            if self.sent_count % 10 == 0:
                logger.info(f"Промежуточный итог: {self.sent_count} записей")
                
        except Exception as e:
            self.error_count += 1
            logger.error(f"[{record_num}] ОШИБКА отправки: {type(e).__name__}: {e}", exc_info=True)
            
    
    async def stream_to_kafka(self, generator, topic: str, total_records: int = 100):
              
        logger.info(f"отправка в топик: '{topic}'")
        logger.info(f"Количество записей топика: {total_records}")
        
        
        self.start_time = datetime.now()
        records_sent = 0
        
        try:
                        
            async for record in generator.generate_stream(total_records):
                logger.info(f" Запись из генератора #{records_sent + 1} ")
                                
                key = None
                if isinstance(record, dict):
                    key = record.get('marketplace') or record.get('product_id')
                    logger.info(f"Ключ для партиционирования: {key}")
                                
                await self.send_record(topic, record, key)
                records_sent += 1
                await asyncio.sleep(0.01)
                        
            logger.info(f"Получено из генератора: {records_sent} записей")
            
        except Exception as e:
            logger.error("Ошибка генератора:", exc_info=True)
            
        
        finally:
            
            self.producer.flush()
            
            elapsed = (datetime.now() - self.start_time).total_seconds()
            
            logger.info(f"Успешно отправлено: {self.sent_count}")
            logger.info(f"Ошибок отправки: {self.error_count}")
            logger.info(f"Всего времени: {elapsed:.2f} сек")
            logger.info(f"Скорость: {self.sent_count/elapsed:.1f} записей/сек" if elapsed > 0 else "Скорость: 0 записей/сек")
            
    
    def close(self):
                
        self.producer.close()
        logger.info(f"Соединение закрыто")