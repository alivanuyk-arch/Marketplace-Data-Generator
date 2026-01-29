from kafka import KafkaConsumer
import json
from datetime import datetime

class MarketplaceConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'marketplace-data',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            group_id='marketplace-processor',
            value_deserializer=lambda x: self.deserialize_message(x)
        )
    
    def deserialize_message(self, message):
        """Обрабатывает разные типы сообщений"""
        try:
            return json.loads(message.decode('utf-8'))
        except json.JSONDecodeError:
            return {"raw_data": str(message), "type": "non_json"}
        except Exception as e:
            return {"error": str(e), "raw": str(message)}
    
    def process_stream(self):
        """Обрабатывает поток сообщений"""
        print("🚀 Запуск обработки сообщений...")
        
        for message in self.consumer:
            record = message.value
            
            if isinstance(record, dict) and 'data_quality' in record:
                # Обработка в зависимости от качества данных
                if record['data_quality'] == 'normal':
                    self.process_normal(record)
                else:
                    self.process_anomaly(record)
            else:
                print(f"⚠️ Неизвестный формат: {type(record)}")
    
    def process_normal(self, record):
        """Обработка нормальных записей"""
        print(f"📦 Нормальная запись: {record.get('product_name')} - {record.get('price_rub')} руб")
    
    def process_anomaly(self, record):
        """Обработка аномалий"""
        anomaly_type = record.get('data_quality', 'unknown')
        print(f"🚨 Аномалия [{anomaly_type}]: {record.get('record_id', 'N/A')}")