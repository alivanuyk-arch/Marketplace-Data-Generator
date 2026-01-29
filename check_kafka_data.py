from kafka import KafkaConsumer
import json

def check_kafka_messages():
    print("🔍 Проверка сообщений в Kafka...")
    
    consumer = KafkaConsumer(
        'marketplace-data',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id='check-group',
        value_deserializer=lambda x: try_decode(x)
    )
    
    print(f"📊 Сообщений в топике: {consumer.partitions_for_topic('marketplace-data')}")
    
    count = 0
    anomaly_count = 0
    
    try:
        for message in consumer:
            count += 1
            
            # Показываем первые 5 сообщений подробно
            if count <= 5:
                print(f"\n📩 Сообщение #{count}:")
                print(f"   Offset: {message.offset}")
                print(f"   Partition: {message.partition}")
                
                if isinstance(message.value, dict):
                    print(f"   Тип: dict")
                    print(f"   record_id: {message.value.get('record_id')}")
                    print(f"   data_quality: {message.value.get('data_quality')}")
                    if message.value.get('data_quality') != 'normal':
                        anomaly_count += 1
                else:
                    print(f"   Тип: {type(message.value).__name__}")
                    print(f"   Значение: {str(message.value)[:50]}...")
                    anomaly_count += 1
            
            if count >= 20:  # Проверим 20 сообщений
                break
    
    finally:
        consumer.close()
    
    print(f"\n📊 Статистика проверки:")
    print(f"   Всего сообщений проверено: {count}")
    print(f"   Аномалий обнаружено: {anomaly_count}")
    print(f"   Процент аномалий: {(anomaly_count/count*100):.1f}%")

def try_decode(value):
    """Пытается декодировать сообщение"""
    try:
        return json.loads(value.decode('utf-8'))
    except:
        return value  # Возвращаем как есть, если не JSON

if __name__ == "__main__":
    check_kafka_messages()