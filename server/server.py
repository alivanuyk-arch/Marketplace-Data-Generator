from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import json
from datetime import datetime
from typing import Optional
import marketplace_generator  # наш генератор
import uvicorn

app = FastAPI(title="Marketplace Data Generator API", version="1.0")

# Разрешаем CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Глобальный счётчик
generated_total = 0
start_time = datetime.now()

@app.get("/")
async def root():
    """Информация о сервере"""
    return {
        "service": "Marketplace Data Generator",
        "version": "1.0",
        "total_generated": generated_total,
        "uptime": str(datetime.now() - start_time),
        "endpoints": {
            "generate": "POST /generate - генерация данных",
            "stream": "GET /stream - потоковая генерация",
            "health": "GET /health - проверка здоровья",
            "stats": "GET /stats - статистика"
        }
    }

@app.post("/generate")
async def generate_data(
    count: int = 1000,
    format: str = "json",
    anomaly_rate: Optional[float] = None,
    output: Optional[str] = None
):
    """
    Генерация данных
    
    - count: количество записей (макс 100000)
    - format: json, csv, jsonl
    - anomaly_rate: процент аномалий (0.0-1.0)
    - output: имя файла для сохранения
    """
    global generated_total
    
    # Ограничения
    if count > 100000:
        raise HTTPException(status_code=400, detail="Максимум 100000 записей за запрос")
    
    if anomaly_rate is not None and not 0 <= anomaly_rate <= 1:
        raise HTTPException(status_code=400, detail="anomaly_rate должен быть между 0 и 1")
    
    # Настраиваем профиль аномалий если нужно
    anomaly_profile = None
    if anomaly_rate is not None:
        anomaly_profile = {
            "normal": 1.0 - anomaly_rate,
            **{k: anomaly_rate/14 for k in marketplace_generator.ANOMALY_PROFILE.keys() if k != "normal"}
        }
    
    generator = marketplace_generator.UnifiedDataGenerator(anomaly_profile)
    
    if format == "json":
        # Возвращаем JSON
        records = []
        async for record in generator.generate_stream(count):
            if record is not None:
                records.append(record)
        
        generated_total += len(records)
        return {
            "count": len(records),
            "records": records,
            "generated_at": datetime.now().isoformat()
        }
    
    elif format == "jsonl":
        # Потоковый вывод
        async def generate_jsonl():
            nonlocal generated_total
            generated = 0
            async for record in generator.generate_stream(count):
                if record is not None:
                    if isinstance(record, dict):
                        yield json.dumps(record, ensure_ascii=False) + "\n"
                    elif isinstance(record, str):
                        yield record + "\n"
                    generated += 1
            
            generated_total += generated
        
        return StreamingResponse(
            generate_jsonl(),
            media_type="application/x-ndjson",
            headers={
                "Content-Disposition": f"attachment; filename=data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            }
        )
    
    elif format == "csv":
        # CSV поток
        async def generate_csv():
            nonlocal generated_total
            generated = 0
            first = True
            
            async for record in generator.generate_stream(count):
                if record is not None and isinstance(record, dict):
                    if first:
                        # Заголовок
                        yield ",".join(record.keys()) + "\n"
                        first = False
                    
                    # Значения (экранируем запятые)
                    values = []
                    for v in record.values():
                        if v is None:
                            values.append("")
                        else:
                            values.append(str(v).replace(",", ";").replace("\n", " "))
                    
                    yield ",".join(values) + "\n"
                    generated += 1
            
            generated_total += generated
        
        return StreamingResponse(
            generate_csv(),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename=data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            }
        )

@app.get("/stream")
async def stream_data(count: int = 1000):
    """Server-Sent Events поток данных"""
    async def event_stream():
        generator = marketplace_generator.UnifiedDataGenerator()
        
        async for record in generator.generate_stream(count):
            if record is not None:
                if isinstance(record, dict):
                    yield f"data: {json.dumps(record, ensure_ascii=False)}\n\n"
                elif isinstance(record, str):
                    yield f"data: {{\"raw\": \"{record}\"}}\n\n"
            await asyncio.sleep(0.01)  # Небольшая задержка
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive"
        }
    )

@app.get("/health")
async def health_check():
    """Проверка здоровья сервера"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "generated_total": generated_total,
        "uptime_seconds": (datetime.now() - start_time).total_seconds()
    }

@app.get("/stats")
async def get_stats():
    """Статистика генерации"""
    return {
        "total_generated": generated_total,
        "requests_per_minute": generated_total / max((datetime.now() - start_time).total_seconds() / 60, 1),
        "start_time": start_time.isoformat(),
        "current_time": datetime.now().isoformat()
    }

@app.post("/generate-file")
async def generate_to_file(
    background_tasks: BackgroundTasks,
    count: int = 10000,
    format: str = "parquet",
    filename: Optional[str] = None
):
    """Генерация данных в файл (асинхронно)"""
    if filename is None:
        filename = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{format}"
    
    async def generate_async():
        if format == "parquet":
            await marketplace_generator.export_to_parquet(filename, count)
        elif format == "jsonl":
            await marketplace_generator.export_to_jsonl(filename, count)
    
    # Запускаем в фоне
    background_tasks.add_task(generate_async)
    
    return {
        "status": "started",
        "filename": filename,
        "count": count,
        "format": format,
        "message": "Генерация запущена в фоне"
    }

if __name__ == "__main__":
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
