# Makefile
.PHONY: help run build deploy test clean

help:
	@echo "Доступные команды:"
	@echo "  make run      - Запустить локально (python server.py)"
	@echo "  make build    - Собрать Docker образ"
	@echo "  make deploy   - Запустить всё через Docker"
	@echo "  make test     - Запустить тесты"
	@echo "  make clean    - Очистить временные файлы"

run:
	uvicorn server:app --reload --host 0.0.0.0 --port 8000

build:
	docker build -t marketplace-generator .

deploy:
	docker-compose up --build

test:
	python -m pytest tests/ -v

clean:
	docker-compose down -v
	rm -rf data/* logs/* mlruns/*

# Дополнительные команды
generate:
	python marketplace_generator.py --count 10000 --format jsonl

benchmark:
	@echo "Тест производительности:"
	@time python -c "import asyncio; from marketplace_generator import UnifiedDataGenerator; asyncio.run((lambda: __import__('asyncio').create_task((lambda g: (async for _ in g.generate_stream(10000)))(UnifiedDataGenerator())))())"
