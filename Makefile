.PHONY: help install test run-web1 run-web2 demo concurrency-check clean docker-up docker-down

help:
	@echo "make install            安装运行与测试依赖"
	@echo "make test               运行 pytest 测试套件"
	@echo "make run-web1           启动 Web 进程 1 (127.0.0.1:8101)"
	@echo "make run-web2           启动 Web 进程 2 (127.0.0.1:8102，共库)"
	@echo "make demo               双进程端到端演示（需先启动两个进程并导入样例）"
	@echo "make concurrency-check  跨进程并发竞赛与不变量校验"
	@echo "make docker-up          docker compose 构建并启动（含样例 seed）"
	@echo "make docker-down        停止并删除卷（清空演示数据）"
	@echo "make clean              清理缓存与本地数据库"

PYTHON ?= python
export ER_DB_PATH ?= $(CURDIR)/data/er.db

install:
	$(PYTHON) -m pip install -r requirements-dev.txt

test:
	$(PYTHON) -m pytest tests/ -q

run-web1:
	$(PYTHON) -m uvicorn app.main:app --host 127.0.0.1 --port 8101

run-web2:
	$(PYTHON) -m uvicorn app.main:app --host 127.0.0.1 --port 8102

demo:
	$(PYTHON) -I scripts/demo.py

concurrency-check:
	$(PYTHON) -I scripts/concurrency_check.py

docker-up:
	docker compose up --build

docker-down:
	docker compose down -v

clean:
	rm -rf .pytest_cache **/__pycache__ data
