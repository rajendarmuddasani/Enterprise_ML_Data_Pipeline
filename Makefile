.PHONY: setup lint test serve clean

setup:
	pip install fastapi uvicorn pydantic httpx pytest pytest-cov mlflow numpy prometheus-client python-multipart

lint:
	ruff check fastapi-app/ tests/ --select E,W,F --ignore E501

test:
	pytest tests/test_fastapi.py -v --tb=short

serve:
	cd fastapi-app && uvicorn main:app --host 0.0.0.0 --port 8000 --reload

clean:
	find . -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null; find . -name '*.pyc' -delete 2>/dev/null; true
