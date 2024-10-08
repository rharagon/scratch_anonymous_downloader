ENV_FILE = .env
# make start IDENTIFIER=2 AMOUNT=3
requirements:
	pip install -r requirements.txt

build:
	docker-compose build

start:
	@echo "IDENTIFIER=$(IDENTIFIER)" > $(ENV_FILE) 
	@echo "AMOUNT=$(AMOUNT)" >> $(ENV_FILE)       
	docker-compose up


stop:
	docker-compose down


