# VARIAVEIS
COMPOSE_FILE = .docker/compose.yml
ENV_FILE = src/.env
CONTAINER = mongodb_ecommerce

.PHONY: up down shell connect clean logs help

# COMANDOS
help:
	@echo "Available commands:"
	@echo " up      - Start MongoDB container"
	@echo " down    - Stop and remove containers"
	@echo " shell   - Access the container shell"
	@echo " connect - Connect to MongoDB using mongosh"
	@echo " clean   - Remove MongoDB data volume"
	@echo " logs    - Show MongoDB container logs"

up:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) down

shell:
	docker exec -it $(CONTAINER) bash

connect:
	docker exec -it $(CONTAINER) mongosh "mongodb://admin:admin@localhost:27017/admin?authSource=admin"

logs:
	docker logs $(CONTAINER)

clean:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) down -v