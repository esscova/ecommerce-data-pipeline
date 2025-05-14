# --- variaveis
COMPOSE_FILE = .docker/compose.yml
ENV_FILE = .env
MONGO_CONTAINER = mongodb_ecommerce
POSTGRES_CONTAINER = postgres_ecommerce

.PHONY: up down shell-mongo shell-postgres connect-mongo connect-postgres logs-mongo logs-postgres clean help all

help:
	@echo "Available commands:"
	@echo " up             - Start all containers"
	@echo " down           - Stop and remove all containers"
	@echo " shell-mongo    - Access MongoDB container shell"
	@echo " shell-postgres - Access PostgreSQL container shell"
	@echo " connect-mongo  - Connect to MongoDB using mongosh"
	@echo " connect-postgres - Connect to PostgreSQL using psql"
	@echo " logs-mongo     - Show MongoDB container logs"
	@echo " logs-postgres  - Show PostgreSQL container logs"
	@echo " clean          - Remove all data volumes"
	@echo " clean-mongo    - Remove only MongoDB data volume"
	@echo " clean-postgres - Remove only PostgreSQL data volume"

# subir todos os containers
up:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) up -d

# parar todos os containers
down:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) down

# MongoDB 
shell-mongo:
	docker exec -it $(MONGO_CONTAINER) bash

connect-mongo:
	docker exec -it $(MONGO_CONTAINER) mongosh "mongodb://admin:admin@localhost:27017/admin?authSource=admin"

logs-mongo:
	docker logs $(MONGO_CONTAINER)

clean-mongo:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) down --volumes mongo_data

# PostgreSQL 
shell-postgres:
	docker exec -it $(POSTGRES_CONTAINER) bash

connect-postgres:
	docker exec -it $(POSTGRES_CONTAINER) psql -U admin -d analytics

logs-postgres:
	docker logs $(POSTGRES_CONTAINER)

clean-postgres:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) down --volumes postgres_data

# limpar volumes
clean:
	docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE) down -v