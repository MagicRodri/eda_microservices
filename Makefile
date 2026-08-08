.PHONY: help bootstrap up down logs connectors status diag demo test clean topics ui

COMPOSE ?= docker compose

help:
	@echo "bootstrap   Clone/register the two service repos under services/"
	@echo "up          Build and start the whole stack"
	@echo "connectors  Register the Debezium connectors with Kafka Connect"
	@echo "status      Show connector and task states"
	@echo "diag        Dump the whole pipeline: tasks, topics, counts, logs"
	@echo "topics      List Kafka topics"
	@echo "ui          Print the Kafdrop URL"
	@echo "demo        Run the end-to-end event-loop walkthrough"
	@echo "logs        Tail the service logs"
	@echo "test        Run both services' unit tests"
	@echo "down        Stop the stack"
	@echo "clean       Stop the stack and delete its volumes"

bootstrap:
	bash ./scripts/bootstrap.sh

up:
	$(COMPOSE) up -d --build

connectors:
	bash ./scripts/register-connectors.sh

# A connector stays RUNNING even when all of its tasks have died, so print both.
status:
	@curl -fsS http://localhost:8083/connectors | jq -r '.[]' | sort | while read -r c; do \
		curl -fsS "http://localhost:8083/connectors/$$c/status" | jq -r \
			'"\(.name)  connector=\(.connector.state)  tasks=\(if (.tasks|length)==0 then "NONE" else ([.tasks[].state]|join(",")) end)"'; \
	done

topics:
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:29092 --list

ui:
	@echo "Kafdrop: http://localhost:$${KAFDROP_HOST_PORT:-9000}"

diag:
	bash ./scripts/diagnose.sh

demo:
	bash ./scripts/demo.sh

logs:
	$(COMPOSE) logs -f customer-service order-service

test:
	cd services/customer-service && go test ./...
	cd services/order-service && go test ./...

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v
