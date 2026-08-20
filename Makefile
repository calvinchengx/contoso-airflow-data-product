# The data product. The ONLY place a platform is mentioned.
SHELL := /bin/bash
PLATFORM ?= ../fabric-platform-airflow3

.PHONY: help up down run logs test lint
help: ## This list
	@grep -hE '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/' | expand -t14

up: ## Stand the platform up, pointed at this product
	$(MAKE) -C $(PLATFORM) up PRODUCT=$(CURDIR)

down: ## Tear it down
	$(MAKE) -C $(PLATFORM) down

logs: ## Follow the platform's Airflow logs
	$(MAKE) -C $(PLATFORM) logs

run: ## Trigger contoso_daily and wait for it
	$(MAKE) -C $(PLATFORM) trigger DAG=contoso_daily

show-product: ## Stage the core product's SQL locally and list what it contains
	@uv run python -m contoso_product.show --into product

test: ## Product unit tests -- no platform, no emulator, no credentials
	uv run --extra dev pytest tests -q

lint: ## ruff over the product
	uv run --extra dev ruff check .
