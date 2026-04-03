# Makefile
IMAGE_NAME ?= docker.elastic.co/cloud/consumption_framework
VERSION ?= latest

LAMBDA_TARGET ?= lambda_package

.PHONY: build-and-release
build-and-release: build release

.PHONY: build
build:
	docker build -t $(IMAGE_NAME):$(VERSION) .

.PHONY: release
release:
	zip -r $(VERSION).zip README.md Dockerfile Makefile config.yml.sample consumption entrypoint.sh kibana_exports main.py requirements*.txt -x "**/__pycache__/*"
	docker push $(IMAGE_NAME):$(VERSION)
	@echo "Pushed Docker image: $(IMAGE_NAME):$(VERSION)"

.PHONY: lambda-layers
lambda-layers:
	echo "Creating dependencies layer"
	mkdir -p $(LAMBDA_TARGET)/python
	pip3 install -r requirements_lambda.txt -t $(LAMBDA_TARGET)/python --platform manylinux2014_aarch64 --implementation cp --only-binary=:all: --upgrade
	cd $(LAMBDA_TARGET) && zip -r ../$(LAMBDA_TARGET)_dependencies.zip python -x "*/__pycache__*" > /dev/null
	echo "Creating main layer"
	zip -r $(LAMBDA_TARGET)_main.zip consumption/ -x "*/__pycache__*" > /dev/null
	zip $(LAMBDA_TARGET)_main.zip lambda_function.py

.PHONY: lambda-release
lambda-release:
	ROLE_ARN=$(shell aws iam create-role \
		--role-name consumption_framework_role \
		--assume-role-policy-document '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}' \
		--query Role.Arn \
		--output text \
		--no-paginate \
		--no-cli-pager) && \
	DEPENDENCIES_LAYER_ARN=$(shell aws lambda publish-layer-version \
		--layer-name consumption_framework_dependencies \
		--zip-file fileb://$(LAMBDA_TARGET)_dependencies.zip \
		--compatible-runtimes python3.11 \
		--compatible-architectures arm64 \
		--query LayerVersionArn \
		--output text \
		--no-paginate \
		--no-cli-pager) && \
	echo "Role ARN: $$ROLE_ARN" && \
	echo "Dependencies Layer ARN: $$DEPENDENCIES_LAYER_ARN" && \
	aws lambda create-function \
		--role $$ROLE_ARN \
		--function-name consumption-framework \
		--runtime python3.11 \
		--handler lambda_function.handler \
		--zip-file fileb://$(LAMBDA_TARGET)_main.zip \
		--timeout 300 \
		--layers $$DEPENDENCIES_LAYER_ARN \
		--architectures arm64 \
		--no-cli-pager
	aws iam attach-role-policy --role-name consumption_framework_role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole


.PHONY: clean
clean:
	docker rmi -f $(IMAGE_NAME):$(VERSION) || true
	rm -rf lambda_package*
