#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = dcf-terrifictotes-solutions
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}:${WD}/src
SHELL := /bin/bash
PROFILE = default
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Some tools (black?) may have issues with Python versions 3.12.5 or greater. Consider selecting an earlier Python interpreter."
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source ./venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)
	$(call execute_in_env, $(PIP) install -r ./requirements.txt -t dependencies/python)

################################################################################################################
# Set Up

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install boto3
boto3:
	$(call execute_in_env, $(PIP) install boto3)

## Install moto
moto:
	$(call execute_in_env, $(PIP) install moto)

## Install pytest
pytest:
	$(call execute_in_env, $(PIP) install pytest)
	$(call execute_in_env, $(PIP) install pytest-cov)

## Install local dependencies -- supplied separately to AWS
local-dependencies:
	$(call execute_in_env, $(PIP) install pandas)
	$(call execute_in_env, $(PIP) install pyarrow)
	$(call execute_in_env, $(PIP) install iso4217)

## Set up dev requirements (bandit, safety, black)
dev-setup: black coverage bandit safety moto pytest local-dependencies

# Build / Run

## Run the security tests
security-test:
	$(call execute_in_env, safety check -r ./requirements.txt --ignore 70612)

	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

## Run the black code check
run-black:
	$(call execute_in_env, black  ./src/*.py ./test/*.py)

## Run terraform formatting check
terraform-fmt:
	$(call execute_in_env, terraform fmt ./terraform/)

## Run the unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest -v test/)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --cov=src test/)

## Run all checks
run-checks: security-test run-black terraform-fmt unit-test check-coverage
