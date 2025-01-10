#!/bin/bash

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Running Gaia Validator Tests${NC}"
echo "=================================="

# Activate existing .gaia environment
source .gaia/bin/activate

# Ensure test dependencies are installed
echo -e "\n${GREEN}Checking test dependencies...${NC}"
pip install -q pytest pytest-asyncio pytest-cov

# Run tests with coverage
echo -e "\n${GREEN}Running tests with coverage...${NC}"
pytest \
    --cov=gaia \
    --cov-report=term-missing \
    --cov-report=html \
    tests/validator/components/ \
    tests/validator/integration/ \
    tests/validator/test_validator_v2.py \
    "$@"

# Check if tests passed
if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}All tests passed!${NC}"
    echo -e "\nCoverage report available in htmlcov/index.html"
else
    echo -e "\n${RED}Some tests failed${NC}"
    exit 1
fi 