#!/bin/bash
set -e

echo "Cleaning Python cache..."
command find src/ -name "__pycache__" -type d -exec rm -rf {} +
command find src/ -name "*.pyc" -delete

echo "Building src.zip (checkpoints only)..."
rm -f src.zip
cd src && zip -r ../src.zip checkpoints/ && cd ..

echo "Applying Terraform..."
cd terraform && terraform apply -auto-approve
cd ..

echo "Deploy complete."
