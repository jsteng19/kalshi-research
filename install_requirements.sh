#!/bin/bash

# Script to install/update requirements with force reinstall for git packages
# This ensures you always get the latest version from your git repos

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing/updating requirements with force reinstall for git packages..."
pip install --upgrade --force-reinstall git+https://github.com/jsteng19/kalshi-python-unofficial.git@main

echo "Installing other requirements..."
pip install -r requirements.txt

echo "Done! All packages updated."
