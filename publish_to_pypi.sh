#!/bin/bash

echo "🚀 Starting package publication process..."

# Create and activate virtual environment if not exists
if [ ! -d "venv" ]; then
    echo "📦 Creating new virtual environment..."
    python3 -m venv venv
else
    echo "📦 Using existing virtual environment..."
fi
source venv/bin/activate

# Install required tools
echo "🔧 Installing required tools..."
pip install twine setuptools

# Get version from setup.py
VERSION=$(python -c "import re; print(re.search(r'version=\"([^\"]+)\"', open('setup.py').read()).group(1))")
echo "📝 Current version: $VERSION"

# Clean dist folder
echo "🧹 Cleaning dist folder..."
rm -rf dist/*

# Build the package
echo "🏗️  Building package..."
python setup.py sdist bdist_wheel

# Create git tag
echo "🏷️  Creating git tag v$VERSION..."
git tag -a "v$VERSION" -m "Release version $VERSION"
git push origin "v$VERSION"

# Upload the package to PyPI
echo "📤 Uploading package to PyPI..."
twine upload dist/*

# Increment version
IFS='.' read -ra VERSION_PARTS <<< "$VERSION"
NEW_PATCH=$((VERSION_PARTS[2] + 1))
NEW_VERSION="${VERSION_PARTS[0]}.${VERSION_PARTS[1]}.$NEW_PATCH"
echo "📈 Incrementing version to $NEW_VERSION..."
sed -i '' "s/version=\"$VERSION\"/version=\"$NEW_VERSION\"/" setup.py

# Deactivate virtual environment
echo "👋 Deactivating virtual environment..."
deactivate

echo "✅ Publication process completed successfully!"
