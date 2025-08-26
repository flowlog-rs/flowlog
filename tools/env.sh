#!/bin/bash
set -e

echo "[SETUP] Setting up Macaron development environment..."

# Detect operating system
OS="$(uname -s)"
echo "[DETECT] Operating system: $OS"

# Install system packages
echo "[CHECK] Checking system packages..."

if [[ "$OS" == "Linux" ]]; then
    # Linux (Ubuntu/Debian)
    sudo apt update -qq && sudo apt upgrade -y -qq
    
    packages=()
    command -v htop >/dev/null || packages+=("htop")
    command -v dos2unix >/dev/null || packages+=("dos2unix")
    
    if [ ${#packages[@]} -gt 0 ]; then
        echo "[INSTALL] Installing Linux packages: ${packages[*]}"
        sudo apt install -y -qq "${packages[@]}"
    fi
    
elif [[ "$OS" == "Darwin" ]]; then
    # Check if Homebrew is installed
    if ! command -v brew >/dev/null 2>&1; then
        echo "[INSTALL] Installing Homebrew..."
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        # Add Homebrew to PATH for current session
        eval "$(/opt/homebrew/bin/brew shellenv)"
    else
        echo "[FOUND] Homebrew already installed"
    fi
    
    # Update Homebrew
    echo "[UPDATE] Updating Homebrew..."
    brew update >/dev/null
    
    packages=()
    command -v htop >/dev/null || packages+=("htop")
    command -v dos2unix >/dev/null || packages+=("dos2unix")
    
    if [ ${#packages[@]} -gt 0 ]; then
        echo "[INSTALL] Installing macOS packages: ${packages[*]}"
        brew install "${packages[@]}"
    fi
    
else
    echo "[WARNING] Unsupported operating system: $OS"
    echo "[WARNING] Skipping system package installation..."
fi

#!/usr/bin/env bash

# This script installs the latest stable Rust toolchain for development purposes.
set -e

if ! command -v rustup &> /dev/null; then
    echo "rustup not found. Installing rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    export PATH="$HOME/.cargo/bin:$PATH"
else
    echo "rustup is already installed."
fi

echo "Updating Rust toolchain to the latest stable version..."
rustup install stable
rustup default stable

echo "Rust installation complete."
fi

echo "[VERIFY] Verifying Macaron compilation..."
cargo check

echo "[COMPLETE] Environment setup completed successfully!"