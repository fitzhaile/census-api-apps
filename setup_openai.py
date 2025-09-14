#!/usr/bin/env python3
"""
Simple script to help set up OpenAI API key for the Census API app
"""

import os
import sys

def setup_openai_key():
    print("🔑 OpenAI API Key Setup for Census API App")
    print("=" * 50)
    
    # Check if key is already set
    existing_key = os.environ.get('OPENAI_API_KEY')
    if existing_key:
        print(f"✅ OpenAI API key is already set: {existing_key[:10]}...")
        return True
    
    print("\n📋 To get your OpenAI API key:")
    print("1. Go to: https://platform.openai.com/api-keys")
    print("2. Sign in to your OpenAI account")
    print("3. Click 'Create new secret key'")
    print("4. Copy the key (starts with 'sk-')")
    
    print("\n🔧 Setup options:")
    print("Option 1: Set for current session only")
    print("  export OPENAI_API_KEY='your-key-here'")
    print("  python server.py")
    
    print("\nOption 2: Set permanently in your shell")
    print("  echo 'export OPENAI_API_KEY=\"your-key-here\"' >> ~/.zshrc")
    print("  source ~/.zshrc")
    
    print("\nOption 3: Create .env file (recommended)")
    print("  echo 'OPENAI_API_KEY=\"your-key-here\"' > .env")
    
    print("\n⚠️  After setting the key, restart the server:")
    print("  python server.py")
    
    return False

if __name__ == "__main__":
    setup_openai_key()
