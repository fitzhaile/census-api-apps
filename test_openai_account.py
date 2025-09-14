#!/usr/bin/env python3
"""
Test script to check OpenAI account status and help troubleshoot quota issues
"""

from dotenv import load_dotenv
import os
import openai

def test_openai_account():
    load_dotenv()
    api_key = os.environ.get('OPENAI_API_KEY')
    
    if not api_key:
        print("❌ No API key found in .env file")
        return
    
    print(f"🔑 API Key found: {api_key[:20]}...")
    
    try:
        client = openai.OpenAI(api_key=api_key)
        
        # Test with a very small request
        print("🧪 Testing API with minimal request...")
        response = client.chat.completions.create(
            model='gpt-3.5-turbo',
            messages=[{'role': 'user', 'content': 'Hi'}],
            max_tokens=5
        )
        print("✅ API key works! Your account is set up correctly.")
        
    except openai.RateLimitError as e:
        print("⚠️  Rate limit exceeded. This might be temporary.")
        print(f"   Error: {e}")
        
    except openai.AuthenticationError as e:
        print("❌ Authentication failed. Check your API key.")
        print(f"   Error: {e}")
        
    except openai.APIConnectionError as e:
        print("❌ Connection error. Check your internet connection.")
        print(f"   Error: {e}")
        
    except Exception as e:
        error_str = str(e)
        if "quota" in error_str.lower() or "billing" in error_str.lower():
            print("💳 QUOTA/BILLING ISSUE DETECTED")
            print("   This usually means:")
            print("   1. No payment method added to your OpenAI account")
            print("   2. Free tier credits exhausted")
            print("   3. Account needs billing verification")
            print("\n🔧 TO FIX:")
            print("   1. Go to: https://platform.openai.com/account/billing")
            print("   2. Add a payment method")
            print("   3. Check your usage at: https://platform.openai.com/usage")
            print(f"\n   Full error: {e}")
        else:
            print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    test_openai_account()
