import asyncio
import os
from dotenv import load_dotenv
from openai import AsyncOpenAI

# Load variables from .env
load_dotenv()

async def test_connection():
    api_key = os.environ.get("OPENROUTER_API_KEY")
    
    if not api_key or api_key == "dummy_key":
        print("❌ Error: OPENROUTER_API_KEY is missing or invalid in your .env file.")
        return

    print("🔌 Connecting to OpenRouter...")
    client = AsyncOpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key
    )

    try:
        # We test with the Gemini model we configured
        resp = await client.chat.completions.create(
            model="google/gemini-2.5-pro",
            messages=[
                {"role": "user", "content": "Reply with only the words: 'Key is working!'"}
            ],
            max_tokens=100
        )
        print("✅ Success! The model responded:")
        print(f"   💬 {resp.choices[0].message.content}")
        print("\n--- Full Response Payload ---")
        print(resp.model_dump_json(indent=2))
        
    except Exception as e:
        print(f"❌ Failed to connect: {e}")

if __name__ == "__main__":
    asyncio.run(test_connection())
