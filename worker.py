import os
from dotenv import load_dotenv
import google.generativeai as genai
from supabase import create_client, Client
import arq
from arq.connections import RedisSettings

load_dotenv()

# --- Gemini Setup ---
gemini_api_key = os.getenv("GEMINI_API_KEY")
if gemini_api_key: genai.configure(api_key=gemini_api_key)
else: print("FATAL: WORKER - GEMINI_API_KEY not found.")

# --- Supabase Setup ---
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_KEY") 
if not supabase_url or not supabase_key:
    print("FATAL: WORKER - Supabase credentials not found.")
    supabase = None
else:
    supabase: Client = create_client(supabase_url, supabase_key)

# --- AI Model Initialization ---
transcript_model = None
icebreaker_model = None
if gemini_api_key:
    try:
        transcript_model = genai.GenerativeModel('gemini-2.5-flash-preview-09-2025', system_instruction="You are an expert meeting analyst. Format your response in Markdown.")
        icebreaker_model = genai.GenerativeModel('gemini-2.5-flash-preview-09-2025', system_instruction="You are a world-class sales rep. Format your response in Markdown.")
    except Exception as e: print(f"Error initializing GenerativeModels: {e}")

# --- Worker Functions (The Jobs) ---

async def run_transcript_analysis(ctx, job_id: str, item_dict: dict):
    redis = ctx['redis'] 
    if transcript_model is None or supabase is None:
        print("WORKER ERROR: Models or Supabase not initialized.")
        await redis.set(f"result:{job_id}", "Error: Worker not initialized", ex=300)
        return
    
    try:
        prompt = f"""
        Review the following meeting transcript from {item_dict.get('company')} held on {item_dict.get('date')} with {item_dict.get('attendees')}.
        Transcript: {item_dict.get('transcript')}
        
        Please summarize (in Markdown):
        1. What was done well and why.
        2. What could be improved.
        3. 3 actionable recommendations.
        """
        
        response = transcript_model.generate_content(prompt)
        output = response.text.strip()

        # 1. Save to Supabase (for history)
        # *** THIS IS THE FIX ***
        data_to_insert = {
            "company": item_dict.get('company'),
            "attendees": item_dict.get('attendees'),
            "date": item_dict.get('date'),
            "transcript": item_dict.get('transcript'),
            "analysis": output,
        }
        supabase.table("meeting_analysis").insert(data_to_insert).execute()
        
        # 2. Save result to Redis FOR THE FRONTEND
        await redis.set(f"result:{job_id}", output, ex=300) 
        
        print(f"WORKER: Successfully processed job {job_id}")

    except Exception as e:
        error_message = f"WORKER ERROR in 'run_transcript_analysis': {e}"
        print(error_message)
        await redis.set(f"result:{job_id}", f"Error: {e}", ex=300)


async def run_icebreaker_generation(ctx, job_id: str, item_dict: dict):
    redis = ctx['redis']
    if icebreaker_model is None or supabase is None:
        print("WORKER ERROR: Models or Supabase not initialized.")
        await redis.set(f"result:{job_id}", "Error: Worker not initialized", ex=300)
        return

    try:
        prompt = f"""
        Analyze this LinkedIn bio and pitch deck.
        Bio: {item_dict.get('linkedin_bio')}
        Deck: {item_dict.get('pitch_deck')}
        
        Generate a cold outreach icebreaker... (rest of your prompt)
        
        IMPORTANT: Format your entire response using Markdown.
        """
        
        response = icebreaker_model.generate_content(prompt)
        output = response.text.strip()

        # 1. Save to Supabase (for history)
        # *** THIS IS THE FIX ***
        data_to_insert = {
            "linkedin_bio": item_dict.get('linkedin_bio'),
            "pitch_deck": item_dict.get('pitch_deck'),
            "analysis": output,
        }
        supabase.table("icebreaker_analysis").insert(data_to_insert).execute()
        
        # 2. Save result to Redis FOR THE FRONTEND
        await redis.set(f"result:{job_id}", output, ex=300) 
        
        print(f"WORKER: Successfully processed job {job_id}")

    except Exception as e:
        error_message = f"WORKER ERROR in 'run_icebreaker_generation': {e}"
        print(error_message)
        await redis.set(f"result:{job_id}", f"Error: {e}", ex=300)

# --- Worker Settings ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

class WorkerSettings:
    functions = [run_transcript_analysis, run_icebreaker_generation]
    redis_settings = RedisSettings.from_dsn(REDIS_URL)
    
if __name__ == "__main__":
    arq.run_worker(WorkerSettings)