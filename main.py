from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
import os
import arq
from arq import create_pool
from arq.connections import ArqRedis, RedisSettings
import uuid # To generate unique job IDs

load_dotenv()

app = FastAPI()

# --- Pydantic Models ---
class TranscriptInput(BaseModel):
    company: Optional[str] = "Unknown Company"
    attendees: Optional[str] = "Not specified"
    date: Optional[str] = "Unknown date"
    transcript: Optional[str] = ""

class IcebreakerInput(BaseModel):
    linkedin_bio: str
    pitch_deck: str

# --- Queue Setup ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_settings = RedisSettings.from_dsn(REDIS_URL)

async def get_queue() -> ArqRedis:
    return await create_pool(redis_settings)

@app.on_event("startup")
async def startup():
    app.state.queue = await get_queue()

@app.on_event("shutdown")
async def shutdown():
    await app.state.queue.close()

# --- Middleware ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Endpoints ---

@app.get("/")
def home():
    return {"message": "Backend API is running!"}

@app.post("/analyze_transcript")
async def queue_transcript_analysis(item: TranscriptInput, queue: ArqRedis = Depends(get_queue)):
    job_id = str(uuid.uuid4())
    try:
        await queue.enqueue_job("run_transcript_analysis", job_id, item.dict())
        return {"status": "queued", "job_id": job_id}
    except Exception as e:
        return {"error": f"Failed to queue job: {str(e)}"}


@app.post("/generate_icebreaker")
async def queue_icebreaker_generation(item: IcebreakerInput, queue: ArqRedis = Depends(get_queue)):
    job_id = str(uuid.uuid4())
    try:
        await queue.enqueue_job("run_icebreaker_generation", job_id, item.dict())
        return {"status": "queued", "job_id": job_id}
    except Exception as e:
        return {"error": f"Failed to queue job: {str(e)}"}

@app.get("/get_job_result")
async def get_job_result(job_id: str, queue: ArqRedis = Depends(get_queue)):
    """
    Checks Redis for the result of a job.
    """
    key = f"result:{job_id}"
    result = await queue.get(key)
    
    if result:
        # Job is done, return result and delete key
        await queue.delete(key) 
        return {"status": "complete", "analysis": result.decode('utf-8')}
    else:
        # Job not done yet
        return {"status": "pending"}


# Supabase client for History Panel (still needed)
from supabase import create_client, Client
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

@app.get("/get_analyses")
def get_analyses():
    if supabase is None: return {"error": "Database not connected"}
    try:
        response = supabase.table("meeting_analysis").select("*").order("created_at", desc=True).execute()
        return {"data": response.data}
    except Exception as e: return {"error": str(e)}

@app.get("/get_icebreakers")
def get_icebreakers():
    if supabase is None: return {"error": "Database not connected"}
    try:
        response = supabase.rpc("get_icebreakers_with_snippet").execute()
        return {"data": response.data}
    except Exception as e: return {"error": str(e)}