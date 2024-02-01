from fastAPI import FastAPI
from .simple_update import run_update
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

app = FastAPI()
scheduler = AsyncIOScheduler()

scheduler.add_job(run_update(), CronTrigger(hour=7, minute=30))

@app.on_event("startup")
async def start_scheduler():
    scheduler.start()

@app.get("/api/health")
def health():
    return {"status": "ok"}

@app.get("/api/whoami")
def health():
    return {"name": "mini_elastic_updater", "version": "0.0.1"}

@app.get("/api/update_agent_search_index")
def update_agent_search_index():
    try:
        run_update()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "message": str(e)}