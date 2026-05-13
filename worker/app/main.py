import io
import redis
import imagehash
from PIL import Image
from fastapi import FastAPI, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional
import json
import time
import os
import asyncio
import aio_pika
from contextlib import asynccontextmanager
import asyncpg


HASH_WINDOW_SECONDS = 86400
SIMILARITY_THRESHOLD = 10

BURST_THRESHOLD = int(os.getenv("BURST_THRESHOLD", "3"))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672")
QUEUE_NAME = "analysis_jobs"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/moderation")



r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True
)


async def process_job(message: aio_pika.IncomingMessage):
    async with message.process():
        data = json.loads(message.body.decode())
        job_id = data["jobId"]
        user_id = data["userId"]
        image_path = data["imagePath"]

        print(f"Processing job {job_id} for user {user_id}")

        try:
            image = Image.open(image_path)
            phash = str(imagehash.phash(image))

            key = f"phash:{phash}"
            entry = json.dumps({"userId": user_id, "timestamp": time.time()})
            r.rpush(key, entry)
            r.expire(key, HASH_WINDOW_SECONDS)

            similar_users = []
            all_keys = r.keys("phash:*")

            for k in all_keys:
                stored_hash_str = k.replace("phash:", "")
                try:
                    stored_hash = imagehash.hex_to_hash(stored_hash_str)
                    current_hash = imagehash.hex_to_hash(phash)
                    distance = stored_hash - current_hash
                    if distance <= SIMILARITY_THRESHOLD:
                        entries = r.lrange(k, 0, -1)
                        now = time.time()
                        for e in entries:
                            parsed = json.loads(e)
                            if parsed["userId"] != user_id and (now - parsed["timestamp"]) <= HASH_WINDOW_SECONDS:
                                similar_users.append(parsed["userId"])
                except Exception:
                    continue

            similar_users = list(set(similar_users))
            burst_detected = len(similar_users) >= BURST_THRESHOLD

            print(f"Job {job_id} done — pHash: {phash}, similarUsers: {similar_users}, burst: {burst_detected}")

            conn = await asyncpg.connect(DATABASE_URL)
            try:
                await conn.execute("""
                    INSERT INTO "Analysis" ("id", "jobId", "pHash", "similarUsers", "burstDetected", "analyzedAt")
                    VALUES (gen_random_uuid(), $1, $2, $3, $4, NOW())
                """, job_id, phash, similar_users, burst_detected)

                await conn.execute("""
                    UPDATE "Job" SET "status" = 'done' WHERE "id" = $1
                """, job_id)
            finally:
                await conn.close()

            if os.path.exists(image_path):
                os.remove(image_path)

        except Exception as e:
            print(f"Error processing job {job_id}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"Connecting to RabbitMQ at {RABBITMQ_URL}")
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        await queue.consume(process_job)
        print("Worker listening for jobs...")
    except Exception as e:
        print(f"Failed to connect to RabbitMQ: {e}")
    yield
    await connection.close()


app = FastAPI(lifespan=lifespan)


class FingerprintResult(BaseModel):
    pHash: str
    similarUsers: list[str]
    burstDetected: bool
    matchCount: int


@app.post("/fingerprint", response_model=FingerprintResult)
async def fingerprint(
    file: UploadFile = File(...),
    userId: str = Form(...)
):
    contents = await file.read()
    image = Image.open(io.BytesIO(contents))
    phash = str(imagehash.phash(image))

    key = f"phash:{phash}"
    entry = json.dumps({"userId": userId, "timestamp": time.time()})
    r.rpush(key, entry)
    r.expire(key, HASH_WINDOW_SECONDS)

    similar_users = []
    all_keys = r.keys("phash:*")

    for k in all_keys:
        stored_hash_str = k.replace("phash:", "")
        try:
            stored_hash = imagehash.hex_to_hash(stored_hash_str)
            current_hash = imagehash.hex_to_hash(phash)
            distance = stored_hash - current_hash
            if distance <= SIMILARITY_THRESHOLD:
                entries = r.lrange(k, 0, -1)
                for e in entries:
                    parsed = json.loads(e)
                    now = time.time()
                    if parsed["userId"] != userId and (now - parsed["timestamp"]) <= HASH_WINDOW_SECONDS:
                        similar_users.append(parsed["userId"])
        except Exception:
            continue

    similar_users = list(set(similar_users))
    burst_detected = len(similar_users) >= BURST_THRESHOLD

    return FingerprintResult(
        pHash=phash,
        similarUsers=similar_users,
        burstDetected=burst_detected,
        matchCount=len(similar_users)
    )


@app.get("/health")
def health():
    return {"status": "ok"}