import io
import imagehash
from PIL import Image
from fastapi import FastAPI, UploadFile, File, Form
from pydantic import BaseModel
import json
import os
import asyncio
import aio_pika
from contextlib import asynccontextmanager


from clip import score_harm, get_clip_embedding, cosine_similarity
from fingerprint import store_phash, find_similar_users, store_clip_embedding, find_max_clip_similarity
from database import save_analysis
from decision import make_decision


BURST_THRESHOLD = int(os.getenv("BURST_THRESHOLD", "3"))
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672")
QUEUE_NAME = "analysis_jobs"


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
            embedding = get_clip_embedding(image)
            harm_score, harm_category = score_harm(image)

            store_phash(phash, user_id)
            store_clip_embedding(job_id, embedding)

            similar_users = find_similar_users(phash, user_id)
            max_clip_similarity = find_max_clip_similarity(job_id, embedding, cosine_similarity)
            burst_detected = len(similar_users) >= BURST_THRESHOLD

            decision = make_decision(harm_score, similar_users, burst_detected, max_clip_similarity)
            verdict = decision["verdict"]
            reasons = decision["reasons"]

            print(f"Job {job_id} done — pHash: {phash}, verdict: {verdict}, reasons: {reasons}, similarUsers: {similar_users}, burst: {burst_detected}, harm: {harm_score}")

            await save_analysis(job_id, phash, similar_users, burst_detected, harm_score, max_clip_similarity, verdict, reasons)

            if os.path.exists(image_path):
                os.remove(image_path)

        except Exception as e:
            print(f"Error processing job {job_id}: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    connection = None
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
    if connection:
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

    store_phash(phash, userId)
    similar_users = find_similar_users(phash, userId)
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