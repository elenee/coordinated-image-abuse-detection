import io
import redis
import imagehash
from PIL import Image
from fastapi import FastAPI, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional
import json
import time

app = FastAPI()

r = redis.Redis(host="redis", port=6379, decode_responses=True)

HASH_WINDOW_SECONDS = 86400
SIMILARITY_THRESHOLD = 10


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
                    if parsed["userId"] != userId:
                        similar_users.append(parsed["userId"])
        except Exception:
            continue

    similar_users = list(set(similar_users))
    burst_detected = len(similar_users) >= 2

    return FingerprintResult(
        pHash=phash,
        similarUsers=similar_users,
        burstDetected=burst_detected,
        matchCount=len(similar_users)
    )


@app.get("/health")
def health():
    return {"status": "ok"}