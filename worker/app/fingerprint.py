import json
import time
import os
import imagehash
import redis as redis_lib
from PIL import Image

HASH_WINDOW_SECONDS = 86400
SIMILARITY_THRESHOLD = 10

r = redis_lib.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True
)

import json
import time
import os
import imagehash
import redis as redis_lib
from PIL import Image

HASH_WINDOW_SECONDS = 86400
SIMILARITY_THRESHOLD = 10

r = redis_lib.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True
)


def store_phash(phash: str, user_id: str):
    key = f"phash:{phash}"
    entry = json.dumps({"userId": user_id, "timestamp": time.time()})
    r.rpush(key, entry)
    r.expire(key, HASH_WINDOW_SECONDS)


def find_similar_users(phash: str, user_id: str) -> list[str]:
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

    return list(set(similar_users))


def store_clip_embedding(job_id: str, embedding: list[float]):
    r.set(f"clip:{job_id}", json.dumps(embedding), ex=HASH_WINDOW_SECONDS)


def find_max_clip_similarity(job_id: str, embedding: list[float], cosine_fn) -> float | None:
    max_similarity = 0.0
    all_clip_keys = r.keys("clip:*")

    for k in all_clip_keys:
        stored_job_id = k.replace("clip:", "")
        if stored_job_id == job_id:
            continue
        try:
            stored_embedding = json.loads(r.get(k))
            similarity = cosine_fn(embedding, stored_embedding)
            if similarity >= 0.85 and similarity > max_similarity:
                max_similarity = similarity
        except Exception:
            continue

    return max_similarity if max_similarity > 0 else None