import asyncio
import httpx
import io
import pathlib
import time
from PIL import Image, ImageEnhance, ImageFilter
import redis
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL", "http://localhost:3000/analysis")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
POLL_RETRIES = int(os.getenv("POLL_RETRIES", "15"))
POLL_INTERVAL = float(os.getenv("POLL_INTERVAL", "2"))


EXPERIMENTS_DIR = pathlib.Path(__file__).parent / "images"

def load_image(filename: str) -> bytes:
    path = EXPERIMENTS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing image: {path}")
    with open(path, "rb") as f:
        return f.read()


def make_near_duplicate(image_bytes: bytes, variant: int) -> bytes:
    img = Image.open(io.BytesIO(image_bytes)).convert("RGB")

    if variant == 1:
        img = ImageEnhance.Brightness(img).enhance(1.08)
    elif variant == 2:
        w, h = img.size
        img = img.crop((4, 4, w - 4, h - 4)).resize((w, h), Image.LANCZOS)
    elif variant == 3:
        img = img.filter(ImageFilter.GaussianBlur(radius=0.6))
    elif variant == 4:
        img = ImageEnhance.Contrast(img).enhance(1.06)

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=92)
    return buf.getvalue()

async def get_result(client: httpx.AsyncClient, job_id: str, retries: int = POLL_RETRIES) -> dict:
    for _ in range(retries):
        await asyncio.sleep(POLL_INTERVAL)
        response = await client.get(f"{API_URL}/{job_id}", timeout=30)
        data = response.json()
        if data.get("verdict"):
            return data
    return {}


async def upload(client: httpx.AsyncClient, image_bytes: bytes, user_id: str, filename: str = "image.jpg") -> dict:
    files = {"image": (filename, image_bytes, "image/jpeg")} 
    data = {"userId": user_id}
    response = await client.post(API_URL, files=files, data=data, timeout=60)
    job = response.json()
    job_id = job.get("jobId")
    print(f"queued {job_id[:8]}..., waiting for worker...")
    return await get_result(client, job_id)


def print_result(result: dict, user_id: str):
    job_id = result.get("jobId", "?")
    verdict = result.get("verdict", "?")
    reasons = result.get("reasons", [])
    print(f"[{user_id}] job={job_id[:8]}... verdict={verdict}")
    for r in reasons:
        print(f"    • {r}")


async def scenario_1(image_bytes: bytes):
    print("\nScenario 1: Coordinated Multi-Account")
    print("Same image uploaded by 4 users. Expect: flagged (burst + cross-user)")
    users = ["atk_s1_user1", "atk_s1_user2", "atk_s1_user3", "atk_s1_user4"]

    async with httpx.AsyncClient() as client:
        for user_id in users:
            result = await upload(client, image_bytes, user_id)
            print_result(result, user_id)
            await asyncio.sleep(0.3)


async def scenario_2(image_bytes: bytes):
    print("\nScenario 2: Burst Attack")
    print("Same user uploads same image 5 times rapidly. Expect: flagged (burst)")
    user_id = "atk_s2_user1"

    async with httpx.AsyncClient() as client:
        responses = await asyncio.gather(*[
            client.post(API_URL, files={"image": ("image.jpg", image_bytes, "image/jpeg")},
                       data={"userId": user_id}, timeout=60)
            for _ in range(5)
        ])
        job_ids = [r.json().get("jobId") for r in responses]
        print(f"    → queued {len(job_ids)} jobs simultaneously, waiting for results...")

        results = await asyncio.gather(*[get_result(client, job_id) for job_id in job_ids])
        for i, result in enumerate(results):
            print_result(result, f"{user_id} (upload {i+1})")


async def scenario_3(image_bytes: bytes):
    print("\nScenario 3: Near-Duplicate Images")
    print("Modified copies of same image from 3 users. Expect: suspicious (CLIP match, no burst)")
    users = ["atk_s3_user1", "atk_s3_user2", "atk_s3_user3"]

    async with httpx.AsyncClient() as client:
        for i, user_id in enumerate(users):
            variant_bytes = make_near_duplicate(image_bytes, variant=i + 1)
            result = await upload(client, variant_bytes, user_id, filename=f"variant_{i+1}.jpg")
            print_result(result, user_id)
            await asyncio.sleep(2.0)


async def scenario_4(clean_images: dict[str, bytes]):
    print("\nScenario 4: Clean Baseline")
    print("Different images, different users, no coordination. Expect: clean")
    users = ["atk_s4_user1", "atk_s4_user2", "atk_s4_user3"]
    image_list = list(clean_images.values())

    async with httpx.AsyncClient() as client:
        for i, user_id in enumerate(users):
            result = await upload(client, image_list[i], user_id)
            print_result(result, user_id)
            await asyncio.sleep(1.0)


# flush redis
def flush_redis():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    r.flushdb()
    print("Redis flushed.")


async def main():
    print("Synthetic Attack Generator — Phase 8")
    print(f"Target: {API_URL}\n")

    print("Loading images...")
    try:
        attack_image = load_image("attack.jpg")
        clean_images = {
            "clean_1": load_image("clean_1.jpg"),
            "clean_2": load_image("clean_2.jpg"),
            "clean_3": load_image("clean_3.jpg"),
        }
        print("  All images loaded.\n")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Place attack.jpg, clean_1.jpg, clean_2.jpg, clean_3.jpg in the experiments/ folder.")
        return

    start = time.time()

    flush_redis()
    await scenario_1(attack_image)
    await asyncio.sleep(3)

    flush_redis()
    await scenario_2(attack_image)
    await asyncio.sleep(3)

    flush_redis()
    await scenario_3(attack_image)
    await asyncio.sleep(3)

    flush_redis()
    await scenario_4(clean_images)

    elapsed = time.time() - start
    print(f"\nDone in {elapsed:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())