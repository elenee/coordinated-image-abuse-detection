import os
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/moderation")

async def save_analysis(job_id: str, phash: str, similar_users: list[str],
                        burst_detected: bool, harm_score: float, clip_similarity: float | None, 
                        verdict: str, reasons: list[str], report: str | None):
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        await conn.execute("""
            INSERT INTO "Analysis" ("id", "jobId", "pHash", "similarUsers", "burstDetected", 
            "harmScore", "clipSimilarity", "verdict", "reasons", "report", "analyzedAt")
            VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
        """, job_id, phash, similar_users, burst_detected, harm_score, 
            clip_similarity, verdict, reasons, report)

        await conn.execute("""
            UPDATE "Job" SET "status" = 'done' WHERE "id" = $1
        """, job_id)
    finally:
        await conn.close()