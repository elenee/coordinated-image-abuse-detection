import os
import httpx

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL = "gemini-2.0-flash"

ENABLE_LLM_REPORT = os.getenv("ENABLE_LLM_REPORT", "false").lower() == "true"


async def generate_report(
    job_id: str,
    user_id: str,
    verdict: str,
    reasons: list[str],
    harm_score: float,
    similar_users: list[str],
    burst_detected: bool,
    clip_similarity: float | None
) -> str | None:
    if not ENABLE_LLM_REPORT:
        return None
    if verdict != "flagged":
        return None

    if not GEMINI_API_KEY:
        print("No GEMINI_API_KEY set, skipping report generation")
        return None

    prompt = f"""You are a content moderation analyst. Write a concise moderation report for the following flagged upload event.

Job ID: {job_id}
Uploaded by: {user_id}
Verdict: {verdict}
Harm score: {harm_score} (scale 0-1)
Similar accounts: {similar_users}
Burst detected: {burst_detected}
CLIP similarity: {clip_similarity}
Reasons: {reasons}

Write 2-3 sentences summarizing what happened and why this was flagged. Be factual and specific. Do not use bullet points."""

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent?key={GEMINI_API_KEY}",
                headers={"content-type": "application/json"},
                json={
                    "contents": [
                        {"parts": [{"text": prompt}]}
                    ],
                    "generationConfig": {
                        "maxOutputTokens": 256
                    }
                },
                timeout=30.0
            )
            data = response.json()
            print(f"Gemini response: {data}")
            return data["candidates"][0]["content"]["parts"][0]["text"]
    except Exception as e:
        print(f"LLM report generation failed: {e}")
        return None