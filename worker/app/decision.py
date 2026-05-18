def make_decision(
    harm_score: float,
    similar_users: list[str],
    burst_detected: bool,
    clip_similarity: float | None
) -> dict:
    reasons = []
    verdict = "clean"

    if harm_score >= 0.7:
        reasons.append(f"high harm score ({harm_score})")
        verdict = "flagged"
    elif harm_score >= 0.4:
        reasons.append(f"elevated harm score ({harm_score})")
        if verdict != "flagged":
            verdict = "suspicious"

    if burst_detected:
        reasons.append(f"coordinated burst across {len(similar_users)} accounts")
        verdict = "flagged"
    elif len(similar_users) >= 1:
        reasons.append(f"same image seen from {len(similar_users)} other account(s): {similar_users}")
        if verdict == "clean":
            verdict = "suspicious"

    if clip_similarity and clip_similarity >= 0.95:
        reasons.append(f"near-identical semantic content (CLIP similarity {clip_similarity})")
        if verdict == "clean":
            verdict = "suspicious"

    return {
        "verdict": verdict,
        "reasons": reasons if reasons else ["no signals detected"]
    }