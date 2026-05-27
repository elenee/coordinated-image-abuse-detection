def make_decision(
    harm_score: float,
    similar_users: list[str],
    burst_detected: bool,
    clip_similarity: float | None
) -> dict:
    reasons = []

    risk_score = 0.0

    risk_score += harm_score * 0.5

    if harm_score >= 0.7:
        reasons.append(f"high harm score ({harm_score})")
    elif harm_score >= 0.4:
        reasons.append(f"elevated harm score ({harm_score})")

    if len(similar_users) >= 1:
        risk_score += 0.25
        reasons.append(
            f"same image seen from {len(similar_users)} other account(s): {similar_users}"
        )

    if burst_detected:
        risk_score += 0.35

        if len(similar_users) >= 1:
            reasons.append(
                f"coordinated burst across {len(similar_users)} accounts"
            )
        else:
            reasons.append("rapid repeat uploads detected")

    if clip_similarity is not None and clip_similarity >= 0.95:
        risk_score += 0.2
        reasons.append(
            f"near-identical semantic content (CLIP similarity {clip_similarity})"
        )

    if risk_score >= 0.8:
        verdict = "flagged"
    elif risk_score >= 0.45:
        verdict = "suspicious"
    else:
        verdict = "clean"

    return {
        "verdict": verdict,
        "riskScore": round(risk_score, 4),
        "reasons": reasons if reasons else ["no signals detected"]
    }