import torch
import open_clip
from PIL import Image

print("Loading CLIP model...")
clip_model, _, clip_preprocess = open_clip.create_model_and_transforms('ViT-B-32', pretrained='openai')
clip_model.eval()
clip_tokenizer = open_clip.get_tokenizer('ViT-B-32')
HARM_LABELS = ["normal content", "violence", "weapons", "hate symbols", "explicit content"]
print("CLIP model loaded.")


def score_harm(image: Image.Image) -> tuple[float, str]:
    image_tensor = clip_preprocess(image).unsqueeze(0)
    text_tokens = clip_tokenizer(HARM_LABELS)
    
    with torch.no_grad():
        image_features = clip_model.encode_image(image_tensor)
        text_features = clip_model.encode_text(text_tokens)
        image_features /= image_features.norm(dim=-1, keepdim=True)
        text_features /= text_features.norm(dim=-1, keepdim=True)
        probs = (100.0 * image_features @ text_features.T).softmax(dim=-1)[0]
    
    probs_list = probs.tolist()
    # normal_prob = probs_list[0]
    # harm_score = round(1.0 - normal_prob, 4)
    harm_probs = probs_list[1:]
    harm_score = round(max(harm_probs), 4)
    harm_category = HARM_LABELS[probs_list.index(max(harm_probs), 1)]
    
    return harm_score, harm_category


def get_clip_embedding(image: Image.Image) -> list[float]:
    image_tensor = clip_preprocess(image).unsqueeze(0)
    with torch.no_grad():
        embedding = clip_model.encode_image(image_tensor)
        embedding /= embedding.norm(dim=-1, keepdim=True)
    return embedding[0].tolist()

def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    return round(dot, 4)