# bcra_x_daily.py
import os, json, requests, re
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

X_API_BASE = "https://api.twitter.com/2"
BA_TZ = ZoneInfo("America/Argentina/Buenos_Aires")
USERNAME = "BancoCentral_AR"
CACHE_FILE = Path(".bcra_x_cache.json")

def ba_today():
    return datetime.now(BA_TZ).date()

def is_today_ba(iso_ts: str) -> bool:
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).astimezone(BA_TZ)
    return dt.date() == ba_today()

def matches_signature(text: str) -> bool:
    t = text.lower()
    # Match laxo: suele contener #databcra; podés endurecer con otras frases si querés
    return "#databcra" in t

def auth_headers():
    token = os.getenv("X_BEARER_TOKEN")
    if not token:
        raise RuntimeError("Falta X_BEARER_TOKEN en el entorno.")
    return {"Authorization": f"Bearer {token}"}

def get_user_id(username: str) -> str:
    # cachear para no gastar reads en runs siguientes
    if CACHE_FILE.exists():
        data = json.loads(CACHE_FILE.read_text())
        if data.get("username") == username and "user_id" in data:
            return data["user_id"]

    url = f"{X_API_BASE}/users/by/username/{username}"
    r = requests.get(url, headers=auth_headers(), timeout=30)
    r.raise_for_status()
    user_id = r.json()["data"]["id"]
    CACHE_FILE.write_text(json.dumps({"username": username, "user_id": user_id}))
    return user_id

def fetch_today_databcra_tweet_and_image(save_dir="bcra_imagenes") -> dict:
    user_id = get_user_id(USERNAME)  # 1 read la primera vez; luego cache

    # 1 read/día: pide tweets recientes + medios en una sola llamada
    url = f"{X_API_BASE}/users/{user_id}/tweets"
    params = {
        "max_results": "10",
        "tweet.fields": "created_at,text,attachments",
        "expansions": "attachments.media_keys",
        "media.fields": "url,type"
    }
    r = requests.get(url, headers=auth_headers(), params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    tweets = payload.get("data", [])
    media_index = {m["media_key"]: m for m in payload.get("includes", {}).get("media", [])}

    # filtrar: hoy (BA) + firma
    chosen = None
    for tw in tweets:
        if not is_today_ba(tw.get("created_at", "")):
            continue
        if not matches_signature(tw.get("text", "")):
            continue
        # ¿tiene imagen?
        media_keys = tw.get("attachments", {}).get("media_keys", [])
        photos = []
        for mk in media_keys:
            m = media_index.get(mk)
            if m and m.get("type") == "photo" and m.get("url"):
                photos.append(m["url"])
        if photos:
            chosen = {"tweet": tw, "photos": photos}
            break

    if not chosen:
        raise RuntimeError("No encontré tuit de HOY con #DataBCRA y foto.")

    # descargar la primera foto
    save_dir = Path(save_dir); save_dir.mkdir(parents=True, exist_ok=True)
    fname = f"bcra_{ba_today().isoformat()}.jpg"
    out_path = save_dir / fname

    img_url = chosen["photos"][0]
    ir = requests.get(img_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    ir.raise_for_status()
    out_path.write_bytes(ir.content)

    return {
        "tweet_id": chosen["tweet"]["id"],
        "created_at_utc": chosen["tweet"]["created_at"],
        "text_preview": re.sub(r"\s+", " ", chosen["tweet"]["text"])[:240],
        "image_url": img_url,
        "saved_path": str(out_path.resolve())
    }

if __name__ == "__main__":
    # export X_BEARER_TOKEN="tu_token"
    meta = fetch_today_databcra_tweet_and_image()
    print(json.dumps(meta, ensure_ascii=False, indent=2))
