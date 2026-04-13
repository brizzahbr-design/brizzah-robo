import os
import re
import json
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

from flask import Flask, request, jsonify, render_template_string, redirect

app = Flask(__name__)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

INSTAGRAM_FILE = DATA_DIR / "instagram_posts.json"
TOP100_FILE = DATA_DIR / "top100.json"
MANUAL_FILE = DATA_DIR / "manual_posts.json"

API_TOKEN = os.getenv("API_TOKEN", "troque-este-token")
WHATSAPP_GROUP_URL = os.getenv("WHATSAPP_GROUP_URL", "https://chat.whatsapp.com/SEU_LINK_AQUI")
SITE_NAME = os.getenv("SITE_NAME", "Brizzah")
INSTAGRAM_URL = os.getenv("INSTAGRAM_URL", "https://instagram.com/brizzah.br")
MAX_INSTAGRAM_ITEMS = int(os.getenv("MAX_INSTAGRAM_ITEMS", "100"))

EXCLUDED_WORDS = [
    "relógio", "relogio", "smartwatch", "watch",
    "carteira", "wallet",
    "película", "pelicula",
]

EXCLUDED_PATTERNS = [
    r"\bkit\b.*\bpel[ií]cula\b",
    r"\bkit\b.*\brel[oó]gio\b",
    r"\bkit\b.*\bcarteira\b",
    r"\bcombo\b.*\brel[oó]gio\b",
    r"\bcombo\b.*\bcarteira\b",
    r"\b[2-9]\s*(un|unds|unidades|peças|pecas|pares)\b",
    r"\bleve\s*[2-9]\b",
    r"\blote\b",
    r"\batacado\b",
    r"\bsortido\b",
    r"\b3\s*t[eê]nis\b",
    r"\b2\s*t[eê]nis\b",
    r"\bkit\b.*\b[2-9]\b",
]

PRIORITY_TERMS = [
    # casa e utilidades
    "organizador", "cozinha", "limpeza", "casa", "lar", "escorredor",
    "aspirador", "mixer", "air fryer", "luminária", "luminaria",
    "suporte celular", "fone", "bluetooth", "portátil", "portatil",
    "multiuso", "dobrável", "dobravel", "recarregável", "recarregavel",

    # moda feminina
    "vestido", "blusa feminina", "calça feminina", "calca feminina",
    "short feminino", "saia", "sandália feminina", "sandalia feminina",
    "tênis feminino", "tenis feminino", "bolsa feminina", "jaqueta feminina",
    "pijama feminino", "conjunto feminino", "legging", "fitness feminino",

    # moda masculina
    "camiseta masculina", "camisa polo", "bermuda masculina",
    "calça masculina", "calca masculina", "moletom masculino",
    "jaqueta masculina", "tênis masculino", "tenis masculino",
    "chinelo masculino", "camisa masculina", "fitness masculino",

    # moda infantil
    "infantil", "bebê", "bebe", "vestido infantil", "conjunto infantil",
    "camiseta infantil", "sandália infantil", "sandalia infantil",
    "tênis infantil", "tenis infantil", "pijama infantil",
    "mochila infantil",
]

BOOST_TERMS = [
    "mais vendido", "promoção", "promocao", "oferta", "lançamento",
    "lancamento", "premium", "viral", "tendência", "tendencia",
    "confortável", "confortavel", "ajustável", "ajustavel",
    "lavável", "lavavel", "impermeável", "impermeavel"
]


def ensure_file(path: Path):
    if not path.exists():
        path.write_text("[]", encoding="utf-8")


def load_json(path: Path):
    ensure_file(path)
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []


def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_text(text: str) -> str:
    return (text or "").strip().lower()


def require_token():
    token = request.headers.get("X-API-KEY") or request.args.get("token")
    if token != API_TOKEN:
        return False
    return True


def is_bad_product(title: str) -> bool:
    t = normalize_text(title)

    for word in EXCLUDED_WORDS:
        if word in t:
            return True

    for pattern in EXCLUDED_PATTERNS:
        if re.search(pattern, t):
            return True

    return False


def relevance_score(title: str, sold_count: int = 0, rating: float = 0.0, price: float = 0.0) -> int:
    t = normalize_text(title)
    score = 0

    for term in PRIORITY_TERMS:
        if term in t:
            score += 2

    for term in BOOST_TERMS:
        if term in t:
            score += 1

    if sold_count >= 50:
        score += 1
    if sold_count >= 200:
        score += 2
    if sold_count >= 500:
        score += 2

    if rating >= 4.8:
        score += 2
    elif rating >= 4.5:
        score += 1

    if 19 <= price <= 199:
        score += 1
    elif 200 <= price <= 299:
        score += 1

    return score


def passes_commercial_rules(price: float, rating: float, sold_count: int) -> bool:
    if rating and rating < 4.4:
        return False
    if sold_count and sold_count < 20:
        return False
    if price is not None and price <= 0:
        return False
    return True


def classify_product(title: str) -> str:
    t = normalize_text(title)
    if any(x in t for x in ["feminina", "vestido", "legging", "saia", "blusa feminina", "bolsa feminina"]):
        return "Moda Feminina"
    if any(x in t for x in ["masculina", "camiseta masculina", "bermuda masculina", "polo", "moletom masculino"]):
        return "Moda Masculina"
    if any(x in t for x in ["infantil", "bebê", "bebe", "pijama infantil", "mochila infantil"]):
        return "Moda Infantil"
    return "Casa & Utilidades"


def should_post_product(title: str, sold_count: int = 0, rating: float = 0.0, price: float = 0.0):
    if is_bad_product(title):
        return False, 0, "Produto bloqueado pelo filtro comercial."

    if not passes_commercial_rules(price, rating, sold_count):
        return False, 0, "Produto reprovado por nota, vendas ou preço."

    score = relevance_score(title, sold_count, rating, price)
    if score >= 3:
        return True, score, "Produto aprovado."
    return False, score, "Pontuação comercial insuficiente."


def normalize_product(item: dict, origin: str):
    now = datetime.utcnow().isoformat()
    title = item.get("title", "").strip()
    category = item.get("category", "").strip() or classify_product(title)

    return {
        "id": item.get("id") or f"{origin}-{int(datetime.utcnow().timestamp() * 1000)}",
        "title": title,
        "price": item.get("price", "").strip(),
        "old_price": item.get("old_price", "").strip(),
        "image": item.get("image", "").strip(),
        "link": item.get("link", "").strip(),
        "badge": item.get("badge", "").strip(),
        "category": category,
        "source": origin,
        "created_at": item.get("created_at") or now,
        "posted_caption": item.get("posted_caption", "").strip(),
        "active": bool(item.get("active", True)),
    }


def detect_marketplace(url: str) -> str:
    u = (url or "").lower()
    if "amazon." in u:
        return "Amazon"
    if "mercadolivre." in u or "mercadolibre." in u:
        return "Mercado Livre"
    if "shopee." in u:
        return "Shopee"
    return "Outro"


def clean_product_url(url: str) -> str:
    if not url:
        return url

    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        query = parse_qs(parsed.query)

        if "amazon." in domain:
            allowed = {"tag", "ascsubtag", "linkCode", "camp", "creative", "creativeASIN"}
        elif "mercadolivre." in domain or "mercadolibre." in domain:
            allowed = {"matt_tool", "matt_word", "matt_source", "matt_campaign_id", "matt_ad_group_id"}
        else:
            allowed = set()

        filtered = {k: v for k, v in query.items() if k in allowed}
        new_query = urlencode(filtered, doseq=True)

        return urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            ""
        ))
    except Exception:
        return url


def build_whatsapp_message(title: str, link: str, price: str = "", old_price: str = "", category: str = "") -> str:
    lines = ["🚨 ACHADO DO DIA!", ""]
    if title:
        lines.append(f"🔥 {title}")
    if category:
        lines.append(f"🛍️ {category}")
    lines.append("")
    if old_price and price:
        lines.append(f"💸 De {old_price} por {price}")
    elif price:
        lines.append(f"💸 {price}")

    lines.extend([
        "",
        "👇 Link:",
        link,
        "",
        "📲 Entre no grupo VIP para receber antes de todo mundo:",
        WHATSAPP_GROUP_URL
    ])
    return "\n".join(lines).strip()


HOME_HTML = """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>{{ site_name }} • Painel rápido</title>
  <style>
    body{font-family:Arial,sans-serif;background:#0d1522;color:#eef4ff;margin:0;padding:24px}
    .wrap{max-width:980px;margin:0 auto}
    .card{background:#132033;border-radius:18px;padding:20px;margin-bottom:18px}
    a{color:#7ee7df}
    input,textarea,select{width:100%;padding:12px;border-radius:12px;border:none;margin-top:8px;margin-bottom:12px}
    button{padding:12px 16px;border:none;border-radius:12px;font-weight:700;cursor:pointer}
    .primary{background:#18d8c8}
    .muted{color:#a9bdd8}
    .row{display:grid;grid-template-columns:1fr 1fr;gap:14px}
    @media(max-width:700px){.row{grid-template-columns:1fr}}
    pre{white-space:pre-wrap;background:#0b1220;padding:14px;border-radius:12px}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>{{ site_name }} • Painel rápido</h1>
      <p class="muted">Instagram: <a href="{{ instagram_url }}" target="_blank">{{ instagram_url }}</a></p>
      <p><a href="/grupo" target="_blank">Entrar no grupo VIP</a></p>
    </div>

    <div class="card">
      <h2>Gerar texto para WhatsApp</h2>
      <form method="post" action="/manual-preview">
        <div class="row">
          <div><label>Nome do produto<input name="title" required></label></div>
          <div><label>Categoria<input name="category" placeholder="Moda Feminina, Moda Masculina, Moda Infantil..."></label></div>
        </div>
        <div class="row">
          <div><label>Preço<input name="price" placeholder="R$ 99,90"></label></div>
          <div><label>Preço antigo<input name="old_price" placeholder="R$ 149,90"></label></div>
        </div>
        <label>Link do produto<input name="link" required></label>
        <button class="primary" type="submit">Gerar preview</button>
      </form>
    </div>
  </div>
</body>
</html>
"""


PREVIEW_HTML = """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Preview WhatsApp</title>
  <style>
    body{font-family:Arial,sans-serif;background:#0d1522;color:#eef4ff;margin:0;padding:24px}
    .wrap{max-width:900px;margin:0 auto}
    .card{background:#132033;border-radius:18px;padding:20px;margin-bottom:18px}
    a{color:#7ee7df}
    pre{white-space:pre-wrap;background:#0b1220;padding:14px;border-radius:12px}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>Preview da mensagem</h1>
      <p>Marketplace detectado: <strong>{{ marketplace }}</strong></p>
      <p>Link limpo: <a href="{{ cleaned_link }}" target="_blank">{{ cleaned_link }}</a></p>
      <pre>{{ text }}</pre>
      <p><a href="/">Voltar</a></p>
    </div>
  </div>
</body>
</html>
"""


@app.route("/")
def home():
    return render_template_string(HOME_HTML, site_name=SITE_NAME, instagram_url=INSTAGRAM_URL)


@app.route("/grupo")
def grupo():
    return redirect(WHATSAPP_GROUP_URL, code=302)


@app.route("/manual-preview", methods=["POST"])
def manual_preview():
    title = request.form.get("title", "").strip()
    price = request.form.get("price", "").strip()
    old_price = request.form.get("old_price", "").strip()
    link = request.form.get("link", "").strip()
    category = request.form.get("category", "").strip() or classify_product(title)

    cleaned = clean_product_url(link)
    text = build_whatsapp_message(title, cleaned, price, old_price, category)
    marketplace = detect_marketplace(cleaned)

    return render_template_string(PREVIEW_HTML, text=text, cleaned_link=cleaned, marketplace=marketplace)


@app.route("/api/filter-product", methods=["POST"])
def api_filter_product():
    data = request.get_json(force=True, silent=True) or {}
    title = data.get("title", "")
    sold_count = int(data.get("sold_count", 0) or 0)
    rating = float(data.get("rating", 0) or 0)
    price = float(data.get("price", 0) or 0)

    allowed, score, reason = should_post_product(title, sold_count, rating, price)

    return jsonify({
        "postar": allowed,
        "score": score,
        "reason": reason,
        "category": classify_product(title),
    })


@app.route("/api/generate-whatsapp", methods=["POST"])
def api_generate_whatsapp():
    data = request.get_json(force=True, silent=True) or {}
    title = data.get("title", "").strip()
    price = data.get("price", "").strip()
    old_price = data.get("old_price", "").strip()
    link = clean_product_url(data.get("link", "").strip())
    category = data.get("category", "").strip() or classify_product(title)

    text = build_whatsapp_message(title, link, price, old_price, category)

    return jsonify({
        "marketplace": detect_marketplace(link),
        "cleaned_link": link,
        "text": text,
    })


@app.route("/api/instagram/add", methods=["POST"])
def api_instagram_add():
    if not require_token():
        return jsonify({"ok": False, "error": "Token inválido"}), 401

    payload = request.get_json(force=True, silent=True) or {}
    product = normalize_product(payload, origin="instagram")

    if not product["title"] or not product["link"]:
        return jsonify({"ok": False, "error": "Campos obrigatórios: title e link"}), 400

    items = load_json(INSTAGRAM_FILE)
    items = [x for x in items if x.get("link") != product["link"]]
    items.insert(0, product)
    save_json(INSTAGRAM_FILE, items[:MAX_INSTAGRAM_ITEMS])

    return jsonify({"ok": True, "item": product})


@app.route("/api/top100/add", methods=["POST"])
def api_top100_add():
    if not require_token():
        return jsonify({"ok": False, "error": "Token inválido"}), 401

    payload = request.get_json(force=True, silent=True) or {}
    product = normalize_product(payload, origin="top100")

    if not product["title"] or not product["link"]:
        return jsonify({"ok": False, "error": "Campos obrigatórios: title e link"}), 400

    items = load_json(TOP100_FILE)
    items = [x for x in items if x.get("link") != product["link"]]
    items.insert(0, product)
    save_json(TOP100_FILE, items[:100])

    return jsonify({"ok": True, "item": product})


@app.route("/api/manual/add", methods=["POST"])
def api_manual_add():
    if not require_token():
        return jsonify({"ok": False, "error": "Token inválido"}), 401

    payload = request.get_json(force=True, silent=True) or {}
    payload["link"] = clean_product_url(payload.get("link", ""))
    product = normalize_product(payload, origin="manual")

    if not product["title"] or not product["link"]:
        return jsonify({"ok": False, "error": "Campos obrigatórios: title e link"}), 400

    items = load_json(MANUAL_FILE)
    items = [x for x in items if x.get("link") != product["link"]]
    items.insert(0, product)
    save_json(MANUAL_FILE, items[:100])

    text = build_whatsapp_message(
        product["title"],
        product["link"],
        product["price"],
        product["old_price"],
        product["category"]
    )

    return jsonify({"ok": True, "item": product, "whatsapp_text": text})


@app.route("/api/health")
def api_health():
    return jsonify({"ok": True, "site_name": SITE_NAME})


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=True)