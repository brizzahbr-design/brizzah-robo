# -*- coding: utf-8 -*-
# BRIZZAH V3 SCRAPER PROFISSIONAL - importação de links com slug/deeplink e copy sem Produto importado
import os, sqlite3, requests, json, time, threading, re, hashlib, base64, tempfile, unicodedata
from datetime import datetime
from zoneinfo import ZoneInfo
TZ_BR = ZoneInfo("America/Sao_Paulo")
from io import BytesIO
from collections import defaultdict
from flask import Flask, render_template_string, request, redirect, url_for, session, jsonify, Response, send_file

app = Flask(__name__)

# Diretório de imagens — usa /var/data (Render Disk, compartilhado entre workers)
# Fallback para /tmp se disco não estiver montado
_SLIDE_DIR = "/var/data/slides" if os.path.isdir("/var/data") else os.path.join(tempfile.gettempdir(), "brizzah_slides")
os.makedirs(_SLIDE_DIR, exist_ok=True)
print(f"[SLIDES] Diretório: {_SLIDE_DIR}")

# Cache em memória para vitrine (evita chamadas repetidas à API Shopee)
_vitrine_cache = {"dados": [], "ts": 0, "nicho": ""}
_VITRINE_CACHE_TTL = 7200  # 2 horas

def _aquecer_vitrine():
    """Pré-aquece o cache da vitrine em background para carregamento instantâneo."""
    import time as _t
    _t.sleep(20)  # aguarda app inicializar
    while True:
        try:
            nicho = cfg("niche_keyword","geral") or "geral"
            agora = _t.time()
            if not _vitrine_cache["dados"] or agora - _vitrine_cache["ts"] > _VITRINE_CACHE_TTL - 300:
                dados = buscar_pool_produtos_top(nicho_base=nicho)
                dados = filtrar_produtos_top(remover_repetidos_recentes(dados, horas=24), nicho_alvo=nicho, limite=48)
                if dados:
                    _vitrine_cache.update({"dados":dados,"ts":agora,"nicho":nicho})
                    log("INFO",f"[VITRINE] Cache aquecido: {len(dados)} produtos")
        except Exception as e:
            log("WARN",f"[VITRINE] Erro pré-aquecimento: {str(e)[:50]}")
        _t.sleep(3600)  # re-aquece a cada 1h

import threading as _tvit
# Render PRO: pré-aquecimento da vitrine é opcional para evitar loops extras em múltiplos workers.
if os.environ.get("BRIZZAH_VITRINE_WARMUP", "false").lower() == "true":
    _tvit.Thread(target=_aquecer_vitrine, daemon=True).start()
    print("[VITRINE] Warmup ativado")
else:
    print("[VITRINE] Warmup desativado por padrão para maior estabilidade no Render")
app.secret_key = os.environ.get("SECRET_KEY", "shopeebot2026")

# ── Banco de dados ───────────────────────────────────────
# Prioridade: 1) DATABASE_PATH env  2) /var/data (Render Disk)  3) ./data local
def _resolver_db_path():
    # Variável de ambiente explícita
    if os.environ.get("DATABASE_PATH"):
        p = os.environ["DATABASE_PATH"]
        os.makedirs(os.path.dirname(p), exist_ok=True)
        return p
    # Render Disk montado em /var/data
    if os.path.isdir("/var/data"):
        return "/var/data/bot2.db"
    # Fallback local
    d = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "bot2.db")

DB = _resolver_db_path()
_DATA_DIR = os.path.dirname(DB)
print(f"[DB] Usando banco: {DB}")
# Auto-recuperação: se banco corrompido, apaga e recria
try:
    _test_conn = sqlite3.connect(DB, timeout=5)
    _test_conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
    _test_conn.close()
except Exception as _db_err:
    print(f"[DB] Banco corrompido ({_db_err}), recriando...")
    try:
        import os as _os2
        _os2.remove(DB)
        # Remove WAL files também
        for _ext in ["-wal","-shm"]:
            try: _os2.remove(DB+_ext)
            except: pass
    except: pass
    print("[DB] Banco removido, será recriado no startup")

def get_db():
    c = sqlite3.connect(DB)
    c.row_factory = sqlite3.Row
    return c

def _limpar_banco():
    import os as _os_limpa
    for _f in [DB, DB+"-wal", DB+"-shm", DB+"-journal"]:
        try: _os_limpa.remove(_f); print(f"[DB] Removido: {_f}")
        except: pass

def init_db():
    _CREATE = """
        CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT, price REAL DEFAULT 0, commission REAL DEFAULT 0,
            image_url TEXT DEFAULT '', product_url TEXT DEFAULT '',
            affiliate_url TEXT DEFAULT '', channels TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            posted_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            level TEXT DEFAULT 'INFO', message TEXT,
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS external_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT DEFAULT '',
            name TEXT DEFAULT '',
            price REAL DEFAULT 0,
            old_price REAL DEFAULT 0,
            image_url TEXT DEFAULT '',
            product_url TEXT DEFAULT '',
            affiliate_url TEXT DEFAULT '',
            category TEXT DEFAULT '',
            brand TEXT DEFAULT '',
            coupon TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            priority INTEGER DEFAULT 0,
            status TEXT DEFAULT 'approved',
            is_active INTEGER DEFAULT 1,
            clicks INTEGER DEFAULT 0,
            last_posted TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
        CREATE TABLE IF NOT EXISTS external_clicks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            external_product_id INTEGER,
            source TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime'))
        );
    """
    for _tentativa in range(2):
        try:
            with get_db() as c:
                c.executescript(_CREATE)
                try:
                    c.execute("DELETE FROM config WHERE key LIKE '_img_%' AND key < '_img_' || CAST(strftime('%s','now','-2 hours') || '000' AS TEXT)")
                    c.execute("DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT 500)")
                    c.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                except Exception:
                    pass
                colunas_existentes = [
                    row[1] for row in c.execute("PRAGMA table_info(products)").fetchall()
                ]
                migracoes = [
                    ("item_id",   "ALTER TABLE products ADD COLUMN item_id TEXT DEFAULT ''"),
                    ("shop_id",   "ALTER TABLE products ADD COLUMN shop_id TEXT DEFAULT ''"),
                    ("shop_name", "ALTER TABLE products ADD COLUMN shop_name TEXT DEFAULT ''"),
                    ("rating",    "ALTER TABLE products ADD COLUMN rating REAL DEFAULT 0"),
                    ("sold",      "ALTER TABLE products ADD COLUMN sold INTEGER DEFAULT 0"),
                ]
                for coluna, sql in migracoes:
                    if coluna not in colunas_existentes:
                        try:
                            c.execute(sql)
                            print(f"[MIGRAÇÃO] Coluna '{coluna}' adicionada ✅")
                        except Exception as e:
                            print(f"[MIGRAÇÃO] Erro '{coluna}': {e}")
            break  # sucesso
        except sqlite3.DatabaseError as _dbe:
            print(f"[DB] CORROMPIDO tentativa {_tentativa+1}: {_dbe}")
            _limpar_banco()
            if _tentativa >= 1:
                raise

init_db()

# ── Agendador automático ─────────────────────────────────
def _agora_brasil():
    """Hora atual no fuso de Brasília (UTC-3)"""
    return datetime.now(TZ_BR)


def normalizar_horarios_agendamento(horarios_raw, fallback="08:00,12:00,18:00,21:00"):
    """Aceita horários em HH:MM, incluindo meia hora, e devolve lista ordenada sem duplicados."""
    import re as _re
    base = []
    for parte in str(horarios_raw or "").replace(";", ",").split(","):
        h = parte.strip()
        if not h:
            continue
        if _re.fullmatch(r"(?:[01]\d|2[0-3]):(?:00|30)", h):
            base.append(h)
    if not base:
        for parte in str(fallback).split(","):
            h = parte.strip()
            if h:
                base.append(h)
    return sorted(set(base), key=lambda x: tuple(map(int, x.split(":"))))


def gerar_grade_horarios(inicio=6, fim=23):
    """Retorna grade 06:00, 06:30 ... 23:30 para o painel."""
    horarios = []
    for h in range(inicio, fim + 1):
        horarios.append(f"{h:02d}:00")
        horarios.append(f"{h:02d}:30")
    return horarios

def verificar_agendamento():
    """Verifica se está na hora de postar (fuso Brasília)"""
    if cfg("auto_enabled", "false") != "true":
        return

    horarios_raw = cfg("auto_schedule", "07:00,07:30,08:00,08:30,09:00,09:30,10:00,10:30,11:00,11:30,12:00,12:30,13:00,13:30,14:00,14:30,15:00,15:30,16:00,16:30,17:00,17:30,18:00,18:30,19:00,19:30,20:00,20:30,21:00,21:30,22:00,22:30,23:00,23:30")
    horarios     = normalizar_horarios_agendamento(horarios_raw)
    agora_dt     = _agora_brasil()
    agora_hm     = agora_dt.strftime("%H:%M")
    chave        = agora_dt.strftime("%Y-%m-%d") + "_" + agora_hm
    ultimo       = cfg("last_auto_run", "")

    print(f"[AGENDADOR] hora_br={agora_hm} | horarios={horarios} | ultimo={ultimo}")

    if agora_hm not in horarios:
        return  # não é hora de postar

    if chave == ultimo:
        return  # já postou neste minuto

    cfg_set("last_auto_run", chave)
    log("INFO", f"▶ AGENDAMENTO DISPARADO às {agora_hm} (Brasília)")

    def run_ciclo():
        for tentativa in range(1, 4):
            try:
                resultado = executar_ciclo()
                if resultado and resultado > 0:
                    log("INFO", f"✅ Agendado OK: {resultado} produto(s) postado(s) | tentativa {tentativa}")
                    return
                log("WARN", f"Agendado: ciclo sem resultado — tentativa {tentativa}/3")
            except Exception as e:
                log("ERROR", f"Agendado erro tentativa {tentativa}: {str(e)[:100]}")
            if tentativa < 3:
                time.sleep(30)

    threading.Thread(target=run_ciclo, daemon=True).start()

def iniciar_agendador():
    """Loop de 1 em 1 minuto verificando horário de postagem (fuso Brasília)"""
    def loop():
        print("[AGENDADOR] ✅ Loop iniciado — verificando a cada 60s (Brasília)")
        while True:
            try:
                verificar_agendamento()
            except Exception as e:
                print(f"[AGENDADOR ERROR] {e}")
            time.sleep(60)

    threading.Thread(target=loop, daemon=True).start()
    print("[AGENDADOR] ✅ Thread iniciada")

# NOTA: iniciar_agendador() é chamado APÓS a definição de cfg() (mais abaixo)

# Mapa de variáveis de ambiente → chaves do banco
ENV_MAP = {
    "shopee_app_id":            "SHOPEE_APP_ID",
    "shopee_secret":            "SHOPEE_SECRET",
    "shopee_affiliate_id":      "SHOPEE_AFFILIATE_ID",
    "instagram_access_token":   "INSTAGRAM_TOKEN",
    "instagram_user_id":        "INSTAGRAM_USER_ID",
    "bot_password":             "BOT_PASSWORD",
    "telegram_token":           "TELEGRAM_TOKEN",
    "telegram_chat_id":         "TELEGRAM_CHAT_ID",
    "whatsapp_instance_id":     "WHATSAPP_INSTANCE",
    "whatsapp_token":           "WHATSAPP_TOKEN",
    "whatsapp_group_id":        "WHATSAPP_GROUP",
    "bot_url":                  "BOT_URL",
    "post_instagram":           "POST_INSTAGRAM",
    "post_telegram":            "POST_TELEGRAM",
    "post_whatsapp":            "POST_WHATSAPP",
    "wa_auto_ativo":            "WA_AUTO_ATIVO",
    "amazon_affiliate_tag":     "AMAZON_TAG",
    "ml_affiliate_id":          "ML_AFFILIATE_ID",
    "netshoes_affiliate_id":    "NETSHOES_AFFILIATE_ID",
    "netshoes_ativo":           "NETSHOES_ATIVO",
}

# Fallbacks hardcoded para credenciais críticas (nunca ficam vazias)
_CFG_DEFAULTS = {
    "shopee_app_id":          "18345690956",
    "shopee_secret":          "CSWN4EHO64ARF4LWQRKMSP22QFFMHQZH",
    "amazon_affiliate_tag":   "brizzah-20",
    "ml_affiliate_id":        "ad20260407202239",
    "netshoes_affiliate_id":  "4686648",
    "netshoes_ativo":         "true",
    "whatsapp_instance_id":   "brizzah-bot",
    "whatsapp_token":         "Brizzah@2025!",
    "whatsapp_group_id":      "120363407236556172@g.us",
    "post_whatsapp":          "true",
    "wa_auto_ativo":          "false",
}

def cfg(key, default=""):
    # 1️⃣ Variável de ambiente do Render (MÁXIMA PRIORIDADE — nunca some)
    env_key = ENV_MAP.get(key)
    if env_key:
        env_val = os.environ.get(env_key, "")
        if env_val:
            return env_val
    # 2️⃣ Default hardcoded (se for credencial crítica, usa antes do banco)
    if key in _CFG_DEFAULTS:
        return _CFG_DEFAULTS[key]
    # 3️⃣ Banco de dados
    try:
        with get_db() as c:
            r = c.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
            if r and r["value"]:
                return r["value"]
    except Exception:
        pass
    return default

def cfg_set(key, val):
    # Salva no banco E atualiza variável de ambiente em memória (sessão atual)
    with get_db() as c:
        c.execute("INSERT OR REPLACE INTO config(key,value) VALUES(?,?)", (key, str(val or "")))
    # Sincroniza na memória para uso imediato
    env_key = ENV_MAP.get(key)
    if env_key and val:
        os.environ[env_key] = str(val)

def log(level, msg):
    with get_db() as c:
        c.execute("INSERT INTO logs(level,message) VALUES(?,?)", (level, msg))
    print(f"[{level}] {msg}")


# ── Helpers globais seguros ─────────────────────────────────────
def _safe_float(v, default=0.0):
    """Converte preço/valor para float sem derrubar o app."""
    try:
        if v is None:
            return default
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return default
        s = s.replace("R$", "").replace(" ", "")
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            s = s.replace(",", ".")
        return float(s)
    except Exception:
        return default


def _safe_int(v, default=0):
    try:
        if v is None:
            return default
        s = str(v).strip().replace(".", "").replace(",", "")
        return int(float(s or default))
    except Exception:
        return default


def _fix_img(img):
    img = (img or "").strip()
    if not img:
        return ""
    if img.startswith("//"):
        return "https:" + img
    if not img.startswith("http") and len(img) > 20:
        return "https://cf.shopee.com.br/file/" + img
    return img


def _brz_norm(txt):
    """Normaliza texto para comparação sem acento e sem caracteres especiais."""
    try:
        import unicodedata, re
        s = str(txt or "").lower()
        s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s
    except Exception:
        return str(txt or "").lower().strip()


def _corrigir_portugues_produto(texto):
    """Corrige português, acentos e nomes vindos de slug/metatag."""
    try:
        import re as _re
        t = str(texto or "").strip()
        if not t:
            return ""

        t = t.replace("_", " ").replace("-", " ")
        t = _re.sub(r"\b_JM\b", "", t, flags=_re.I)
        t = _re.sub(r"\bP\d[A-Z]\b", "", t, flags=_re.I)
        t = _re.sub(r"\bSKU\b", "", t, flags=_re.I)
        t = _re.sub(r"\s+", " ", t).strip()

        regras = [
            (r"\b100\s*algod[oa]\b", "100% algodão"),
            (r"\balgodo\b", "algodão"),
            (r"\balgodao\b", "algodão"),
            (r"\btenis\b", "tênis"),
            (r"\btnis\b", "tênis"),
            (r"\bcalcado\b", "calçado"),
            (r"\bcalcados\b", "calçados"),
            (r"\balca\b", "alça"),
            (r"\bpeca\b", "peça"),
            (r"\bpecas\b", "peças"),
            (r"\brelogio\b", "relógio"),
            (r"\boculos\b", "óculos"),
            (r"\bpromocao\b", "promoção"),
            (r"\bpreco\b", "preço"),
            (r"\beletronico\b", "eletrônico"),
            (r"\beletronicos\b", "eletrônicos"),
            (r"\bdecoracao\b", "decoração"),
            (r"\borganizacao\b", "organização"),
            (r"\butilidades domesticas\b", "utilidades domésticas"),
            (r"\bconfortavel\b", "confortável"),
            (r"\bpre treino\b", "pré-treino"),
            (r"\bpos treino\b", "pós-treino"),
            (r"\bcaes\b", "cães"),
            (r"\bces\b", "cães"),
            (r"\bcao\b", "cão"),
            (r"\baco\b", "aço"),
            (r"\badnet\b", "Adnet"),
            (r"\badidas\b", "Adidas"),
            (r"\bpuma\b", "Puma"),
            (r"\bnike\b", "Nike"),
            (r"\bfila\b", "Fila"),
            (r"\bjbl\b", "JBL"),
            (r"\bxiaomi\b", "Xiaomi"),
        ]
        for pat, repl in regras:
            t = _re.sub(pat, repl, t, flags=_re.I)

        t = _re.sub(r"\b30\s+40\s+50\s*cm\b", "30, 40 ou 50 cm", t, flags=_re.I)
        t = _re.sub(r"\b3\s*stripes\b", "3-Stripes", t, flags=_re.I)
        t = _re.sub(r"\bessentials\b", "Essentials", t, flags=_re.I)
        t = _re.sub(r"\boriginal\s+nf\b", "original", t, flags=_re.I)
        t = _re.sub(r"\bcães e gatos cama luxo\b", "para cães e gatos", t, flags=_re.I)
        t = _re.sub(r"\bcães e gatos\b", "para cães e gatos", t, flags=_re.I)
        t = _re.sub(r"\s+", " ", t).strip()

        if t:
            t = t[0].upper() + t[1:]
        return t
    except Exception:
        return str(texto or "").strip()


def _nome_profissional_produto(nome):
    """Transforma nome bruto em nome natural com cara de loja grande."""
    n = _brz_norm(nome)
    nome_corrigido = _corrigir_portugues_produto(nome)

    if "toalha" in n:
        return "Jogo de toalhas de banho 100% algodão felpudas e macias"
    if "camiseta" in n and "adidas" in n and "infantil" in n:
        return "Camiseta Adidas Essentials infantil"
    if "camiseta" in n and "adidas" in n:
        return "Camiseta Adidas Essentials 3-Stripes em algodão"
    if "meia" in n and "puma" in n:
        return "Kit com 3 meias Puma infantil cano longo em algodão"
    if "chinelo" in n and "adidas" in n:
        return "Chinelo Adidas Flexmove preto confortável original"
    if "espelho" in n and "adnet" in n:
        return "Espelho Adnet redondo 30, 40 ou 50 cm com alça em couro decorativo"
    if ("caminha" in n or "cama pet" in n) and ("pet" in n or "gato" in n or "cao" in n or "caes" in n):
        return "Caminha pet redonda sherpa peludinha para cães e gatos"
    return nome_corrigido


def _headline_natural_produto(nome):
    """Chamada natural e vendedora com base no produto."""
    n = _brz_norm(nome)
    if "toalha" in n:
        return "TOALHAS MACIAS COM PREÇO QUE VALE A PENA"
    if "camiseta" in n or "camisa" in n:
        return "CAMISETA BOA PRA USAR MUITO PAGANDO POUCO"
    if "chinelo" in n:
        return "CONFORTO NO DIA A DIA SEM PAGAR CARO"
    if "tenis" in n:
        return "TÊNIS CONFORTÁVEL COM PREÇO DE OPORTUNIDADE"
    if "pet" in n or "caminha" in n or "gato" in n or "cachorro" in n or "cao" in n:
        return "CONFORTO PRO SEU PET COM PREÇO BAIXO"
    if "espelho" in n or "adnet" in n:
        return "UM TOQUE BONITO PRA CASA GASTANDO POUCO"
    if "meia" in n:
        return "BÁSICO QUE TODO MUNDO USA COM PREÇO BOM"
    if "fone" in n or "bluetooth" in n or "jbl" in n:
        return "ACHADO TECH COM PREÇO BOM"
    return "OFERTA BOA PRA APROVEITAR HOJE"


def _limpar_url_compartilhamento(url):
    """Remove pedaços de compartilhamento que atrapalham e preserva parâmetros essenciais."""
    try:
        import urllib.parse as _up
        url = (url or "").strip().replace("\u200b", "").replace("\n", "").replace("\r", "")
        if not url:
            return ""
        p = _up.urlparse(url)
        qs = dict(_up.parse_qsl(p.query, keep_blank_values=True))
        host = (p.netloc or "").lower()
        fragment = ""  # remove #origin=share, sid, action=copy etc.

        # Mercado Livre: manter matt_tool se já veio no link, limpar o restante.
        if "mercadolivre" in host:
            keep = {}
            if qs.get("matt_tool"):
                keep["matt_tool"] = qs.get("matt_tool")
            return _up.urlunparse((p.scheme, p.netloc, p.path, "", _up.urlencode(keep), fragment))

        # meli.la: é link curto oficial; preservar query e só remover fragmento.
        if "meli.la" in host:
            return _up.urlunparse((p.scheme, p.netloc, p.path, "", p.query, fragment))

        # Netshoes/Linksynergy: preservar query afiliada.
        if "linksynergy" in host or "netshoes" in host:
            return _up.urlunparse((p.scheme, p.netloc, p.path, "", p.query, fragment))

        # Amazon: manter tag de afiliado quando vier.
        if "amazon" in host:
            keep = {}
            if qs.get("tag"):
                keep["tag"] = qs.get("tag")
            return _up.urlunparse((p.scheme, p.netloc, p.path, "", _up.urlencode(keep), fragment))

        # amzn.to e Shopee: preservar query e limpar fragmento.
        if "amzn.to" in host or "shopee" in host:
            return _up.urlunparse((p.scheme, p.netloc, p.path, "", p.query, fragment))

        return _up.urlunparse((p.scheme, p.netloc, p.path, "", p.query, fragment))
    except Exception:
        return (url or "").strip().split("#", 1)[0]


def _html_unescape(txt):
    try:
        import html as _html
        return _html.unescape(str(txt or "")).strip()
    except Exception:
        return str(txt or "").strip()


def _limpar_nome_produto_ext(nome, source=""):
    import re as _re
    nome = _html_unescape(nome)
    nome = _re.sub(r"\s+", " ", nome).strip()
    sufixos = [
        "| Mercado Livre", "| Netshoes", "| Amazon.com.br", "Amazon.com.br",
        "- Netshoes", "- Mercado Livre", " | Amazon", " | MercadoLivre"
    ]
    for suf in sufixos:
        nome = nome.replace(suf, "")
    return nome.strip(" -|\n\t")[:140]


def _extract_jsonld_products(html):
    """Extrai blocos JSON-LD de Product/Offer quando a página disponibiliza."""
    import re as _re, json as _json
    products = []
    for m in _re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html or "", flags=_re.I|_re.S):
        raw = _html_unescape(m.group(1))
        try:
            data = _json.loads(raw)
        except Exception:
            continue
        queue = data if isinstance(data, list) else [data]
        for obj in queue:
            if not isinstance(obj, dict):
                continue
            if obj.get("@graph") and isinstance(obj.get("@graph"), list):
                queue.extend(obj.get("@graph"))
            typ = obj.get("@type")
            if isinstance(typ, list): typ = " ".join(map(str, typ))
            if typ and "Product" in str(typ):
                products.append(obj)
    return products


def _extrair_preco_texto(html):
    """Fallback genérico: busca padrões de preço no HTML."""
    import re as _re
    if not html:
        return 0
    pats = [
        r'"price"\s*:\s*"?([0-9]{1,6}(?:[\.,][0-9]{2})?)',
        r'"priceAmount"\s*:\s*"?([0-9]{1,6}(?:[\.,][0-9]{2})?)',
        r'"currentPrice"\s*:\s*\{[^}]*"value"\s*:\s*([0-9]{1,6}(?:[\.,][0-9]{2})?)',
        r'R\$\s*([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2})',
    ]
    candidatos = []
    for pat in pats:
        for m in _re.finditer(pat, html, flags=_re.I|_re.S):
            val = _safe_float(m.group(1), 0)
            if val and 5 <= val <= 50000:
                candidatos.append(val)
    if not candidatos:
        return 0
    # menor preço costuma ser o preço atual, evita pegar parcelas/simulações grandes demais
    return sorted(candidatos)[0]


def _extrair_preco_antigo_texto(html, preco_atual=0):
    import re as _re
    candidatos = []
    pats = [
        r'"listPrice"\s*:\s*([0-9]{1,6}(?:[\.,][0-9]{2})?)',
        r'"originalPrice"\s*:\s*([0-9]{1,6}(?:[\.,][0-9]{2})?)',
        r'"wasPrice"\s*:\s*"?([0-9]{1,6}(?:[\.,][0-9]{2})?)',
        r'preço anterior[^0-9]{0,40}R\$\s*([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2})',
        r'de\s*R\$\s*([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2})',
    ]
    for pat in pats:
        for m in _re.finditer(pat, html or "", flags=_re.I|_re.S):
            val = _safe_float(m.group(1), 0)
            if val and val > (preco_atual or 0):
                candidatos.append(val)
    return max(candidatos) if candidatos else 0


def _url_destino_e_nome_slug_externo(url):
    """Resolve URL real dentro de links afiliados/encurtados e extrai nome pelo slug."""
    import urllib.parse as _up, re as _re
    raw = (url or "").strip()
    if not raw:
        return "", ""
    parsed = _up.urlparse(raw)
    qs = _up.parse_qs(parsed.query)
    for key in ("murl", "url", "u", "redirect", "target"):
        val = qs.get(key, [""])[0]
        if val and val.startswith("http"):
            raw_dest = _up.unquote(val)
            break
    else:
        raw_dest = raw
    raw_dest = raw_dest.split("#")[0]
    p = _up.urlparse(raw_dest)
    path = _up.unquote(p.path or "")
    parts = [x for x in path.strip("/").split("/") if x]
    slug = parts[-1] if parts else ""
    if slug.lower() in ("p", "produto") and len(parts) >= 2:
        slug = parts[-2]
    slug = _re.sub(r"^(MLB-?\d+-)?", "", slug, flags=_re.I)
    slug = _re.sub(r"_JM$", "", slug, flags=_re.I)
    slug = _re.sub(r"-[A-Z0-9]{2,}(?:-[0-9A-Z]+)?$", "", slug)
    slug = _re.sub(r"[\-_+]+", " ", slug)
    slug = _re.sub(r"\s+", " ", slug).strip()
    slug = _re.sub(r"\b(p|produto|comprar)\b", "", slug, flags=_re.I).strip()
    if slug:
        slug = slug[:1].upper() + slug[1:]
        slug = _corrigir_portugues_produto(slug)
    return raw_dest, slug[:140]


def _nome_generico_externo(nome):
    n = (nome or "").strip().lower()
    return (not n) or n in {"produto importado", "produto em oferta", "oferta mercado livre", "oferta netshoes", "oferta amazon", "oferta shopee"}


def _nome_apresentavel_externo(p):
    nome = (p.get("name") or "").strip()
    if _nome_generico_externo(nome):
        source = (p.get("source") or "").lower()
        if source == "mercadolivre": return "Oferta selecionada no Mercado Livre"
        if source == "netshoes": return "Oferta selecionada na Netshoes"
        if source == "amazon": return "Oferta selecionada na Amazon"
        if source == "shopee": return "Oferta selecionada na Shopee"
        return "Oferta selecionada Brizzah"
    return nome


def extrair_dados_link_externo(url, source=""):
    """
    Importador PRO V2: tenta enriquecer links externos com nome, imagem e preço.
    Mercado Livre/Netshoes/Amazon podem bloquear partes do HTML; por isso sempre retorna fallback seguro.
    """
    import re as _re
    source = (source or "").lower().strip()
    url = _limpar_url_compartilhamento(url)
    url_destino, nome_slug = _url_destino_e_nome_slug_externo(url)
    nome_default = "Produto importado"
    if source == "mercadolivre": nome_default = "Oferta Mercado Livre"
    elif source == "netshoes": nome_default = "Oferta Netshoes"
    elif source == "amazon": nome_default = "Oferta Amazon"
    elif source == "shopee": nome_default = "Oferta Shopee"

    dados = {
        "name": nome_slug or nome_default, "price": 0, "old_price": 0, "image_url": "",
        "product_url": url_destino or url, "store_name": "", "coupon": "", "installments": ""
    }
    if not url:
        return dados

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Cache-Control": "no-cache",
        }
        request_url = url_destino or url
        r = requests.get(request_url, headers=headers, timeout=15, allow_redirects=True)
        html = r.text or ""
        final_url = r.url or url
        dados["product_url"] = final_url

        def _meta(prop):
            pats = [
                r'<meta[^>]+property=["\']' + _re.escape(prop) + r'["\'][^>]+content=["\']([^"\']+)',
                r'<meta[^>]+name=["\']' + _re.escape(prop) + r'["\'][^>]+content=["\']([^"\']+)',
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']' + _re.escape(prop) + r'["\']',
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']' + _re.escape(prop) + r'["\']',
            ]
            for pat in pats:
                m = _re.search(pat, html, flags=_re.I|_re.S)
                if m:
                    return _html_unescape(m.group(1))
            return ""

        # 1) JSON-LD Product é o melhor caminho quando existe.
        for prod in _extract_jsonld_products(html):
            if prod.get("name") and _nome_generico_externo(dados.get("name")):
                dados["name"] = _limpar_nome_produto_ext(prod.get("name"), source)
            img = prod.get("image")
            if isinstance(img, list): img = img[0] if img else ""
            if img and not dados["image_url"]:
                dados["image_url"] = _fix_img(str(img))
            offers = prod.get("offers") or {}
            if isinstance(offers, list): offers = offers[0] if offers else {}
            if isinstance(offers, dict):
                price = offers.get("price") or offers.get("lowPrice") or offers.get("highPrice")
                if price and not dados["price"]:
                    dados["price"] = _safe_float(price, 0)
                seller = offers.get("seller") or {}
                if isinstance(seller, dict) and seller.get("name"):
                    dados["store_name"] = seller.get("name")

        # 2) Metatags sociais.
        title = _meta("og:title") or _meta("twitter:title")
        if not title:
            m = _re.search(r"<title[^>]*>(.*?)</title>", html, flags=_re.I|_re.S)
            if m:
                title = _re.sub(r"\s+", " ", m.group(1)).strip()
        if title and _nome_generico_externo(dados.get("name")):
            dados["name"] = _limpar_nome_produto_ext(title, source)
        if title and not _nome_generico_externo(title) and len(str(title)) > len(str(dados.get("name", ""))) + 8:
            dados["name"] = _limpar_nome_produto_ext(title, source)

        img = _meta("og:image") or _meta("twitter:image") or _meta("image")
        if img and not dados["image_url"]:
            dados["image_url"] = _fix_img(img)

        # 3) Preços por metatag e HTML.
        price = _meta("product:price:amount") or _meta("og:price:amount") or _meta("twitter:data1")
        if not dados["price"]:
            dados["price"] = _safe_float(price, 0) or _extrair_preco_texto(html)
        dados["old_price"] = _extrair_preco_antigo_texto(html, dados["price"])
        # Brizzah PRO: nunca inventar preço antigo. Se o anúncio não informar, mantém 0.
        if not dados["old_price"]:
            dados["old_price"] = 0

        # 4) Cupom/parcelas: heurísticas leves.
        cupom = ""
        for pat in [r'Cupom\s*[:\-]?\s*</?[^>]*>?\s*([A-Z0-9]{4,20})', r'cupom\s+([A-Z0-9]{4,20})']:
            m = _re.search(pat, html, flags=_re.I|_re.S)
            if m:
                cupom = m.group(1).upper(); break
        dados["coupon"] = cupom
        parc = _re.search(r'([0-9]{1,2})x\s*(?:de\s*)?R\$\s*([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2})', html, flags=_re.I)
        if parc:
            dados["installments"] = f"{parc.group(1)}x R$ {parc.group(2)}"

        # 5) Loja/origem amigável.
        if source == "mercadolivre": dados["store_name"] = dados.get("store_name") or "Mercado Livre"
        elif source == "netshoes":
            dados["store_name"] = dados.get("store_name") or "Netshoes"
            dados["old_price"] = 0  # evita R$ 500/valores antigos falsos quando Netshoes bloqueia HTML
        elif source == "amazon": dados["store_name"] = dados.get("store_name") or "Amazon Brasil"
        elif source == "shopee": dados["store_name"] = dados.get("store_name") or "Shopee"

    except Exception as e:
        try:
            log("WARN", f"[EXT] não consegui enriquecer link: {str(e)[:80]}")
        except Exception:
            pass
    return dados

# ── Modo PRO 30 minutos: mantém agendamento de meia em meia hora ──
def _brz_aplicar_modo_30min():
    try:
        grade = "07:00,07:30,08:00,08:30,09:00,09:30,10:00,10:30,11:00,11:30,12:00,12:30,13:00,13:30,14:00,14:30,15:00,15:30,16:00,16:30,17:00,17:30,18:00,18:30,19:00,19:30,20:00,20:30,21:00,21:30,22:00,22:30,23:00,23:30"
        atual = cfg("auto_schedule", "")
        # Aplica automaticamente porque o usuário escolheu postagem a cada 30 minutos.
        if cfg("force_30min_schedule", "true") == "true" and atual != grade:
            cfg_set("auto_schedule", grade)
            log("INFO", "[PRO] Agenda ajustada para postagem a cada 30 minutos")
        if cfg("products_per_cycle", "1") != "1":
            cfg_set("products_per_cycle", "1")
    except Exception as e:
        try: log("WARN", f"[PRO] Falha ao aplicar agenda 30min: {str(e)[:80]}")
        except Exception: pass

_brz_aplicar_modo_30min()

# ── Inicia agendador AQUI — após cfg(), cfg_set() e log() estarem definidos ──
# Em Render com múltiplos workers, mantenha WEB_CONCURRENCY=1.
# Para desligar o agendador deste processo, use BRIZZAH_DISABLE_SCHEDULER=true.
if os.environ.get("BRIZZAH_DISABLE_SCHEDULER", "false").lower() != "true":
    iniciar_agendador()
else:
    print("[AGENDADOR] Desativado por BRIZZAH_DISABLE_SCHEDULER=true")

def get_stats():
    today = datetime.now().strftime("%Y-%m-%d")
    with get_db() as c:
        t = c.execute("SELECT COUNT(*) n FROM products WHERE posted_at LIKE ? AND status='success'", (today+"%",)).fetchone()["n"]
        w = c.execute("SELECT COUNT(*) n FROM products WHERE posted_at >= date('now','-7 days') AND status='success'").fetchone()["n"]
        total = c.execute("SELECT COUNT(*) n FROM products WHERE status='success'").fetchone()["n"]
    return {"today": t, "week": w, "total": total}

def login_required(f):
    from functools import wraps
    @wraps(f)
    def dec(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return dec


def buscar_external_products_aprovados(limit=30):
    with get_db() as c:
        rows = c.execute("""
            SELECT * FROM external_products
            WHERE status='approved' AND is_active=1
            ORDER BY priority DESC, id DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def bonus_clicks_external(prod_id):
    with get_db() as c:
        r = c.execute("SELECT COUNT(*) n FROM external_clicks WHERE external_product_id=?", (prod_id,)).fetchone()
    return int(r["n"] or 0) if r else 0


def ja_postado_recentemente_externo(prod_id, horas=6):
    with get_db() as c:
        r = c.execute("SELECT last_posted FROM external_products WHERE id=?", (prod_id,)).fetchone()
    if not r or not r["last_posted"]:
        return False
    try:
        from datetime import timedelta
        last = datetime.fromisoformat(r["last_posted"])
        return datetime.now() - last < timedelta(hours=horas)
    except Exception:
        return False


def score_externo(p):
    s = 0
    nome = (p.get("name") or "").lower()
    preco = float(p.get("price", 0) or 0)
    antigo = float(p.get("old_price", 0) or 0)
    prioridade = int(p.get("priority", 0) or 0)
    source = (p.get("source") or "").lower()
    s += prioridade
    if antigo > preco and preco > 0:
        desconto = int((1 - preco / antigo) * 100)
        if desconto >= 40: s += 4
        elif desconto >= 30: s += 3
        elif desconto >= 20: s += 2
    if preco <= 199: s += 2
    if source == "mercadolivre": s += 3
    elif source == "netshoes": s += 2
    elif source == "amazon": s += 1
    marcas_fortes = ["nike", "adidas", "puma", "fila", "mizuno", "new balance", "lattafa", "jbl", "xiaomi"]
    if any(m in nome for m in marcas_fortes):
        s += 4
    s += min(bonus_clicks_external(p.get("id", 0)), 5)
    return s


def ajustar_link_afiliado_externo(source, url):
    source = (source or "").lower().strip()
    url = (url or "").strip()
    if not url:
        return ""
    if source == "amazon":
        tag = cfg("amazon_affiliate_tag", "")
        if tag and "tag=" not in url:
            sep = "&" if "?" in url else "?"
            return f"{url}{sep}tag={tag}"
    return url


def _fonte_emoji_externo(source):
    source = (source or "").lower()
    if source == "mercadolivre":
        return "🟡", "Mercado Livre"
    if source == "netshoes":
        return "👟", "Netshoes"
    if source == "amazon":
        return "📦", "Amazon Brasil"
    if source == "shopee":
        return "🟠", "Shopee"
    return "🛍️", "Produto"


def _headline_promo_externo(p):
    """Manchetes curtas estilo grupo de promoções, sem depender de IA externa."""
    import random as _random
    nome = (p.get("name") or "").lower()
    source = (p.get("source") or "").lower()

    if any(w in nome for w in ["nike", "adidas", "puma", "fila", "olympikus", "kappa", "mizuno", "new balance"]):
        opts = [
            "CORRE QUE ESSE TÊNIS TÁ COM PREÇO FORTE",
            "PRA SAIR NO ESTILO SEM PAGAR CARO",
            "PREÇO DE OPORTUNIDADE NESSE CALÇADO",
            "ACHADO FORTE PRA QUEM CURTE MARCA BOA",
        ]
    elif any(w in nome for w in ["perfume", "lattafa", "asad", "armani", "colônia", "colonia", "la vie", "oud"]):
        opts = [
            "CHEIRO DE QUEM GARIMPA OFERTA BOA",
            "PERFUME COM PREÇO PRA APROVEITAR",
            "ACHADO PERFUMADO DO DIA",
            "PRA FICAR CHEIROSO SEM SOFRER NO CARTÃO",
        ]
    elif any(w in nome for w in ["creatina", "whey", "suplemento", "protein"]):
        opts = [
            "PRA REFORÇAR O PROJETO FITNESS",
            "SUPLEMENTO COM PREÇO DE GUERRA",
            "ACHADO PRA QUEM TREINA DE VERDADE",
        ]
    elif any(w in nome for w in ["fone", "jbl", "smartwatch", "relógio", "relogio", "xiaomi", "caixa de som"]):
        opts = [
            "GADGET COM PREÇO BOM DEMAIS",
            "TECH BARATA PRA GARANTIR HOJE",
            "ACHADO ELETRÔNICO PRA APROVEITAR",
        ]
    elif any(w in nome for w in ["cadeira", "escritório", "escritorio", "home office"]):
        opts = [
            "SEU HOME OFFICE SEM DOR NA COLUNA",
            "CADEIRA BOA COM PREÇO MELHOR AINDA",
            "ACHADO PRA TRABALHAR MAIS CONFORTÁVEL",
        ]
    elif any(w in nome for w in ["mochila", "bolsa"]):
        opts = [
            "CABE MUITA COISA E O PREÇO AJUDA",
            "MOCHILA PRA USAR TODO DIA",
            "ACHADO PRA QUEM CARREGA O MUNDO NAS COSTAS",
        ]
    elif any(w in nome for w in ["chinelo", "havaianas"]):
        opts = [
            "CHINELO DE RESPEITO PRO DIA A DIA",
            "PREÇO BOM PRA GARANTIR O CONFORTO",
            "ACHADO SIMPLES QUE TODO MUNDO USA",
        ]
    else:
        opts = [
            "OFERTA BOA PRA APROVEITAR HOJE",
            "ACHADO FORTE DO DIA",
            "PREÇO BOM DEMAIS PRA DEIXAR PASSAR",
            "GARIMPO BRIZZAH PRA ECONOMIZAR",
        ]
    if source == "netshoes":
        opts.append("NETSHOES COM PREÇO DE GARIMPO")
    if source == "mercadolivre":
        opts.append("ACHADO FORTE NO MERCADO LIVRE")
    return _random.choice(opts)


def _format_brl(v):
    v = _safe_float(v, 0)
    if not v:
        return ""
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _caption_wa_externo(p):
    emoji, fonte = _fonte_emoji_externo(p.get("source"))
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p))[:115]
    preco = _safe_float(p.get("price"), 0)
    orig = _safe_float(p.get("old_price"), 0)
    # Brizzah PRO: não inventa preço antigo
    orig = _brz_preco_antigo_confiavel(preco, orig, p.get("source"))
    desc = int((1 - preco / orig) * 100) if orig and preco and orig > preco else 0
    link = p.get("affiliate_url") or p.get("product_url") or ""
    cupom = (p.get("coupon") or "").strip()
    installments = (p.get("installments") or "").strip()

    linhas = [f"*{_headline_promo_externo(p)}*", "", nome, ""]
    if preco:
        if orig and orig > preco:
            linhas.append(f"de {_format_brl(orig)} por *{_format_brl(preco)}* 👊")
            if desc >= 10:
                linhas.append(f"🔥 {desc}% OFF")
        else:
            linhas.append(f"por *{_format_brl(preco)}* 👊")
        if installments:
            linhas.append(f"💳 {installments}")
    else:
        linhas.append("💰 Confira o preço atualizado no link 👇")

    if cupom:
        linhas.append(f"Cupom: *{cupom}* ⚠️")

    linhas += ["", f"{emoji} Origem: *{fonte}*", f"🔗 {link}", "", "_Brizzah | Achados inteligentes_ 🔥"]
    return "\n".join(linhas)


def _caption_ig_externo(p):
    emoji, fonte = _fonte_emoji_externo(p.get("source"))
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p))[:110]
    preco = _safe_float(p.get("price"), 0)
    orig = _safe_float(p.get("old_price"), 0)
    # Brizzah PRO: não inventa preço antigo
    orig = _brz_preco_antigo_confiavel(preco, orig, p.get("source"))
    desc = int((1 - preco / orig) * 100) if orig and preco and orig > preco else 0
    link = p.get("affiliate_url") or p.get("product_url") or ""

    linhas = [f"{emoji} {_headline_promo_externo(p)}", "", nome, ""]
    if preco:
        linhas.append(f"De {_format_brl(orig)} por {_format_brl(preco)} 🔥 {desc}% OFF" if desc >= 10 else f"Preço de hoje: {_format_brl(preco)}")
    else:
        linhas.append("Confira o preço atualizado no link 👇")
    linhas += ["", f"Origem: {fonte}", "Link na bio ou direto no grupo VIP 👆", link[:120], "", "#achadinhos #ofertas #brizzah #promocao"]
    return "\n".join(linhas)

# ════════════════════════════════════════════════════════
#  HASHTAGS AUTOMÁTICAS POR PRODUTO / NICHO
# ════════════════════════════════════════════════════════
HASHTAG_MAP = {
    # ── Beleza & Skincare ──
    "maquiagem":   "#maquiagem #makeup #beleza #makeover #maquiagembrasileira #maquiagemdodia #tutorialmaquiagem #brizzah #shopee #achadinhos #desconto #oferta",
    "batom":       "#batom #lipstick #lips #maquiagem #gloss #beleza #labbonitos #makeupbrasil #brizzah #shopee #achadinhos",
    "base":        "#base #maquiagem #peleperfeita #makeup #fundacao #coberturatotal #skincareroutine #brizzah #shopee #achadinhos",
    "skincare":    "#skincare #peleperfeita #cuidadosdapele #serumfacial #hidratante #rotinadebeleza #glowingskin #brizzah #shopee #oferta",
    "serum":       "#serum #skincare #peleperfeita #antiidade #vitamina #rotinadebeleza #brizzah #shopee #achadinhos #desconto",
    "perfume":     "#perfume #fragancia #cheirosa #perfumaria #perfumeimportado #parfum #fragrancecommunity #brizzah #shopee #desconto",
    "cabelo":      "#cabelo #haircare #cabelosbonitos #tratamentocapilar #cabelosaudavel #hairtransformation #brizzah #shopee #achadinhos",
    "hidratante":  "#hidratante #skincare #peleperfeita #cuidadosdapele #beleza #bodyskincare #brizzah #shopee #desconto",
    "protetor":    "#protetorsolar #skincare #peleperfeita #fps #solarbrasil #brizzah #shopee #achadinhos",
    "niacinamida": "#niacinamida #skincare #peleperfeita #cuidadosdapele #porominimizer #brizzah #shopee #desconto",
    # ── Moda Feminina ──
    "vestido":     "#vestido #moda #modafeminina #looks #ootd #estilo #fashion #lookdodia #tendencia #brizzah #shopee #desconto",
    "blusa":       "#blusa #moda #modafeminina #looks #estilo #roupa #lookdodia #brizzah #shopee #achadinhos",
    "calca":       "#calca #calcajeans #moda #modafeminina #looks #jeans #denim #ootd #brizzah #shopee #desconto",
    "conjunto":    "#conjunto #moda #modafeminina #look #ootd #twinset #lookdodia #brizzah #shopee #achadinhos",
    "roupa":       "#moda #modafeminina #looks #ootd #estilo #fashion #roupa #lookdodia #brizzah #shopee #achadinhos",
    "moletom":     "#moletom #moda #casual #streetwear #conforto #looks #cozywear #brizzah #shopee #achadinhos",
    "camiseta":    "#camiseta #tshirt #moda #casual #streetwear #lookdodia #brizzah #shopee #achadinhos #desconto",
    "cropped":     "#cropped #moda #modafeminina #fitness #lookdodia #ootd #brizzah #shopee #achadinhos",
    "saia":        "#saia #moda #modafeminina #looks #feminina #ootd #brizzah #shopee #achadinhos #desconto",
    # ── Moda Masculina ──
    "masculina":   "#moda #modamasculina #mensfashion #lookmasculino #estilo #ootd #brizzah #shopee #desconto",
    "camisa":      "#camisa #modamasculina #social #trabalho #lookdodia #brizzah #shopee #achadinhos",
    "bermuda":     "#bermuda #modamasculina #casual #verao #praia #brizzah #shopee #desconto",
    # ── Calçados ──
    "tenis":       "#tenis #calcados #sneakers #shoes #sneakerhead #snkrs #modafeminina #brizzah #shopee #desconto",
    "sapato":      "#sapato #calcados #shoes #moda #modafeminina #heels #brizzah #shopee #achadinhos",
    "sandalia":    "#sandalia #calcados #shoes #verao #praia #modafeminina #brizzah #shopee #achadinhos",
    "chinelo":     "#chinelo #slides #calcados #verao #casual #conforto #brizzah #shopee #desconto",
    "bota":        "#bota #boots #calcados #moda #inverno #fashion #brizzah #shopee #achadinhos",
    # ── Tech & Gadgets ──
    "celular":     "#celular #smartphone #tecnologia #tech #gadget #iphone #android #brizzah #shopee #oferta",
    "fone":        "#fone #headphone #bluetooth #earbuds #musica #audio #tecnologia #brizzah #shopee #desconto",
    "smartwatch":  "#smartwatch #relogiointeligente #wearable #tecnologia #gadget #brizzah #shopee #oferta",
    "carregador":  "#carregador #techcessorios #celular #gadget #tecnologia #brizzah #shopee #achadinhos",
    "notebook":    "#notebook #computador #laptop #tecnologia #trabalho #homeoffice #brizzah #shopee #oferta",
    "fones":       "#fones #bluetooth #musica #audio #podcast #brizzah #shopee #desconto",
    "cabo":        "#cabo #tecnologia #acessorio #celular #gadget #brizzah #shopee #achadinhos",
    # ── Casa & Decoração ──
    "airfryer":    "#airfryer #fritadeirasemoleio #cozinha #receitassaudaveis #foodie #brizzah #shopee #desconto",
    "panela":      "#panela #cozinha #utilidades #culinaria #receitas #chefemcasa #brizzah #shopee #achadinhos",
    "organizador": "#organizacao #casa #homedecor #organizado #minimalismo #brizzah #shopee #desconto",
    "decoracao":   "#decoracao #homedecor #casaperfeita #interiores #designdeinteriores #brizzah #shopee #desconto",
    "tapete":      "#tapete #decoracao #casa #homedecor #sala #quarto #brizzah #shopee #achadinhos",
    "cozinha":     "#cozinha #utilidadesdomesticas #casa #culinaria #chefcaseiro #brizzah #shopee #oferta",
    "luminaria":   "#luminaria #decoracao #casa #homedecor #iluminacao #brizzah #shopee #achadinhos",
    "prateleira":  "#prateleira #organizacao #decoracao #homedecor #brizzah #shopee #desconto",
    # ── Fitness & Saúde ──
    "legging":     "#legging #fitness #academia #treino #workout #gymwear #ativefashion #brizzah #shopee #desconto",
    "fitness":     "#fitness #academia #treino #saudavel #workout #gym #bodybuilding #brizzah #shopee #desconto",
    "suplemento":  "#suplemento #proteina #wheyprotein #fitness #academia #saude #nutricao #brizzah #shopee",
    "garrafa":     "#garrafatermica #fitness #hidratacao #academia #workout #brizzah #shopee #achadinhos",
    "yoga":        "#yoga #pilates #fitness #bemestar #meditacao #brizzah #shopee #desconto",
    "colchao":     "#colchao #sono #qualidadedevida #descanso #brizzah #shopee #oferta",
    # ── Pet ──
    "cachorro":    "#cachorro #pet #petlovers #dogs #dogsofinstagram #petshop #brizzah #shopee #achadinhos",
    "gato":        "#gato #cat #pet #petlovers #cats #catsofinstagram #brizzah #shopee #achadinhos",
    "pet":         "#pet #petlovers #animaisdeestimacao #petshop #brizzah #shopee #achadinhos #desconto",
    "racao":       "#racao #pet #cachorro #gato #petlovers #petshop #brizzah #shopee #desconto",
    # ── Infantil ──
    "brinquedo":   "#brinquedo #infantil #kids #criancas #presente #brinquedos #brizzah #shopee #desconto",
    "escolar":     "#voltaasaulas #papelaria #escolar #kids #escola #brizzah #shopee #achadinhos",
    "bebe":        "#bebe #maternidade #mamaeblogger #enxovaldebebe #brizzah #shopee #desconto",
    # ── Acessórios ──
    "bolsa":       "#bolsa #moda #acessorios #fashion #handbag #brizzah #shopee #achadinhos #desconto",
    "relogio":     "#relogio #acessorios #moda #estilo #watches #brizzah #shopee #desconto",
    "oculos":      "#oculos #acessorios #sunglasses #moda #estilo #brizzah #shopee #achadinhos",
    "mochila":     "#mochila #moda #trabalho #escola #viagem #brizzah #shopee #desconto",
    "cinto":       "#cinto #acessorios #moda #look #estilo #brizzah #shopee #achadinhos",
    # ── Outros ──
    "kit":         "#kit #presente #oferta #shopee #brizzah #achadinhos #desconto #combo",
    "livro":       "#livro #leitura #literatura #books #bookstagram #brizzah #shopee #desconto",
    "viagem":      "#viagem #travel #mochilao #ferias #brizzah #shopee #achadinhos",
    "natal":       "#natal #presente #christmas #festadenatal #brizzah #shopee #desconto",
    "default":     "#achadinhos #shopee #ofertadodia #desconto #brizzah #comprasshopee #promocao #achado #economize #oferta #melhorpreco"
}

def gerar_hashtags(nome_produto, keyword_nicho=""):
    """Gera hashtags inteligentes baseadas NO PRODUTO — nunca no nicho fixo configurado."""
    import re as _re
    # USA APENAS O NOME DO PRODUTO para detectar categoria — ignora keyword_nicho
    # Isso evita que o nicho fixo "maquiagem" apareça em todos os posts
    texto = nome_produto.lower()
    # Remove acentos para facilitar match
    import unicodedata
    texto_norm = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    # Busca todas as chaves que fazem match
    matches = []
    for chave, tags in HASHTAG_MAP.items():
        if chave == 'default': continue
        chave_norm = ''.join(c for c in unicodedata.normalize('NFD', chave) if unicodedata.category(c) != 'Mn')
        if chave_norm in texto_norm:
            matches.append(tags)
            if len(matches) >= 2: break
    if matches:
        return matches[0]
    # Fallback: gera hashtags a partir de palavras do produto
    palavras = _re.findall(r'\b[a-z]{4,}\b', texto_norm)
    base = "#achadinhos #shopee #ofertadodia #desconto #brizzah"
    extras = " ".join([f"#{p}" for p in palavras[:4] if p not in ('para','este','essa','voce','mais','muito','todos','todas','cada','pelo','pela')])
    return f"{extras} {base}".strip()

def adicionar_opiniao(produto):
    """Frase humanizada que aumenta conversão — varia por categoria."""
    import random as _r
    nome = (produto.get("name","") or "").lower()
    preco = float(produto.get("price",0) or 0)
    sold  = int(produto.get("sold",0) or 0)

    # Frases por categoria
    if any(k in nome for k in ["skincare","creme","serum","hidratante","perfume","beleza"]):
        frases = [
            "esse aqui é favorito das meninas 💆‍♀️",
            "qualidade surpreendente pelo preço 🌸",
            "eu compraria fácil esse produto ✨",
            "tô vendo muita gente amando esse ❤️",
        ]
    elif any(k in nome for k in ["fone","smartwatch","celular","bluetooth","carregador","cabo"]):
        frases = [
            "tech de qualidade por esse preço é raro 📱",
            "esse gadget tá fazendo sucesso demais 🔥",
            "vale cada centavo, sério 💯",
            "tá absurdo de barato pra qualidade que entrega ⚡",
        ]
    elif any(k in nome for k in ["vestido","blusa","calca","camiseta","conjunto","moda"]):
        frases = [
            "look completo por esse valor? tá de graça 👗",
            "tô vendo muita gente arrasando com esse 🔥",
            "esse aqui vale muito a pena 👀",
            "qualidade incrível pelo preço que tá 💕",
        ]
    elif any(k in nome for k in ["airfryer","panela","cozinha","organizador","casa"]):
        frases = [
            "esse produto mudou minha rotina em casa 🏠",
            "compra indispensável pra casa 🍳",
            "tá vendendo muito por um motivo: funciona 👌",
            "economia real no dia a dia 💰",
        ]
    elif sold > 500:
        frases = [
            f"já são {sold:,}+ pessoas aprovando esse produto 👏".replace(",","."),
            "produto validado por muita gente — não é à toa 🏆",
            "best-seller com motivo: qualidade real ⭐",
        ]
    else:
        frases = [
            "esse aqui vale MUITO a pena 👀",
            "eu compraria fácil esse produto",
            "esse tá absurdo de barato",
            "qualidade surpreendente pelo preço",
            "corre que ainda tem em estoque! 🔥",
            "tô recomendando demais esse achado 💯",
        ]
    return _r.choice(frases)


def gerar_descricao_inteligente(produto):
    """
    Caption máxima conversão — preço em destaque, desconto, urgência, CTA bio.
    Zero comissão. Linguagem brasileira natural e atrativa.
    Suporte a datas comemorativas e contexto temporal.
    """
    import random

    # Lê contexto temporal injetado pelo ciclo (se disponível)
    data_especial = produto.get("_data_especial", "")
    periodo       = produto.get("_periodo", "")
    try:
        with get_db() as _c:
            _de = _c.execute("SELECT value FROM config WHERE key='_data_especial'").fetchone()
            if _de and not data_especial: data_especial = _de["value"] or ""
    except Exception:
        pass

    nome      = produto.get("name", "")
    preco     = produto.get("price", 0)
    estrelas  = produto.get("rating", 0)
    vendidos  = produto.get("sold", 0)
    _src_emoji, _src_nome = _fonte_emoji(produto)
    loja      = produto.get("shop_name","") or _src_nome
    keyword   = cfg("niche_keyword", "")
    nome_lower = nome.lower()
    preco_f    = float(preco)

    # ── Categoria ──────────────────────────────────────────────
    def _cat(keys):
        return any(k in nome_lower for k in keys)

    if _cat(["maquiagem","batom","perfume","skincare","cabelo","hidratante","creme","serum","blush","base","sombra","rímel","protetor","vitamina c"]):
        categoria = "beleza"
    elif _cat(["airfryer","air fryer","panela","frigideira","chaleira","coador","mixer","batedeira","cozinha"]):
        categoria = "cozinha"
    elif _cat(["casa","organiza","limpeza","tapete","vaso","decoração","prateleira","cabide"]):
        categoria = "casa"
    elif _cat(["roupa","vestido","camiseta","calça","blusa","saia","conjunto","bermuda","moletom"]):
        categoria = "moda"
    elif _cat(["tenis","tênis","sapato","sandália","chinelo","bota","rasteirinha"]):
        categoria = "calcados"
    elif _cat(["celular","fone","cabo","carregador","notebook","tablet","smartwatch","headset","câmera"]):
        categoria = "tech"
    elif _cat(["pet","cachorro","gato","cão","ração","coleira"]):
        categoria = "pet"
    elif _cat(["suplemento","proteína","whey","colágeno","vitamina","creatina","bcaa"]):
        categoria = "saude"
    else:
        categoria = "geral"

    # ── Desconto e preços ──────────────────────────────────────
    desconto_pct = int(produto.get("discount_pct", 0) or 0)
    if not desconto_pct:
        if preco_f < 30:    desconto_pct = random.choice([35, 40, 45])
        elif preco_f < 80:  desconto_pct = random.choice([30, 35, 40])
        elif preco_f < 200: desconto_pct = random.choice([25, 30, 35])
        else:               desconto_pct = random.choice([20, 25, 30])

    preco_original = round(preco_f / (1 - desconto_pct / 100), 2)
    economia       = round(preco_original - preco_f, 2)
    preco_str      = f"R$ {preco_f:.2f}".replace(".", ",")
    original_str   = f"R$ {preco_original:.2f}".replace(".", ",")
    economia_str   = f"R$ {economia:.2f}".replace(".", ",")

    # ── HOOK — 1ª linha que para o scroll ─────────────────────
    hooks = {
        "beleza": [
            f"Eu nao deveria estar contando isso, mas esse produto de beleza esta com {desconto_pct}% OFF agora",
            f"Esse era o segredo das influencers e agora ta por apenas {preco_str} na Shopee",
            f"Vi esse produto de beleza por {preco_str} e nao consegui segurar",
            f"Quem disse que pele bonita custa caro? Esse aqui ta com {desconto_pct}% de desconto",
            f"As meninas que eu sigo tao loucas nesse produto de beleza — e eu entendo",
            f"Beleza sem gastar muito? Esse aqui prova que e possivel",
        ],
        "tech": [
            f"Esse gadget que todo mundo quer caiu {desconto_pct}% hoje na Shopee",
            f"Tecnologia de primeira por {preco_str}? Sim, e verdade",
            f"O produto tech mais desejado do momento com {desconto_pct}% OFF",
            f"Voce vai economizar {economia_str} comprando esse produto hoje",
        ],
        "moda": [
            f"Esse look saiu do armario das influencers e chegou na Shopee por {preco_str}",
            f"Ninguem precisa saber que voce pagou apenas {preco_str} nessa peca",
            f"Moda de qualidade com {desconto_pct}% OFF — so hoje na Shopee",
            f"Essa peca que vi por {preco_str} e fiquei impressionada com a qualidade",
        ],
        "calcados": [
            f"Esse calçado que parece caro e custa apenas {preco_str}",
            f"Conforto e estilo com {desconto_pct}% de desconto real na Shopee",
            f"Frete gratis e {desconto_pct}% OFF nesse calçado incrivel",
        ],
        "cozinha": [
            f"Esse eletrodomestico que facilita sua vida ta com {desconto_pct}% OFF agora",
            f"Transforme sua cozinha gastando apenas {preco_str} nesse achado",
            f"O produto de cozinha mais vendido com desconto absurdo de {desconto_pct}%",
        ],
        "casa": [
            f"Deixei minha casa linda gastando apenas {preco_str} nesse achado da Shopee",
            f"Produto de casa com {desconto_pct}% OFF — aproveita que ta acabando",
            f"Esse item que toda casa precisa ta por {preco_str} com frete gratis",
        ],
        "saude": [
            f"O suplemento que profissionais recomendam ta com {desconto_pct}% OFF",
            f"Cuide da sua saude sem pesar no bolso: {preco_str} com desconto real",
            f"Esse produto de saude ta {desconto_pct}% mais barato e nao vai durar",
        ],
        "pet": [
            f"Seu pet merece o melhor e ta com {desconto_pct}% de desconto agora",
            f"Esse achado pet por {preco_str} e impossivel resistir",
        ],
        "geral": [
            f"Encontrei esse produto por {preco_str} e precisei compartilhar com voces",
            f"Nao consigo acreditar nesse desconto de {desconto_pct}% — corre ver",
            f"De {original_str} por {preco_str} — economia real de {economia_str}",
            f"Esse achado da Shopee ta com {desconto_pct}% OFF e pouca gente sabe",
        ],
    }
    hook = random.choice(hooks.get(categoria, hooks["geral"]))

    # ── Prova social ───────────────────────────────────────────
    prova = []
    if estrelas and float(estrelas) >= 4.5:
        s = float(estrelas)
        estrelas_emoji = "⭐" * min(int(round(s)), 5)
        prova.append(f"{estrelas_emoji} Avaliacao {s:.1f}/5 pelos compradores")
    if vendidos and int(vendidos) > 0:
        v = int(vendidos)
        v_txt = f"{v/1000:.1f}mil".replace(".", ",") if v >= 1000 else str(v)
        prova.append(f"Mais de {v_txt} unidades vendidas — produto aprovado!")

    # ── Benefícios por categoria ───────────────────────────────
    beneficios_cat = {
        "beleza":   ["Resultado visivel desde a primeira semana", "Produto original com nota fiscal"],
        "tech":     ["Garantia do fabricante inclusa", "Compativel com Android e iPhone"],
        "moda":     ["Tecido de qualidade e confortavel", "Disponivel em varios tamanhos e cores"],
        "calcados": ["Solado antiderrapante e super confortavel", "Numeracao variada disponivel"],
        "cozinha":  ["Pratico, facil de usar e de limpar", "Economiza tempo e energia na cozinha"],
        "casa":     ["Facil instalacao sem precisar de tecnico", "Transforma qualquer ambiente"],
        "saude":    ["Formula desenvolvida por especialistas", "Sem contraindicacoes para uso diario"],
        "pet":      ["Seguro e aprovado por veterinarios", "Material resistente e duravel"],
        "geral":    ["Produto original com qualidade garantida", "Entrega rapida para todo Brasil"],
    }
    beneficio = random.choice(beneficios_cat.get(categoria, beneficios_cat["geral"]))

    # ── Urgência ───────────────────────────────────────────────
    urgencias = [
        f"Estoque limitado — essa oferta de {desconto_pct}% OFF pode acabar a qualquer hora",
        f"Preco promocional por tempo limitado — nao deixa pra amanha",
        f"Muita gente ja adicionou no carrinho — corre antes de esgotar",
        f"Essa promocao de {desconto_pct}% OFF nao vai durar a semana toda",
        f"Ultima chance de economizar {economia_str} nesse produto incrivel",
    ]
    urgencia = random.choice(urgencias)

    # ── Hashtags ───────────────────────────────────────────────
    # Hashtags pelo nome real do produto, nunca pelo nicho fixo
    hashtags = gerar_hashtags(nome, "")

    # ══════════════════════════════════════════════════════════
    #  MONTA O CAPTION COMPLETO
    # ══════════════════════════════════════════════════════════
    p = []

    # 0. Banner de data comemorativa (quando aplicável)
    BANNER_DATA = {
        "Natal":             "🎄 ESPECIAL DE NATAL — presentes com ate 80% OFF!",
        "Natal prep":        "🎄 Ja comecou o clima de Natal — ofertas especiais!",
        "Dia das Maes":      "💐 ESPECIAL DIA DAS MAES — o melhor presente com desconto!",
        "Dia dos Namorados": "❤️ ESPECIAL DIA DOS NAMORADOS — surpreenda com esse achado!",
        "Dia dos Namorados BR": "❤️ ESPECIAL DIA DOS NAMORADOS — surpreenda com esse achado!",
        "Black Friday":      "🖤 BLACK FRIDAY CHEGOU — desconto real, nao e golpe!",
        "Dia das Criancas":  "🎈 ESPECIAL DIA DAS CRIANCAS — presente perfeito com desconto!",
        "Dia dos Pais":      "👔 ESPECIAL DIA DOS PAIS — o presente que ele vai amar!",
        "Dia da Mulher":     "🌸 ESPECIAL DIA DA MULHER — voce merece esse presente!",
        "Reveillon":         "🎉 ESPECIAL REVEILLON — entra o ano novo com estilo!",
        "Ano Novo":          "🥂 FELIZ ANO NOVO — comece 2025 economizando muito!",
        "Ferias Julho":      "☀️ FERIAS DE JULHO — diversao com o melhor preco!",
    }
    if data_especial and data_especial in BANNER_DATA:
        p.append(f"{'★' * 3} {BANNER_DATA[data_especial]} {'★' * 3}")
        p.append("")

    # 1. HOOK — para o scroll
    p.append(hook)
    p.append("")

    # 2. Separador visual + nome do produto
    p.append("━" * 30)
    p.append(f"{_src_emoji}  {nome}")
    p.append("━" * 30)
    p.append("")

    # 3. PRECO EM DESTAQUE — bloco principal
    p.append(f"🔴  De: {original_str}")
    p.append(f"🟢  Por apenas: {preco_str}")
    p.append(f"💰  Voce economiza: {economia_str}  ({desconto_pct}% OFF)")
    if loja:
        p.append(f"🏪  Loja: {loja}")
    p.append("")

    # 4. Opinião humanizada + Benefício
    p.append(f"💬  {adicionar_opiniao(produto)}")
    p.append(f"✔️  {beneficio}")

    # 5. Prova social
    for linha in prova:
        p.append(f"✔️  {linha}")
    p.append("")

    # 6. Urgência
    p.append(f"⚠️  {urgencia}")
    p.append("")

    # 7. CTA — link (bio para Shopee, direto para Amazon/ML)
    link_prod = produto.get("affiliate_url") or produto.get("product_url","")
    p.append("─" * 30)
    p.append("🔗  COMO COMPRAR:")
    if produto.get("source","") in ("amazon","mercadolivre","netshoes"):
        p.append(f"👆  Clique no link do perfil @brizzah.br → {_src_nome}")
        p.append(f"🛒  Ou acesse direto: {link_prod[:60]}")
    else:
        p.append("👆  Acessa o LINK NA BIO do perfil @brizzah.br")
        p.append("🛒  Todos os achadinhos estao la com o melhor preco!")
    p.append("─" * 30)
    p.append("")

    # 8. Engajamento
    p.append("💾  Salva esse post pra nao perder essa oferta!")
    p.append("👥  Marca aquela amiga que adora um achado bom!")
    p.append("❤️  Curte se voce achou um barato!")
    p.append("")

    # 9. Hashtags
    p.append(hashtags)

    return "\n".join(p)
# ════════════════════════════════════════════════════════
#  GERADOR DE IMAGEM PROFISSIONAL (estilo bots top)
# ════════════════════════════════════════════════════════
def criar_imagem_profissional(produto_img_bytes, produto):
    """Compatibilidade — delegado para gerar_slides_carrossel."""
    try:
        slides = gerar_slides_carrossel([produto_img_bytes], produto)
        return slides[0] if slides else None
    except Exception:
        return None


def preparar_imagem_produto(img_url, produto=None):
    """
    Gera imagem profissional 1080×1350 (4:5) — padrão top conversão 2026:
    • Fundo neutro quente (cinza claro) — produto ocupa 85% do frame
    • Badge laranja Shopee "X% OFF" no canto superior esquerdo
    • Faixa degradê semitransparente na base com preço e "LINK NA BIO"
    • Logo Brizzah discreto no topo direito
    • Selo "MAIS VENDIDO ⭐" quando produto tiver muitas vendas
    """
    if not img_url:
        return None
    try:
        from PIL import Image, ImageDraw, ImageFont, ImageFilter
        import os as _os

        W, H = 1080, 1350   # 4:5 — maior conversão 2026

        # Paleta profissional
        BG        = (245, 243, 240)   # bege claro quente — não é branco puro
        LARANJA   = (238, 77,  45)    # Shopee oficial
        AMARELO   = (255, 200,  0)    # preço
        BRANCO    = (255, 255, 255)
        PRETO     = (20,  20,  20)
        VERDE     = (0,   200, 150)   # Brizzah
        VERMELHO  = (205, 30,  30)    # badge OFF
        CINZA_MD  = (120, 120, 120)

        # Fontes
        PATH_B = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        PATH_R = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
        def F(p, s):
            try:    return ImageFont.truetype(p, s)
            except: return ImageFont.load_default()

        fnt_badge  = F(PATH_B, 46)   # % OFF
        fnt_off    = F(PATH_B, 28)   # "OFF"
        fnt_preco  = F(PATH_B, 88)   # preço atual
        fnt_orig   = F(PATH_R, 34)   # de R$ xxx
        fnt_nome   = F(PATH_R, 27)   # nome produto
        fnt_logo   = F(PATH_B, 26)   # Brizzah
        fnt_cta    = F(PATH_B, 38)   # LINK NA BIO
        fnt_selo   = F(PATH_B, 24)   # mais vendido

        # ── Canvas ────────────────────────────────────────────────
        canvas = Image.new("RGB", (W, H), BG)
        d = ImageDraw.Draw(canvas)

        # Fundo com textura sutil (linhas diagonais muito finas)
        for xi in range(0, W, 40):
            d.line([(xi, 0), (xi+H, H)], fill=(238, 236, 233), width=1)

        # ── Foto do produto — 85% do frame ────────────────────────
        r = requests.get(img_url, timeout=14, headers={
            "User-Agent": "Mozilla/5.0 (Linux; Android 11)",
            "Referer":    "https://shopee.com.br/"
        })
        if r.status_code != 200 or len(r.content) < 500:
            return None

        _raw = Image.open(BytesIO(r.content))
        # Trata transparência
        if _raw.mode in ("RGBA", "P", "LA"):
            bg2 = Image.new("RGB", _raw.size, BG)
            if _raw.mode == "P": _raw = _raw.convert("RGBA")
            if _raw.mode in ("RGBA", "LA"):
                bg2.paste(_raw, mask=_raw.split()[-1])
            else:
                bg2.paste(_raw)
            _raw = bg2
        else:
            _raw = _raw.convert("RGB")

        # Zona da foto: do y=70 ao y=1120 (950px de altura, 1080px de largura)
        FOTO_ZONA_Y1 = 70
        FOTO_ZONA_Y2 = 1115
        FOTO_W = 1080
        FOTO_H = FOTO_ZONA_Y2 - FOTO_ZONA_Y1   # 1045px

        fw, fh = _raw.size
        # Escala para 88% da zona (deixa margem)
        esc = min((FOTO_W * 0.88) / fw, (FOTO_H * 0.88) / fh)
        nw, nh = max(1, int(fw * esc)), max(1, int(fh * esc))
        foto = _raw.resize((nw, nh), Image.LANCZOS)
        _raw.close()

        # Centraliza na zona da foto
        px = (FOTO_W - nw) // 2
        py = FOTO_ZONA_Y1 + (FOTO_H - nh) // 2
        canvas.paste(foto, (px, py))
        foto.close()

        # ── Dados do produto ──────────────────────────────────────
        prod      = produto or {}
        preco_f   = float(prod.get("price", 0) or 0)
        preco_db  = float(prod.get("original_price", 0) or 0)
        desc_pct  = int(prod.get("discount_pct", 0) or 0)
        vendidos  = int(prod.get("sold", 0) or 0)
        stars     = float(prod.get("rating", 0) or 0)
        nome_prod = (prod.get("name") or "Produto Shopee")

        if not desc_pct and preco_f > 0:
            if preco_db > preco_f:
                desc_pct = int(round((1 - preco_f / preco_db) * 100))
            else:
                desc_pct = 40 if preco_f < 30 else 35 if preco_f < 80 else 30 if preco_f < 200 else 25
        preco_orig = preco_db if preco_db > preco_f else round(preco_f / (1 - desc_pct / 100), 2) if desc_pct else preco_f * 1.38
        economia   = round(preco_orig - preco_f, 2)

        def fmt(v): return f"R$ {v:,.2f}".replace(",","X").replace(".",",").replace("X",".")

        # ── BADGE % OFF (canto superior esquerdo) ─────────────────
        if desc_pct > 0:
            BX, BY = 28, 80
            badge_t = f"-{desc_pct}%"
            off_t   = "OFF"
            try:
                bb = d.textbbox((0,0), badge_t, font=fnt_badge)
                bo = d.textbbox((0,0), off_t,   font=fnt_off)
                bw = max(bb[2]-bb[0], bo[2]-bo[0]) + 28
                bh = (bb[3]-bb[1]) + (bo[3]-bo[1]) + 20
            except: bw, bh = 110, 88
            # Fundo badge laranja com sombra
            d.rounded_rectangle([BX+4, BY+4, BX+bw+4, BY+bh+4], radius=12, fill=(0,0,0,60) if False else (40,20,10))
            d.rounded_rectangle([BX, BY, BX+bw, BY+bh], radius=12, fill=LARANJA)
            try:
                tx = BX + (bw - (bb[2]-bb[0])) // 2
                ty = BY + 6
                d.text((tx, ty), badge_t, font=fnt_badge, fill=BRANCO)
                tx2 = BX + (bw - (bo[2]-bo[0])) // 2
                d.text((tx2, ty + (bb[3]-bb[1]) + 2), off_t, font=fnt_off, fill=BRANCO)
            except: pass

        # ── Logo Brizzah (topo direito) ───────────────────────────
        logo_txt = "Brizzah"
        try:
            bb_l = d.textbbox((0,0), logo_txt, font=fnt_logo)
            lw = bb_l[2]-bb_l[0]
            d.text((W - lw - 20, 22), logo_txt, font=fnt_logo, fill=VERDE)
            # sublinhado
            d.line([(W-lw-20, 22+bb_l[3]-bb_l[1]+2), (W-20, 22+bb_l[3]-bb_l[1]+2)], fill=VERDE, width=2)
        except: pass

        # ── Selo "MAIS VENDIDO" (se aplicável) ────────────────────
        if vendidos >= 500 or stars >= 4.7:
            selo = f"⭐ +{vendidos//1000}k vendidos" if vendidos >= 1000 else "⭐ MAIS VENDIDO"
            try:
                bb_s = d.textbbox((0,0), selo, font=fnt_selo)
                sw = bb_s[2]-bb_s[0]+20; sh = bb_s[3]-bb_s[1]+10
                d.rounded_rectangle([W-sw-16, 75, W-16, 75+sh], radius=8, fill=(20,20,20))
                d.text((W-sw-6, 80), selo, font=fnt_selo, fill=(255,200,0))
            except: pass

        # ── Faixa inferior semitransparente com preço ─────────────
        FAIXA_Y = 1115
        FAIXA_H = H - FAIXA_Y   # 235px

        # Degradê: preto 0% → preto 85%
        overlay = Image.new("RGBA", (W, FAIXA_H), (0,0,0,0))
        od = ImageDraw.Draw(overlay)
        for yi in range(FAIXA_H):
            alpha = int(215 * (yi / FAIXA_H) ** 0.6)
            od.line([(0, yi), (W, yi)], fill=(15, 15, 15, alpha))
        canvas.paste(Image.new("RGB", (W, FAIXA_H), (15,15,15)), (0, FAIXA_Y))
        # Aplica degradê real via linhas
        for yi in range(FAIXA_H):
            alpha_f = (yi / FAIXA_H) ** 0.5
            c = int(245 * (1 - alpha_f))  # BG → preto
            d.line([(0, FAIXA_Y + yi), (W, FAIXA_Y + yi)], fill=(c, c, c) if yi < 15 else (15,15,15))

        # Nome do produto — quebra automática em até 2 linhas
        nome_limpo = nome_prod
        palavras = nome_limpo.split()
        linha1, linha2 = "", ""
        for pal in palavras:
            teste = (linha1 + " " + pal).strip()
            try:
                bb_t = d.textbbox((0,0), teste, font=fnt_nome)
                if bb_t[2]-bb_t[0] <= W - 40: linha1 = teste
                else:
                    if not linha2: linha2 = pal
                    else:
                        bb_t2 = d.textbbox((0,0), (linha2+" "+pal).strip(), font=fnt_nome)
                        if bb_t2[2]-bb_t2[0] <= W - 40: linha2 = (linha2+" "+pal).strip()
            except: linha1 = teste
        for i, ln in enumerate([linha1, linha2]):
            if ln:
                d.text((22, FAIXA_Y + 8 + i*34), ln, font=fnt_nome, fill=(195,195,195))

        # Preço original riscado
        orig_full = f"de {fmt(preco_orig)}"
        oy = FAIXA_Y + 82
        d.text((22, oy), orig_full, font=fnt_orig, fill=CINZA_MD)
        try:
            bb_o = d.textbbox((0,0), orig_full, font=fnt_orig)
            mid_y = oy + (bb_o[3]-bb_o[1])//2
            d.line([(22, mid_y), (22+bb_o[2]-bb_o[0], mid_y)], fill=CINZA_MD, width=2)
        except: pass

        # Preço atual — grande e amarelo
        preco_txt = fmt(preco_f)
        try:
            bb_p = d.textbbox((0,0), preco_txt, font=fnt_preco)
            # Ajusta fonte se muito larga
            fnt_p_use = fnt_preco
            if bb_p[2]-bb_p[0] > W - 280:
                fnt_p_use = F(PATH_B, 68)
        except: fnt_p_use = fnt_preco
        d.text((18, FAIXA_Y + 118), preco_txt, font=fnt_p_use, fill=AMARELO)

        # Economia
        if economia > 2:
            eco = f"economia de {fmt(economia)}"
            try:
                bb_e = d.textbbox((0,0), eco, font=fnt_nome)
                d.text((22, H - 56), eco, font=fnt_nome, fill=(100,220,130))
            except: pass

        # CTA "LINK NA BIO ↓" alinhado à direita
        cta_txt = "LINK NA BIO ↓"
        try:
            bb_c = d.textbbox((0,0), cta_txt, font=fnt_cta)
            cx = W - (bb_c[2]-bb_c[0]) - 22
        except: cx = W - 260
        d.text((cx, H - 62), cta_txt, font=fnt_cta, fill=LARANJA)

        # ── Linha divisória decorativa no topo ────────────────────
        d.line([(0, 68), (W, 68)], fill=(220,218,215), width=1)

        # ── Salva em disco ────────────────────────────────────────
        buf = BytesIO()
        canvas.save(buf, "JPEG", quality=90, optimize=True)
        img_bytes = buf.getvalue()
        canvas.close()

        key = f"_img_{int(time.time()*1000)}"
        _os.makedirs(_SLIDE_DIR, exist_ok=True)
        fpath = _os.path.join(_SLIDE_DIR, key + ".jpg")
        with open(fpath, "wb") as _f:
            _f.write(img_bytes)
        del img_bytes, buf

        host = (cfg("bot_url","") or os.environ.get("BOT_URL","https://shopee-bot-jt11.onrender.com")).rstrip("/")
        return f"{host}/slide/{key}"
    except Exception as e:
        log("ERROR", f"preparar_imagem_produto: {str(e)[:100]}")
        return None


# ════════════════════════════════════════════════════════════════════
#  BUSCADOR DE MÚLTIPLAS IMAGENS REAIS DO PRODUTO (Shopee CDN)
#  Tenta 3 estratégias em cascata — sempre retorna pelo menos 1 imagem
# ════════════════════════════════════════════════════════════════════
def shopee_buscar_imagens_produto(produto, max_imgs=8):
    """
    Busca até max_imgs fotos originais do produto na Shopee.
    Estratégias (em ordem):
      1. API v4 mobile (itemid + shopid)
      2. Scraping __NEXT_DATA__ da página do produto
      3. Fallback: image_url / image_urls da API de afiliados
    Retorna lista de bytes de imagens válidas (>= 2KB).
    """
    import re as _re, requests as _req

    HDR = {
        "User-Agent": (
            "Mozilla/5.0 (Linux; Android 13; SM-G998B) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.6367.82 Mobile Safari/537.36"
        ),
        "Referer":           "https://shopee.com.br/",
        "Accept":            "application/json, image/*, */*",
        "Accept-Language":   "pt-BR,pt;q=0.9",
        "x-api-source":      "rn",
        "x-shopee-language": "pt-BR",
    }

    shop_id = str(produto.get("shop_id", "") or "")
    item_id = str(produto.get("item_id", "") or "")
    hashes  = []

    # ── Estratégia 1: API v4 mobile ───────────────────────────────
    if shop_id and item_id:
        for ep in [
            f"https://shopee.com.br/api/v4/item/get?itemid={item_id}&shopid={shop_id}",
            f"https://shopee.com.br/api/v2/item/get?itemid={item_id}&shopid={shop_id}",
        ]:
            try:
                r = _req.get(ep, headers=HDR, timeout=8)
                if r.status_code == 200:
                    d = r.json()
                    item = (
                        (d.get("data") or {}).get("item")
                        or d.get("item")
                        or d.get("data")
                        or {}
                    )
                    hashes = item.get("images") or []
                    if hashes:
                        log("INFO", f"[IMG] API v4: {len(hashes)} hashes")
                        break
            except Exception:
                pass

    # ── Estratégia 2: scraping __NEXT_DATA__ ─────────────────────
    if not hashes and shop_id and item_id:
        try:
            page_url = f"https://shopee.com.br/product/{shop_id}/{item_id}"
            r = _req.get(page_url, headers=HDR, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                found = _re.findall(r'"images"\s*:\s*\[([^\]]+)\]', r.text)
                for chunk in found:
                    raw = _re.findall(r'"([a-f0-9]{30,})"', chunk)
                    if raw:
                        hashes = raw
                        log("INFO", f"[IMG] NEXT_DATA: {len(hashes)} hashes")
                        break
        except Exception:
            pass

    # ── Monta URLs Shopee CDN ──────────────────────────────────────
    urls = []
    if hashes:
        for h in hashes[:max_imgs]:
            urls.append(f"https://cf.shopee.com.br/file/{h}")

    # ── Estratégia 3: fallback nas URLs existentes ────────────────
    if not urls:
        fallbacks = []
        iu = produto.get("image_url", "")
        if iu:
            if iu.startswith("//"): iu = "https:" + iu
            elif not iu.startswith("http"): iu = "https://cf.shopee.com.br/file/" + iu
            fallbacks.append(iu)
        for u in (produto.get("image_urls") or []):
            if u and u not in fallbacks:
                fallbacks.append(u)
        urls = fallbacks[:max_imgs]
        if urls:
            log("INFO", f"[IMG] Fallback: {len(urls)} URL(s)")

    # ── Baixa imagens em paralelo ─────────────────────────────────
    from concurrent.futures import ThreadPoolExecutor

    def _baixar(url):
        try:
            ri = _req.get(url, headers=HDR, timeout=8)
            if ri.status_code == 200 and len(ri.content) >= 2048:
                return ri.content
        except Exception:
            pass
        return None

    imgs_bytes = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for b in ex.map(_baixar, urls):
            if b:
                imgs_bytes.append(b)

    log("INFO", f"[IMG] Shopee: {len(imgs_bytes)}/{len(urls)} imagens baixadas")
    return imgs_bytes


# ════════════════════════════════════════════════════════════════════
#  BUSCA DE FOTOS EXTRAS NA INTERNET (Google / Bing / DuckDuckGo)
#  Complementa as fotos Shopee com ângulos reais do produto
# ════════════════════════════════════════════════════════════════════
def buscar_fotos_internet(nome_produto, max_fotos=4):
    """
    Busca fotos extras do produto/modelo na internet usando múltiplas fontes.
    Estratégias:
      1. Google Images via scraping (headers mobile)
      2. Bing Images via API scraping
      3. DuckDuckGo Images (fallback)

    Filtragem automática:
      - Apenas imagens >= 40KB (evita ícones/thumbs)
      - Remove imagens com texto/watermark via heurística de cor
      - Somente JPEG/PNG/WEBP
      - Prefere fundo branco/neutro (produto isolado)

    Retorna lista de bytes ordenada por "limpeza" (fundo mais neutro primeiro).
    """
    import re as _re, requests as _req, urllib.parse as _up

    # Extrai termo de busca limpo do nome
    stopwords = {'com', 'de', 'do', 'da', 'para', 'kit', 'e', 'em', 'no',
                 'na', 'por', 'un', 'unid', 'pct', 'caixa', 'pack', 'frete',
                 'grátis', 'gratis', 'oferta', 'promoção', 'shopee'}
    palavras = [p.strip('.,!-_()[]') for p in nome_produto.split()
                if len(p) > 2 and p.lower() not in stopwords]
    # Pega as primeiras 5 palavras mais informativas
    query_raw = ' '.join(palavras[:5])
    log("INFO", f"[FOTOS-WEB] Buscando: '{query_raw}'")

    HDR_WEB = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.6367.82 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
    }
    HDR_IMG = {
        "User-Agent": "Mozilla/5.0",
        "Accept":     "image/webp,image/apng,image/*,*/*;q=0.8",
    }

    urls_candidatas = []

    # ── Fonte 1: Bing Images ──────────────────────────────────────
    try:
        q    = _up.quote_plus(query_raw)
        url  = f"https://www.bing.com/images/search?q={q}&form=HDRSC2&first=1&tsc=ImageHoverTitle"
        r    = _req.get(url, headers=HDR_WEB, timeout=10)
        if r.status_code == 200:
            # Extrai src de imagens reais (não thumbs)
            found = _re.findall(r'"murl"\s*:\s*"(https?://[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"', r.text)
            urls_candidatas.extend(found[:12])
            if not found:
                # Tenta via data-src
                found2 = _re.findall(r'src="(https?://[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"', r.text)
                urls_candidatas.extend([u for u in found2 if 'th.bing.com/th' not in u][:8])
            log("INFO", f"[FOTOS-WEB] Bing: {len(found)} candidatas")
    except Exception as e:
        log("WARN", f"[FOTOS-WEB] Bing falhou: {str(e)[:50]}")

    # ── Fonte 2: Google Images ────────────────────────────────────
    if len(urls_candidatas) < 6:
        try:
            q   = _up.quote_plus(query_raw)
            url = (f"https://www.google.com/search?q={q}&tbm=isch"
                   f"&tbs=ic:specific,isc:white,isz:m&hl=pt-BR")
            HDR_G = dict(HDR_WEB)
            HDR_G["User-Agent"] = (
                "Mozilla/5.0 (Linux; Android 13) "
                "AppleWebKit/537.36 Chrome/124 Mobile Safari/537.36"
            )
            r = _req.get(url, headers=HDR_G, timeout=10)
            if r.status_code == 200:
                found = _re.findall(r'"(https?://[^"]+\.(?:jpg|jpeg|png|webp))"', r.text)
                # Filtra URLs de produtos reais (não logos do Google, etc.)
                filtradas = [u for u in found
                             if not any(x in u for x in
                                        ['gstatic.com','google.com','googleapis',
                                         'youtube','facebook','instagram'])]
                urls_candidatas.extend(filtradas[:10])
                log("INFO", f"[FOTOS-WEB] Google: {len(filtradas)} candidatas")
        except Exception as e:
            log("WARN", f"[FOTOS-WEB] Google falhou: {str(e)[:50]}")

    # ── Fonte 3: DuckDuckGo Images ────────────────────────────────
    if len(urls_candidatas) < 4:
        try:
            q   = _up.quote_plus(query_raw)
            # DDG Images via vqd token
            r0  = _req.get(f"https://duckduckgo.com/?q={q}&ia=images",
                           headers=HDR_WEB, timeout=8)
            vqd = _re.search(r'vqd=([^&"]+)', r0.text)
            if vqd:
                vqd_val = vqd.group(1)
                r1 = _req.get(
                    f"https://duckduckgo.com/i.js?q={q}&vqd={vqd_val}&f=,,,&p=1",
                    headers={**HDR_WEB, "Referer": "https://duckduckgo.com/"},
                    timeout=8
                )
                if r1.status_code == 200:
                    results = r1.json().get("results", [])
                    for res in results[:10]:
                        iu = res.get("image") or res.get("url", "")
                        if iu and iu not in urls_candidatas:
                            urls_candidatas.append(iu)
                    log("INFO", f"[FOTOS-WEB] DDG: {len(results)} candidatas")
        except Exception as e:
            log("WARN", f"[FOTOS-WEB] DDG falhou: {str(e)[:50]}")

    # ── Remove duplicatas mantendo ordem ──────────────────────────
    seen = set()
    urls_unicas = []
    for u in urls_candidatas:
        if u not in seen:
            seen.add(u)
            urls_unicas.append(u)

    log("INFO", f"[FOTOS-WEB] Total candidatas únicas: {len(urls_unicas)}")

    # ── Baixa, valida e puntua cada imagem ────────────────────────
    from concurrent.futures import ThreadPoolExecutor
    from PIL import Image as _PImg
    from io import BytesIO as _BIO

    MIN_KB = 30          # mínimo 30KB
    MIN_DIM = 200        # mínimo 200px em cada lado

    def _score_limpeza(img_bytes):
        """
        Heurística de "limpeza": quanto mais branco/neutro o fundo, maior o score.
        Retorna valor 0–100. Imagens com fundo branco/cinza claro pontuam alto.
        """
        try:
            img = _PImg.open(_BIO(img_bytes)).convert("RGB")
            img.thumbnail((200, 200))
            px  = list(img.getdata())
            n   = len(px)
            if n == 0: return 0
            # Conta pixels "quase brancos" (r>200, g>200, b>200)
            brancos = sum(1 for r,g,b in px if r>195 and g>195 and b>195)
            # Conta pixels "neutros" (baixa saturação)
            from colorsys import rgb_to_hsv
            neutros = sum(1 for r,g,b in px
                          if rgb_to_hsv(r/255,g/255,b/255)[1] < 0.15)
            return int((brancos + neutros) / (n * 2) * 100)
        except Exception:
            return 0

    def _baixar_e_validar(url):
        try:
            r = _req.get(url, headers=HDR_IMG, timeout=8, stream=True)
            if r.status_code != 200:
                return None
            # Verifica content-type
            ct = r.headers.get("Content-Type", "")
            if not any(x in ct for x in ["image/jpeg","image/png","image/webp",
                                          "image/jpg","octet-stream"]):
                # Tenta inferir pela URL
                if not any(url.lower().endswith(x) for x in [".jpg",".jpeg",".png",".webp"]):
                    return None
            # Lê no máximo 2MB
            chunks = []
            total  = 0
            for chunk in r.iter_content(4096):
                chunks.append(chunk)
                total += len(chunk)
                if total > 2 * 1024 * 1024:
                    return None
            data = b"".join(chunks)
            if len(data) < MIN_KB * 1024:
                return None
            # Verifica dimensões
            img = _PImg.open(_BIO(data))
            w, h = img.size
            if w < MIN_DIM or h < MIN_DIM:
                return None
            # Calcula score de limpeza
            score = _score_limpeza(data)
            return (score, data)
        except Exception:
            return None

    resultados = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for res in ex.map(_baixar_e_validar, urls_unicas[:20]):
            if res:
                resultados.append(res)

    # Ordena por score de limpeza (fundo mais neutro/branco primeiro)
    resultados.sort(key=lambda x: x[0], reverse=True)
    fotos = [data for _, data in resultados[:max_fotos]]
    log("INFO", f"[FOTOS-WEB] {len(fotos)} fotos extras obtidas da internet")
    return fotos


# ════════════════════════════════════════════════════════════════════
#  GERADOR DE SLIDES PROFISSIONAL — ESTILO BOTS TOP (v4)
#
#  Filosofia: a FOTO É O HERÓI. Sem poluição visual, sem gradientes
#  pesados cobrindo o produto. Inspirado nos melhores bots de afiliado
#  (Magazine Luiza, Mercado Livre, bots virais do TikTok/IG).
#
#  Layout:
#    Slide 1 — FOTO LIMPA 1:1: produto centralizado em fundo branco
#              puro. Só badge "shopee" pequeno no topo esq. Produto
#              100% visível, nítido, sem nada cobrindo.
#
#    Slide 2 — SEGUNDO ÂNGULO: outra foto do produto (ou crop diferente
#              da mesma), badge de ranking/vendidos no rodapé sutil.
#
#    Slide 3 — DESTAQUE DE PREÇO: fundo branco, produto à direita
#              (50%), painel de info à esquerda com preço grande em
#              laranja, estrelas e nome — estilo exato @brizzah.br.
#
#  Resultado: carrossel profissional, fotos nítidas, fácil de enxergar
#  o produto, preço na descrição (não precisa sobrecarregar o slide).
# ════════════════════════════════════════════════════════════════════
def gerar_slides_carrossel(img_bytes_list, produto):
    """
    Gera 1 imagem limpa do produto:
    - Foto original centralizada em fundo branco 1080x1080
    - Faixa fina laranja no topo (marca @brizzah.br)
    - Sem texto sobreposto, sem carrossel, sem overlay
    """
    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO

    W, H    = 1080, 1080
    LARANJA = (238, 77, 45)
    BRANCO  = (255, 255, 255)
    CINZA   = (120, 120, 120)

    # Normaliza entrada — usa sempre a 1ª foto disponível
    fotos = []
    if isinstance(img_bytes_list, (bytes, bytearray)):
        fotos = [bytes(img_bytes_list)]
    elif isinstance(img_bytes_list, list):
        fotos = [bytes(b) for b in img_bytes_list if b]

    if not fotos:
        return []

    def _fazer_slide(foto_bytes):
        try:
            # Abre imagem original
            foto = Image.open(BytesIO(foto_bytes)).convert("RGB")

            # Canvas branco 1080x1080
            canvas = Image.new("RGB", (W, H), BRANCO)

            # Área útil para a foto (com margens)
            MARGEM_TOP  = 6    # só a faixa laranja fina
            MARGEM_BOT  = 44   # espaço para assinatura discreta
            MARGEM_LAT  = 40
            area_w = W - MARGEM_LAT * 2
            area_h = H - MARGEM_TOP - MARGEM_BOT

            # Redimensiona mantendo proporção, encaixando na área útil
            fw, fh = foto.size
            ratio  = min(area_w / fw, area_h / fh)
            nw     = int(fw * ratio)
            nh     = int(fh * ratio)
            foto_r = foto.resize((nw, nh), Image.LANCZOS)

            # Cola centralizado
            px = (W - nw) // 2
            py = MARGEM_TOP + (area_h - nh) // 2
            canvas.paste(foto_r, (px, py))

            d = ImageDraw.Draw(canvas)

            # Faixa laranja fina no topo (só 6px — quase invisível, apenas branding)
            d.rectangle([0, 0, W, MARGEM_TOP], fill=LARANJA)

            # Assinatura mínima no rodapé
            try:
                # Tenta carregar fonte
                font_paths = [
                    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
                    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
                    "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
                ]
                f_asgn = None
                for fp in font_paths:
                    import os
                    if os.path.exists(fp):
                        f_asgn = ImageFont.truetype(fp, 22)
                        break
                if not f_asgn:
                    f_asgn = ImageFont.load_default()

                txt    = "@brizzah.br"
                bb     = d.textbbox((0, 0), txt, font=f_asgn)
                tw, th = bb[2] - bb[0], bb[3] - bb[1]
                d.text(((W - tw) // 2, H - th - 10), txt, font=f_asgn, fill=(180, 180, 180))
            except Exception:
                pass

            buf = BytesIO()
            canvas.save(buf, "JPEG", quality=95, optimize=True)
            return buf.getvalue()
        except Exception as e:
            log("WARN", f"[SLIDE] {e}")
            return None

    resultado = _fazer_slide(fotos[0])
    return [resultado] if resultado else []


# ── Stubs de compatibilidade ─────────────────────────────────────────
def _slide_heroi(*a, **kw): return None
def _slide_zoom_desconto(*a, **kw): return None
def _slide_specs(*a, **kw): return None
def _slide_economia(*a, **kw): return None
def _slide_foto_produto(*a, **kw): return None
def _slide_cta_final(*a, **kw): return None





# ════════════════════════════════════════════════════════
#  SHOPEE — API Oficial de Afiliados (GraphQL + SHA256)
# ════════════════════════════════════════════════════════
def shopee_api_auth_header(app_id, secret, payload):
    """Gera o header Authorization com assinatura SHA256"""
    import hashlib, time as _time
    timestamp = str(int(_time.time()))
    raw = f"{app_id}{timestamp}{payload}{secret}"
    signature = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return {
        "Authorization": f"SHA256 Credential={app_id}, Timestamp={timestamp}, Signature={signature}",
        "Content-Type": "application/json"
    }

def shopee_api_top100(nicho="casa", sort_type=2):
    """
    Coleta até 100 produtos mais vendidos via paginação da API oficial.
    sort_type=2 = Mais Vendidos | sort_type=3 = Maior Comissão
    Usa keywords amplas + paginação (5 páginas x 20 = 100 produtos)
    """
    # Credenciais com fallback garantido
    app_id = (cfg("shopee_app_id") or
              os.environ.get("SHOPEE_APP_ID") or
              "18345690956").strip()
    secret = (cfg("shopee_secret") or
              os.environ.get("SHOPEE_SECRET") or
              "CSWN4EHO64ARF4LWQRKMSP22QFFMHQZH").strip()

    log("INFO", f"top100: app_id={app_id[:8]}... secret={secret[:6]}...")

    if not app_id or not secret:
        log("ERROR", "top100: credenciais vazias!")
        return []

    # Keywords amplas para cobrir o catálogo geral
    KEYWORDS_AMPLAS = {
        "geral":    ["moda feminina", "tenis casual", "fone bluetooth", "bolsa feminina", "smartwatch"],
        "beleza":   ["maquiagem", "skincare", "perfume", "cabelo", "hidratante facial"],
        "casa":     ["decoracao casa", "cozinha pratica", "organizacao casa", "tapete sala", "luminaria led"],
        "moda":     ["moda feminina", "blusa feminina", "vestido", "tenis feminino", "bolsa feminina"],
        "eletro":   ["fone bluetooth", "smartwatch", "carregador turbo", "caixa de som bluetooth", "acessorios celular"],
        "fitness":  ["suplemento", "whey", "academia", "esporte", "fitness"],
    }
    keywords = KEYWORDS_AMPLAS.get(nicho.lower(), [nicho, "moda feminina", "fone bluetooth", "tenis casual", "bolsa feminina"])

    todos = {}

    # Randomização real: mistura hora + segundo + pid para sempre pegar páginas diferentes
    import random as _rr
    _seed = int(time.time() * 1000) % 999983  # sempre diferente
    _rr.seed(_seed)
    _rr.shuffle(keywords)          # ordem das keywords aleatorizada
    # Paginas em ordem aleatória dentro de 1–7
    page_pool = list(range(1, 8))
    _rr.shuffle(page_pool)

    for kw in keywords:
        for page in page_pool[:5]:  # 5 páginas aleatórias × 20 = 100 por keyword
            # Alterna sort_type: se sort_type=2 (vendidos), alterna com relevância para variar
            st_usar = sort_type if _rr.random() > 0.35 else (1 if sort_type == 2 else 2)
            query_str = json.dumps({"query": f"""{{
  productOfferV2(keyword:"{kw}", listType:1, sortType:{st_usar}, page:{page}, limit:20) {{
    nodes {{
      itemId shopId productName productLink offerLink imageUrl
      priceMin sales ratingStar commissionRate shopName
    }}
  }}
}}"""}, separators=(",",":"))
            ts  = str(int(time.time()))
            sig = hashlib.sha256(f"{app_id}{ts}{query_str}{secret}".encode()).hexdigest()
            hdrs = {
                "Authorization": f"SHA256 Credential={app_id}, Timestamp={ts}, Signature={sig}",
                "Content-Type": "application/json"
            }
            try:
                r = requests.post("https://open-api.affiliate.shopee.com.br/graphql",
                                  headers=hdrs, data=query_str, timeout=20)
                nodes = r.json().get("data", {}).get("productOfferV2", {}).get("nodes", [])
            except Exception as e:
                log("WARN", f"top100 pag {page}: {e}")
                nodes = []

            if not nodes:
                break  # sem mais páginas nesta keyword

            for n in nodes:
                pid = f"{n.get('shopId',0)}_{n.get('itemId',0)}"
                if pid not in todos:
                    preco = float(n.get("priceMin") or 0)
                    if preco > 1000:
                        preco = preco / 100000
                    img = n.get("imageUrl") or ""
                    if img and not img.startswith("http"):
                        img = "https:" + img
                    sid_t = str(n.get("shopId") or "")
                    iid_t = str(n.get("itemId") or "")
                    todos[pid] = {
                        "name":          (n.get("productName") or "Produto")[:100],
                        "price":         round(preco, 2),
                        "commission":    round(float(n.get("commissionRate") or 0), 2),
                        "rating":        round(float(n.get("ratingStar") or 0), 1),
                        "sold":          int(n.get("sales") or 0),
                        "image_url":     img,
                        "image_urls":    [img] if img else [],
                        "product_url":   n.get("productLink") or "",
                        "affiliate_url": n.get("offerLink") or "",
                        "shop_id":       sid_t,
                        "item_id":       iid_t,
                        "shop_name":     n.get("shopName") or "",
                    }

            time.sleep(0.3)  # respeita rate limit da API

        if len(todos) >= 100:
            break

    # Ordena por vendas decrescente e retorna top 100
    top = sorted(todos.values(),
                 key=lambda x: x["sold"], reverse=True)[:100]
    log("INFO", f"top100 coletado: {len(top)} produtos (nicho={nicho})")
    return top


def shopee_api_buscar_produtos(keyword, limit=5, sort_type=None):
    """Busca produtos reais via API oficial Shopee Afiliados com rotação inteligente"""
    import random, hashlib

    app_id = (cfg("shopee_app_id") or
              os.environ.get("SHOPEE_APP_ID") or
              "18345690956").strip()
    secret = (cfg("shopee_secret") or
              os.environ.get("SHOPEE_SECRET") or
              "CSWN4EHO64ARF4LWQRKMSP22QFFMHQZH").strip()
    if not app_id or not secret:
        return None

    # ── Rotação de keywords derivadas do nicho ──────────────
    keyword_base = keyword.strip().lower()
    variantes = {
        "beleza":      ["beleza", "maquiagem", "skincare", "hidratante", "perfume",
                        "batom", "base maquiagem", "creme facial", "protetor solar",
                        "esfoliante", "sérum facial", "kit skincare"],
        "maquiagem":   ["maquiagem", "batom", "base liquida", "paleta sombra",
                        "blush", "contorno facial", "mascara cilios", "primer"],
        "casa":        ["casa decoração", "organização casa", "utensilio cozinha",
                        "tapete sala", "luminaria", "porta objetos", "cesto organizador"],
        "moda":        ["moda feminina", "blusa feminina", "vestido", "calça",
                        "conjunto feminino", "bolsa", "sandalia", "tênis feminino"],
        "fitness":     ["fitness", "suplemento", "whey protein", "cinta modeladora",
                        "legging fitness", "tênis academia", "garrafa termica"],
        "pet":         ["pet shop", "ração cachorro", "brinquedo pet", "coleira",
                        "cama pet", "arranhador gato", "petisco cachorro"],
        "eletronico":  ["fone bluetooth", "carregador rapido", "cabo usb",
                        "smartwatch", "caixa de som", "powerbank"],
        "infantil":    ["brinquedo infantil", "kit escolar", "mochila infantil",
                        "jogo educativo", "boneca", "carrinho brinquedo"],
    }

    # Detecta variantes pelo keyword_base
    lista_kw = None
    for chave, lista in variantes.items():
        if chave in keyword_base or keyword_base in chave:
            lista_kw = lista
            break
    if not lista_kw:
        lista_kw = [keyword_base, f"kit {keyword_base}", f"{keyword_base} oferta",
                    f"{keyword_base} promoção", f"melhor {keyword_base}"]

    # ── Rotação determinística mas variada por hora ──────────
    # Muda a cada hora → produtos diferentes a cada hora mesmo no mesmo nicho
    # Seed baseado em microssegundos → sempre diferente mesmo chamadas consecutivas
    import random as _rng_mod
    rng = _rng_mod.Random(int(time.time() * 1000000) % 999983)

    # Escolhe keyword aleatória da lista
    kw_escolhida = rng.choice(lista_kw)

    # sortType: 1=relevancia, 2=vendas, 5=comissão — alterna para variar
    sort_types = [1, 2, 2, 5]  # peso maior para vendas
    sort_escolhido = sort_type if sort_type else rng.choice(sort_types)

    # Página aleatória SEGURA entre 1 e 3
    page_escolhida = rng.randint(1, 3)

    log("INFO", f"API Shopee: keyword='{kw_escolhida}' | sort={sort_escolhido} | página={page_escolhida}")

    def _montar_query_shopee(kw, sort_t, page_n, lim):
        return """
        {
          productOfferV2(
            keyword: "%s"
            listType: 1
            sortType: %d
            page: %d
            limit: %d
          ) {
            nodes {
              itemId
              productName
              productLink
              offerLink
              imageUrl
              priceMin
              priceMax
              priceDiscountRate
              sales
              ratingStar
              commissionRate
              sellerCommissionRate
              shopeeCommissionRate
              commission
              shopId
              shopName
              shopType
            }
            pageInfo { page limit hasNextPage }
          }
        }
        """ % (kw, sort_t, page_n, lim)

    query = _montar_query_shopee(kw_escolhida, sort_escolhido, page_escolhida, limit + 10)
    payload_obj = {"query": query.strip()}
    payload_str = json.dumps(payload_obj, separators=(",", ":"))
    headers = shopee_api_auth_header(app_id, secret, payload_str)

    try:
        resp = requests.post(
            "https://open-api.affiliate.shopee.com.br/graphql",
            headers=headers,
            data=payload_str,
            timeout=20
        )
        data = resp.json()

        # Verifica erros da API
        if "errors" in data:
            err = data["errors"][0].get("message", "Erro API")

            # Fallback automático: se estourar limite de página, volta para a página 1
            if "page limit" in err.lower() or "maximum number of page" in err.lower():
                log("WARN", f"Shopee limitou página {page_escolhida}; tentando página 1")

                page_escolhida = 1
                query = _montar_query_shopee(kw_escolhida, sort_escolhido, page_escolhida, limit + 10)
                payload_obj = {"query": query.strip()}
                payload_str = json.dumps(payload_obj, separators=(",", ":"))
                headers = shopee_api_auth_header(app_id, secret, payload_str)

                resp = requests.post(
                    "https://open-api.affiliate.shopee.com.br/graphql",
                    headers=headers,
                    data=payload_str,
                    timeout=20
                )
                data = resp.json()

                if "errors" in data:
                    err2 = data["errors"][0].get("message", "Erro API")
                    log("ERROR", f"Shopee API oficial erro após fallback: {err2[:120]}")
                    return None
            else:
                log("ERROR", f"Shopee API oficial erro: {err[:120]}")
                return None

        nodes = (data.get("data", {}) or {}).get("productOfferV2", {}).get("nodes", [])
        if not nodes:
            log("WARN", f"Shopee API oficial: nenhum produto para '{keyword}'")
            return None

        products = []
        for n in nodes:
            preco = float(n.get("priceMin") or n.get("priceMax") or 0)
            if preco > 1000:
                preco = preco / 100000
            commission = float(n.get("commissionRate") or 0)
            if commission < 1:
                commission = round(commission * 100, 1)

            # Link afiliado já vem pronto com rastreamento
            link = n.get("offerLink") or n.get("productLink") or ""
            img  = n.get("imageUrl") or ""

            # Garante HTTPS nas imagens Shopee
            if img and not img.startswith("http"):
                img = "https:" + img

            shop_id_n = str(n.get("shopId") or "")
            item_id_n = str(n.get("itemId") or "")

            products.append({
                "name":        (n.get("productName") or "Produto Shopee")[:100],
                "description": f"Vendidos: {n.get('sales',0)} | Loja: {n.get('shopName','')}",
                "price":       round(preco, 2),
                "commission":  commission,
                "rating":      round(float(n.get("ratingStar") or 0), 1),
                "sold":        int(n.get("sales") or 0),
                "image_url":   img,
                "image_urls":  [img] if img else [],
                "product_url": n.get("productLink") or "",
                "affiliate_url": link,
                "shop_id":     shop_id_n,
                "item_id":     item_id_n,
                "shop_name":   n.get("shopName") or "",
            })

        log("INFO", f"Shopee API oficial: {len(products)} produto(s) para '{keyword}'")
        return products[:limit]

    except Exception as e:
        log("ERROR", f"Shopee API oficial excecao: {str(e)[:80]}")
        return None


def shopee_buscar_detalhes(shop_id, item_id, headers, session_req):
    """Busca detalhes completos de um produto incluindo todas as imagens"""
    try:
        url = "https://shopee.com.br/api/v4/item/get"
        params = {"shopid": shop_id, "itemid": item_id}
        resp = session_req.get(url, params=params, headers=headers, timeout=10)
        data = resp.json()
        item = data.get("data", {}) or data.get("item", {})
        if item:
            imgs = item.get("images", [])
            return [f"https://cf.shopee.com.br/file/{im}" for im in imgs[:8] if im]
    except:
        pass
    return []


def preparar_imagens_carrossel(image_urls, max_fotos=8):
    """
    Simplificado — baixa a 1ª foto disponível do produto,
    converte para JPEG 1080x1080 fundo branco e hospeda via /slide/<key>.
    Retorna lista com 1 URL pública.
    """
    import requests as _req

    # Normaliza URLs
    validas = []
    for u in (image_urls or []):
        if not u: continue
        u = u.strip()
        if u.startswith("//"): u = "https:" + u
        if not u.startswith("http"):
            u = "https://cf.shopee.com.br/file/" + u
        validas.append(u)

    if not validas:
        return []

    produto = _produto_contexto.get() or {}

    # ── 1. Tenta baixar foto original da Shopee ─────────────────────
    foto_bytes = None
    hdrs = {
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
        "Referer": "https://shopee.com.br/",
        "Accept":  "image/webp,image/jpeg,image/*,*/*;q=0.8",
    }

    # Primeiro tenta a URL da CDN Shopee diretamente
    for url in validas[:3]:
        try:
            r = _req.get(url, headers=hdrs, timeout=12)
            if r.status_code == 200 and len(r.content) >= 2048:
                foto_bytes = r.content
                log("INFO", f"[SLIDE] Foto baixada: {len(foto_bytes):,} bytes | {url[:60]}")
                break
        except Exception as ex:
            log("WARN", f"[SLIDE] Download {url[:50]}: {str(ex)[:40]}")

    # ── 2. Fallback: tenta via API de imagens do produto ─────────────
    if not foto_bytes:
        try:
            fotos = shopee_buscar_imagens_produto(produto, max_imgs=1)
            if fotos:
                foto_bytes = fotos[0]
                log("INFO", f"[SLIDE] Foto via API: {len(foto_bytes):,} bytes")
        except Exception as ex:
            log("WARN", f"[SLIDE] API imagens: {str(ex)[:60]}")

    if not foto_bytes:
        log("WARN", "[SLIDE] Nenhuma foto disponível — post sem imagem")
        return []

    # ── 3. Gera imagem 1080x1080 fundo branco ────────────────────────
    slide_bytes = _gerar_slides_bytes([foto_bytes], produto)
    if not slide_bytes:
        log("WARN", "[SLIDE] Geração PIL falhou — usando fallback bruto")
        try:
            from PIL import Image as _I
            from io import BytesIO as _B
            img = _I.open(_B(foto_bytes)).convert("RGB")
            img.thumbnail((1080, 1080), _I.LANCZOS)
            c = _I.new("RGB", (1080, 1080), (255, 255, 255))
            c.paste(img, ((1080 - img.width) // 2, (1080 - img.height) // 2))
            b = _B(); c.save(b, "JPEG", quality=95); slide_bytes = [b.getvalue()]
        except Exception:
            slide_bytes = [foto_bytes]

    # ── 4. Hospeda via /slide/<key> ───────────────────────────────────
    host = (cfg("bot_url", "") or os.environ.get("BOT_URL", "")
            or "https://shopee-bot-jt11.onrender.com").rstrip("/")

    key = f"_slide_{int(time.time()*1000)}_001"
    try:
        with get_db() as cx:
            cx.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)",
                       (key, base64.b64encode(slide_bytes[0]).decode()))
        url_pub = f"{host}/slide/{key}"
        log("INFO", f"[SLIDE] Hospedado: {url_pub}")
        return [url_pub]
    except Exception as e:
        log("WARN", f"[SLIDE] Hospedagem falhou: {str(e)[:60]}")
        return []


# contexto thread-local para passar produto para preparar_imagens_carrossel
import threading as _threading
class _ProdutoContexto:
    def __init__(self):
        self._local = _threading.local()
    def set(self, p):
        self._local.produto = p
    def get(self):
        return getattr(self._local, "produto", {})
_produto_contexto = _ProdutoContexto()


def _gerar_slides_bytes(img_bytes_list, produto):
    """
    Wrapper seguro para gerar_slides_carrossel.
    Aceita tanto list[bytes] quanto bytes simples.
    Retorna list[bytes] sempre.
    """
    # Normaliza entrada
    if isinstance(img_bytes_list, (bytes, bytearray)):
        fotos = [bytes(img_bytes_list)]
    elif isinstance(img_bytes_list, list):
        fotos = [bytes(b) for b in img_bytes_list if b]
    else:
        fotos = []

    if not fotos:
        return []

    try:
        slides = gerar_slides_carrossel(fotos, produto)
        if slides:
            return slides
    except Exception as e:
        log("WARN", f"[SLIDES] gerar_slides_carrossel: {str(e)[:80]}")

    # fallback: imagem limpa 1:1 sem overlay
    try:
        from PIL import Image as _Img
        from io import BytesIO as _BIO
        img = _Img.open(_BIO(fotos[0])).convert("RGB")
        img.thumbnail((1080, 1080), _Img.LANCZOS)
        canvas = _Img.new("RGB", (1080, 1080), (255, 255, 255))
        pw, ph = img.size
        canvas.paste(img, ((1080-pw)//2, (1080-ph)//2))
        buf = _BIO()
        canvas.save(buf, "JPEG", quality=92)
        return [buf.getvalue()]
    except Exception:
        return [fotos[0]]


def shopee_buscar_imagens(shop_id, item_id, img_principal=""):
    """
    Busca múltiplas imagens do produto via __NEXT_DATA__ da página Shopee.
    Chamada SOMENTE na hora de postar (nunca na listagem de produtos).
    Timeout curto: se falhar, usa só a imagem principal.
    """
    try:
        if not shop_id or not item_id:
            return [img_principal] if img_principal else []

        # Constrói URL do produto
        prod_url = f"https://shopee.com.br/product/{shop_id}/{item_id}"
        hdrs = {
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/124.0.0.0 Safari/537.36"),
            "Accept":          "text/html,*/*;q=0.9",
            "Accept-Language": "pt-BR,pt;q=0.9",
            "Referer":         "https://shopee.com.br/",
        }
        r = requests.get(prod_url, headers=hdrs, timeout=8, allow_redirects=True)
        if r.status_code != 200:
            return [img_principal] if img_principal else []

        # Extrai JSON embutido no __NEXT_DATA__
        import re as _re
        m = _re.search(r'"images":\s*\[([^\]]+)\]', r.text)
        if m:
            raw = _re.findall(r'"([a-f0-9]{20,})"', m.group(1))
            if raw:
                urls = [f"https://cf.shopee.com.br/file/{h}" for h in raw[:8]]
                log("INFO", f"[IMG] {len(urls)} foto(s) via scraping para item {item_id}")
                return urls

        # Fallback: busca hashes direto no HTML (formato sg-XXXXX ou br-XXXXX)
        hashes = _re.findall(
            r'https?://(?:cf\.shopee\.com\.br|down-br\.img\.susercontent\.com)/file/([a-zA-Z0-9_-]{15,})',
            r.text)
        # Remove duplicatas mantendo ordem
        seen, unique = set(), []
        for h in hashes:
            if h not in seen:
                seen.add(h)
                unique.append(f"https://cf.shopee.com.br/file/{h}")
        if unique:
            log("INFO", f"[IMG] {len(unique[:8])} foto(s) via regex para item {item_id}")
            return unique[:8]

    except Exception as e:
        log("WARN", f"[IMG] scraping falhou para {item_id}: {str(e)[:50]}")

    return [img_principal] if img_principal else []


def shopee_montar_produto(i):
    """Monta dicionário de produto a partir dos dados da API Shopee"""
    price_raw  = i.get("price", 0) or i.get("price_min", 0)
    price      = price_raw / 100000 if price_raw > 1000 else float(price_raw)
    shop_id    = i.get("shopid", "")
    item_id    = i.get("itemid", "")
    img        = i.get("image", "")
    img_url    = f"https://cf.shopee.com.br/file/{img}" if img else ""
    product_url= f"https://shopee.com.br/product/{shop_id}/{item_id}"
    name       = (i.get("name") or i.get("title") or "Produto Shopee")[:100]
    all_images = i.get("images", [])
    image_urls = [f"https://cf.shopee.com.br/file/{im}" for im in all_images[:8] if im]
    if not image_urls and img_url:
        image_urls = [img_url]
    sold       = i.get("sold", 0) or i.get("historical_sold", 0)
    rating_obj = i.get("item_rating", {}) or {}
    rating     = round(float(rating_obj.get("rating_star", 0)), 1)
    desc       = (i.get("description") or i.get("desc") or "")[:200]
    commission = 0
    comm_info  = i.get("commission_info") or i.get("affiliate_info") or {}
    if comm_info:
        commission = round(float(comm_info.get("commission_percentage", 0)) / 100, 1)
    # Extrai video do produto (quando disponivel na API)
    video_info = i.get("video_info_list") or i.get("video_infos") or []
    video_url = ""
    if video_info and isinstance(video_info, list) and len(video_info) > 0:
        vi = video_info[0]
        video_url = vi.get("default_format", {}).get("video_url", "") or vi.get("url", "") or ""
    if not video_url:
        vid_raw = i.get("video_url","") or i.get("video_link","") or ""
        if vid_raw: video_url = vid_raw
    return {
        "name":       name,
        "description": desc,
        "price":      round(price, 2),
        "commission": commission,
        "image_url":  image_urls[0] if image_urls else "",
        "image_urls": image_urls,
        "video_url":  video_url,
        "product_url": product_url,
        "shop_id":    str(shop_id),
        "item_id":    str(item_id),
        "sold":       sold,
        "rating":     rating,
    }

def shopee_search(keyword, limit=5):
    import random, hashlib
    offset = random.randint(0, 30)

    headers_base = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Referer": f"https://shopee.com.br/search?keyword={keyword.strip()}",
        "Origin": "https://shopee.com.br",
        "X-Requested-With": "XMLHttpRequest",
        "X-API-SOURCE": "rn",
        "If-None-Match-": "*",
    }

    s = requests.Session()
    s.headers.update(headers_base)

    # ── Estratégia 1: API v4 search (mobile user-agent) ──
    try:
        s.get("https://shopee.com.br/", timeout=8)
        params = {
            "by": "relevancy", "keyword": keyword.strip(),
            "limit": limit + 3, "newest": offset,
            "order": "desc", "page_type": "search",
            "scenario": "PAGE_GLOBAL_SEARCH", "version": 2,
            "locations": "", "match_id": 0
        }
        r = s.get("https://shopee.com.br/api/v4/search/search_items",
                  params=params, timeout=15)
        data = r.json()
        items = data.get("items", [])
        if items:
            products = [shopee_montar_produto(it.get("item_basic", it)) for it in items]
            products = [p for p in products if p["name"] and p["image_url"]]
            if products:
                log("INFO", f"Shopee v4: {len(products)} produto(s) para '{keyword}'")
                return products[:limit]
    except Exception as e:
        log("WARN", f"Shopee v4 falhou: {str(e)[:60]}")

    # ── Estratégia 2: API recommend/trending ──
    try:
        r2 = s.get(
            "https://shopee.com.br/api/v4/recommend/recommend",
            params={"bundle": "top_picks_for_you", "limit": limit + 5,
                    "offset": offset, "type": 1},
            timeout=15)
        data2 = r2.json()
        items2 = (data2.get("data", {}) or {}).get("sections", [])
        raw = []
        for sec in items2:
            raw += sec.get("data", {}).get("item", [])
        if raw:
            products = [shopee_montar_produto(it) for it in raw]
            products = [p for p in products if p["name"] and p["image_url"]]
            if products:
                log("INFO", f"Shopee recommend: {len(products)} produto(s)")
                return products[:limit]
    except Exception as e:
        log("WARN", f"Shopee recommend falhou: {str(e)[:60]}")

    # ── Estratégia 3: API flash sale / ofertas do dia ──
    try:
        r3 = s.get(
            "https://shopee.com.br/api/v4/flash_sale/get_all_sessions",
            params={"limit": 1}, timeout=10)
        data3 = r3.json()
        session_id = ((data3.get("data") or {}).get("sessions") or [{}])[0].get("promotionid")
        if session_id:
            r4 = s.get(
                "https://shopee.com.br/api/v4/flash_sale/flash_sale_batch_get_items",
                params={"promotionid": session_id, "limit": limit + 5,
                        "offset": offset, "need_personalize": "false"},
                timeout=15)
            data4 = r4.json()
            items4 = (data4.get("data") or {}).get("items", [])
            if items4:
                products = [shopee_montar_produto(it.get("item_basic", it)) for it in items4]
                products = [p for p in products if p["name"] and p["image_url"]]
                if products:
                    log("INFO", f"Shopee flash: {len(products)} produto(s)")
                    return products[:limit]
    except Exception as e:
        log("WARN", f"Shopee flash falhou: {str(e)[:60]}")

    # ── Fallback: demo com imagem ÚNICA por produto (sem mismatch) ──
    log("WARN", f"Shopee API indisponivel. Usando demo para '{keyword}'")
    seed = int(hashlib.md5(f"{keyword}{offset}".encode()).hexdigest()[:8], 16)
    random.seed(seed)

    # Imagens temáticas por categoria — 1 imagem coerente por produto
    temas = {
        "maquiagem": ["https://images.unsplash.com/photo-1522335789203-aabd1fc54bc9?w=1080&h=1080&fit=crop",
                      "https://images.unsplash.com/photo-1596462502278-27bfdc403348?w=1080&h=1080&fit=crop"],
        "roupa": ["https://images.unsplash.com/photo-1445205170230-053b83016050?w=1080&h=1080&fit=crop",
                  "https://images.unsplash.com/photo-1479064555552-3ef4979f8908?w=1080&h=1080&fit=crop"],
        "tenis": ["https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=1080&h=1080&fit=crop",
                  "https://images.unsplash.com/photo-1600185365483-26d7a4cc7519?w=1080&h=1080&fit=crop"],
        "relogio": ["https://images.unsplash.com/photo-1523275335684-37898b6baf30?w=1080&h=1080&fit=crop",
                    "https://images.unsplash.com/photo-1547996160-81dfa63595aa?w=1080&h=1080&fit=crop"],
        "eletronico": ["https://images.unsplash.com/photo-1505740420928-5e560c06d30e?w=1080&h=1080&fit=crop",
                       "https://images.unsplash.com/photo-1526170375885-4d8ecf77b99f?w=1080&h=1080&fit=crop"],
        "perfume": ["https://images.unsplash.com/photo-1585386959984-a4155224a1ad?w=1080&h=1080&fit=crop",
                    "https://images.unsplash.com/photo-1541643600914-78b084683702?w=1080&h=1080&fit=crop"],
        "fitness": ["https://images.unsplash.com/photo-1517836357463-d25dfeac3438?w=1080&h=1080&fit=crop",
                    "https://images.unsplash.com/photo-1571019614242-c5c5dee9f50b?w=1080&h=1080&fit=crop"],
        "default": ["https://images.unsplash.com/photo-1607082348824-0a96f2a4b9da?w=1080&h=1080&fit=crop",
                    "https://images.unsplash.com/photo-1472851294608-062f824d29cc?w=1080&h=1080&fit=crop",
                    "https://images.unsplash.com/photo-1556742049-0cfed4f6a45d?w=1080&h=1080&fit=crop"],
    }
    kw_lower = keyword.lower()
    tema_imgs = next((v for k, v in temas.items() if k in kw_lower), temas["default"])

    precos  = [19.90, 24.90, 29.90, 34.90, 39.90, 44.90, 49.90, 54.90, 59.90, 79.90, 99.90]
    ratings = [4.5, 4.6, 4.7, 4.8, 4.9]
    vendidos= [320, 540, 780, 1100, 1500, 2300, 3800]
    adjetivos = ["Incrível","Top","Premium","Exclusivo","Original","Especial","Profissional","Ultra"]
    tipos    = ["Frete Grátis","Super Desconto","Imperdível","Oferta Relâmpago","Preço Baixo","Queima Estoque"]

    demos = []
    for i in range(limit):
        # CADA produto recebe UMA imagem temática consistente
        img_idx = i % len(tema_imgs)
        img     = tema_imgs[img_idx]
        nome    = f"{random.choice(adjetivos)} {keyword.title()} - {random.choice(tipos)}"
        preco   = random.choice(precos)
        demos.append({
            "name":        nome,
            "description": f"Produto de qualidade na categoria {keyword}. Compre agora com frete grátis!",
            "price":       preco,
            "commission":  round(random.uniform(5.0, 12.0), 1),
            "rating":      random.choice(ratings),
            "sold":        random.choice(vendidos),
            "image_url":   img,
            "image_urls":  [img],          # ← 1 única imagem coerente
            "product_url": f"https://shopee.com.br/search?keyword={keyword.strip()}",
            "shop_id":     "demo",
            "item_id":     f"demo_{i}_{offset}",
        })
    return demos


def gerar_link_afiliado(product_url, affiliate_id):
    if affiliate_id:
        if "?" in product_url:
            return f"{product_url}&smtt={affiliate_id}"
        return f"{product_url}?smtt={affiliate_id}"
    return product_url

# ════════════════════════════════════════════════════════
#  INSTAGRAM — Graph API
# ════════════════════════════════════════════════════════
def _ig_checar_limite_diario(access_token, ig_user_id):
    """
    Verifica quantos posts a conta já fez hoje via API.
    Limite oficial: 25 posts por 24h.
    Retorna (posts_usados, limite, pode_postar).
    """
    try:
        r = requests.get(
            f"https://graph.facebook.com/v19.0/{ig_user_id}/content_publishing_limit",
            params={
                "fields": "config,quota_usage",
                "access_token": access_token
            },
            timeout=15
        )
        d = r.json()
        dados = d.get("data", [{}])[0] if d.get("data") else {}
        usado  = dados.get("quota_usage", 0)
        config = dados.get("config", {})
        limite = config.get("quota_total", 25)
        pode   = usado < limite
        log("INFO", f"[IG] Limite diário: {usado}/{limite} posts usados | pode_postar={pode}")
        return usado, limite, pode
    except Exception as e:
        log("WARN", f"[IG] Não foi possível checar limite diário: {e}")
        return 0, 25, True  # assume que pode postar


def _ig_aguardar_container(creation_id, access_token):
    """
    Aguarda o container de mídia ficar FINISHED antes de publicar.
    Polling com backoff: 3s → 4s → 5s (max 10 tentativas ≈ 40s total).
    """
    log("INFO", f"[IG] Aguardando container {creation_id}...")
    espera = 3
    for tentativa in range(1, 11):   # max ~40s
        try:
            r = requests.get(
                f"https://graph.facebook.com/v19.0/{creation_id}",
                params={"fields": "status_code,status", "access_token": access_token},
                timeout=10
            )
            d = r.json()
            sc = d.get("status_code", "")
            log("INFO", f"[IG] Container #{tentativa}: {sc}")

            if sc == "FINISHED":
                log("INFO", f"[IG] ✅ Container pronto ({tentativa * espera}s)")
                return True, ""
            if sc == "ERROR":
                erro = d.get("status", "Erro processamento IG")
                log("ERROR", f"[IG] Container ERRO: {erro}")
                return False, f"Erro no container: {erro}"

            time.sleep(espera)
            espera = min(espera + 1, 5)   # backoff suave até 5s

        except Exception as e:
            log("WARN", f"[IG] poll container: {e}")
            time.sleep(espera)

    log("ERROR", "[IG] Timeout: container não ficou FINISHED em 40s")
    return False, "Timeout container"


def instagram_post(image_url, caption, access_token, ig_user_id):
    """
    Posta uma imagem no Instagram Feed via Graph API.
    Fluxo: verificar cota → criar container → aguardar FINISHED → publicar.
    Retry automático para erros transientes (code 2 / is_transient).
    """
    log("INFO", f"[IG] Iniciando post | img={image_url[:70]}")

    # Mantém a URL processada (com badge/preço) — é pública e acessível
    img_ok = image_url

    try:
        # ── Passo 0: Verificar token válido ────────────────────
        try:
            r_tok = requests.get(
                f"https://graph.facebook.com/v19.0/me",
                params={"access_token": access_token}, timeout=10)
            d_tok = r_tok.json()
            if "error" in d_tok:
                ec = d_tok["error"].get("code",0)
                em = d_tok["error"].get("message","")
                if ec in (190,102,4):
                    log("ERROR", f"[IG] TOKEN EXPIRADO ou inválido (code={ec}): {em[:60]}")
                    return False, f"TOKEN EXPIRADO — renove em /ig_setup (code={ec})"
        except Exception: pass

        # ── Passo 1: Verificar limite diário ───────────────────
        usado, limite, pode = _ig_checar_limite_diario(access_token, ig_user_id)
        if not pode:
            msg = f"❌ Limite diário atingido: {usado}/{limite} posts. Aguarde amanhã."
            log("ERROR", f"[IG] {msg}")
            return False, msg

        # ── Passo 2: Criar container ───────────────────────────
        creation_id = ""
        for tentativa in range(2):
            if tentativa > 0:
                time.sleep(5)
            r1 = requests.post(
                f"https://graph.facebook.com/v19.0/{ig_user_id}/media",
                data={"image_url": img_ok, "caption": caption, "access_token": access_token},
                timeout=45
            )
            d1 = r1.json()
            log("INFO", f"[IG] criar_media: {str(d1)[:200]}")
            if "error" in d1:
                code = d1["error"].get("code", 0)
                msg  = d1["error"].get("message","Erro")
                if code in (190, 102):
                    return False, f"TOKEN EXPIRADO (code={code}) — renove em /ig_setup"
                if d1["error"].get("is_transient") and tentativa == 0:
                    log("WARN", "[IG] Transiente, retry..."); continue
                log("ERROR", f"[IG] criar_media erro: {msg}")
                return False, msg
            creation_id = d1.get("id","")
            if creation_id: break

        if not creation_id:
            return False, "Sem creation_id"

        # ── Passo 2: Aguardar FINISHED ────────────────────────
        pronto, motivo = _ig_aguardar_container(creation_id, access_token)
        if not pronto:
            return False, motivo

        # ── Passo 3: Publicar ────────────────────────────────────
        r2 = requests.post(
            f"https://graph.facebook.com/v19.0/{ig_user_id}/media_publish",
            data={"creation_id": creation_id, "access_token": access_token},
            timeout=45
        )
        d2 = r2.json()
        log("INFO", f"[IG] publicar: {str(d2)[:150]}")
        if "error" in d2:
            msg = d2["error"].get("message","Erro ao publicar")
            log("ERROR", f"[IG] Erro publicar: {msg}")
            return False, msg
        post_id = d2.get("id","")
        if post_id:
            log("INFO", f"[IG] ✅ POSTADO! post_id={post_id}")
            return True, post_id
        return False, "Sem post_id na resposta"

    except requests.exceptions.Timeout:
        log("ERROR", "[IG] Timeout")
        return False, "Timeout"
    except Exception as e:
        log("ERROR", f"[IG] Exceção: {str(e)}")
        return False, str(e)


def preparar_imagem_story(img_url, produto):
    """
    Gera uma imagem 1080×1920 (9:16) para Story com:
      - Fundo degradê escuro
      - Foto do produto centralizada com borda laranja
      - Badge vermelho "X% OFF"
      - Preço original riscado + preço atual em destaque
      - Faixa "OFERTA IMPERDÍVEL ⚡" no topo
      - "LINK NA BIO 👇 @brizzah.br" no rodapé
    Hospeda via /slide/<key> e retorna URL pública.
    """
    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO as _BIO
    import os as _os

    W, H = 1080, 1920
    LARANJA   = (238, 77, 45)
    LARANJA_C = (255, 120, 50)
    VERDE     = (0, 229, 180)
    BRANCO    = (255, 255, 255)
    PRETO     = (10, 10, 10)
    VERMELHO  = (220, 30, 30)
    CINZA_ESC = (30, 30, 30)

    try:
        fnt_huge  = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 96)
        fnt_big   = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 72)
        fnt_mid   = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 54)
        fnt_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 40)
        fnt_xs    = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",      34)
    except:
        fnt_huge = fnt_big = fnt_mid = fnt_small = fnt_xs = ImageFont.load_default()

    # ── Canvas com fundo degradê escuro ──────────────────────────
    canvas = Image.new("RGB", (W, H), PRETO)
    d = ImageDraw.Draw(canvas)

    # Gradiente vertical manual (preto → cinza escuro → preto)
    for y in range(H):
        t = y / H
        if t < 0.5:
            c = int(10 + 25 * (t / 0.5))
        else:
            c = int(35 - 25 * ((t - 0.5) / 0.5))
        d.line([(0, y), (W, y)], fill=(c, c, c))

    # ── TOPO: faixa laranja "OFERTA IMPERDÍVEL ⚡" ────────────────
    d.rectangle([0, 0, W, 130], fill=LARANJA)
    txt_topo = "OFERTA IMPERDIVEL!"
    try:
        bb = d.textbbox((0,0), txt_topo, font=fnt_mid)
        tx = (W - (bb[2]-bb[0])) // 2
    except:
        tx = 80
    d.text((tx, 30), txt_topo, font=fnt_mid, fill=BRANCO)

    # ── Logo Brizzah (texto simulado) ────────────────────────────
    d.text((40, 150), "Brizzah", font=fnt_mid, fill=VERDE)
    try:
        bb2 = d.textbbox((0,0), "Achados Inteligentes", font=fnt_xs)
        d.text((42, 215), "Achados Inteligentes", font=fnt_xs, fill=(180,180,180))
    except:
        pass

    # ── Foto do produto centralizada ─────────────────────────────
    foto_y_start = 280
    foto_size    = 860  # quadrado dentro do story
    foto_x       = (W - foto_size) // 2

    try:
        ri = requests.get(img_url, timeout=10)
        foto = Image.open(_BIO(ri.content)).convert("RGB")
        fw, fh = foto.size
        esc = min(foto_size / fw, foto_size / fh)
        nw, nh = max(1, int(fw * esc)), max(1, int(fh * esc))
        foto_r = foto.resize((nw, nh), Image.LANCZOS)

        # Fundo branco para a foto
        foto_bg = Image.new("RGB", (foto_size, foto_size), (252, 252, 252))
        px = (foto_size - nw) // 2
        py = (foto_size - nh) // 2
        foto_bg.paste(foto_r, (px, py))

        # Borda laranja
        d.rectangle([foto_x - 4, foto_y_start - 4,
                     foto_x + foto_size + 4, foto_y_start + foto_size + 4],
                    outline=LARANJA, width=6)
        canvas.paste(foto_bg, (foto_x, foto_y_start))
        foto.close(); foto_r.close(); foto_bg.close()
    except Exception as fe:
        log("WARN", f"[STORY-IMG] foto: {str(fe)[:60]}")

    # ── Constantes de layout (definidas aqui para uso em todo o bloco) ──
    footer_h = 150   # altura do rodapé verde

    # ── Dados do produto ──────────────────────────────────────────
    preco_atual  = float(produto.get("price",    0) or 0)
    preco_orig   = float(produto.get("original_price", 0) or preco_atual * 1.4)
    if preco_orig <= preco_atual:
        preco_orig = preco_atual * 1.4
    desconto = int(round((1 - preco_atual / preco_orig) * 100)) if preco_orig > 0 else 0
    nome = (produto.get("name") or "Produto em destaque")  # sem corte — mostra completo

    preco_txt    = f"R$ {preco_atual:,.2f}".replace(",","X").replace(".",",").replace("X",".")
    orig_txt     = f"R$ {preco_orig:,.2f}".replace(",","X").replace(".",",").replace("X",".")
    pct_txt      = f"{desconto}% OFF" if desconto > 0 else "OFERTA"

    area_y = foto_y_start + foto_size + 20

    # ── Badge % OFF ───────────────────────────────────────────────
    if desconto > 0:
        try:
            bb_pct = d.textbbox((0,0), pct_txt, font=fnt_big)
            bw = bb_pct[2] - bb_pct[0] + 48
            bh = bb_pct[3] - bb_pct[1] + 24
        except:
            bw, bh = 220, 100
        bx = (W - bw) // 2
        d.rounded_rectangle([bx, area_y, bx + bw, area_y + bh], radius=16, fill=VERMELHO)
        try:
            tx_off = bx + (bw - (bb_pct[2]-bb_pct[0])) // 2
        except:
            tx_off = bx + 24
        d.text((tx_off, area_y + 12), pct_txt, font=fnt_big, fill=BRANCO)
        area_y += bh + 18

    # ── Preço original riscado ────────────────────────────────────
    try:
        bb_orig = d.textbbox((0,0), f"de {orig_txt}", font=fnt_small)
        ox = (W - (bb_orig[2]-bb_orig[0])) // 2
        d.text((ox, area_y), f"de {orig_txt}", font=fnt_small, fill=(160,160,160))
        # risca o texto
        my = area_y + (bb_orig[3]-bb_orig[1])//2
        d.line([(ox, my), (ox + bb_orig[2]-bb_orig[0], my)], fill=(160,160,160), width=3)
        area_y += (bb_orig[3]-bb_orig[1]) + 10
    except:
        area_y += 50

    # ── Preço atual em destaque ───────────────────────────────────
    try:
        bb_p = d.textbbox((0,0), f"por {preco_txt}", font=fnt_huge)
        px2 = (W - (bb_p[2]-bb_p[0])) // 2
    except:
        px2 = 60
    d.text((px2, area_y), f"por {preco_txt}", font=fnt_huge, fill=LARANJA_C)
    area_y += 110

    # ── Nome do produto — quebra automática em quantas linhas precisar ──
    def _quebrar_texto(texto, fonte, largura_max):
        """Divide o texto em linhas que cabem dentro de largura_max px."""
        palavras = texto.split()
        linhas = []
        linha_atual = ""
        for pal in palavras:
            teste = (linha_atual + " " + pal).strip()
            try:
                bb = d.textbbox((0, 0), teste, font=fonte)
                if bb[2] - bb[0] <= largura_max:
                    linha_atual = teste
                else:
                    if linha_atual:
                        linhas.append(linha_atual)
                    linha_atual = pal
            except:
                linha_atual = teste
        if linha_atual:
            linhas.append(linha_atual)
        return linhas

    MARGEM_NOME = 60          # margem lateral total (30px cada lado)
    LARGURA_NOME = W - MARGEM_NOME

    # Tenta fonte normal; se gerar muitas linhas, reduz o tamanho
    for tamanho_fonte in [34, 30, 26, 22]:
        try:
            fnt_nome = ImageFont.truetype(
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", tamanho_fonte)
        except:
            fnt_nome = ImageFont.load_default()
        linhas_nome = _quebrar_texto(nome, fnt_nome, LARGURA_NOME)
        altura_total = len(linhas_nome) * (tamanho_fonte + 8)
        espaco_disponivel = (H - footer_h - 30) - area_y
        if altura_total <= espaco_disponivel:
            break   # cabe! usa esse tamanho

    for ln in linhas_nome:
        try:
            bb_l = d.textbbox((0, 0), ln, font=fnt_nome)
            lx = (W - (bb_l[2] - bb_l[0])) // 2
        except:
            lx = MARGEM_NOME // 2
        d.text((lx, area_y), ln, font=fnt_nome, fill=(210, 210, 210))
        area_y += tamanho_fonte + 8

    # ── RODAPÉ: "LINK NA BIO @brizzah.br" ────────────────────────
    d.rectangle([0, H - footer_h, W, H], fill=VERDE)
    rodape1 = "LINK NA BIO"
    rodape2 = "@brizzah.br"
    try:
        bb_r1 = d.textbbox((0,0), rodape1, font=fnt_mid)
        bb_r2 = d.textbbox((0,0), rodape2, font=fnt_big)
        d.text(((W-(bb_r1[2]-bb_r1[0]))//2, H-footer_h+12), rodape1, font=fnt_mid, fill=PRETO)
        d.text(((W-(bb_r2[2]-bb_r2[0]))//2, H-footer_h+68), rodape2, font=fnt_big, fill=PRETO)
    except:
        d.text((100, H-footer_h+20), "LINK NA BIO @brizzah.br", font=fnt_mid, fill=PRETO)

    # ── Seta decorativa apontando para baixo ─────────────────────
    cx2 = W // 2
    d.polygon([(cx2-30, H-footer_h-50), (cx2+30, H-footer_h-50), (cx2, H-footer_h-10)],
              fill=VERDE)

    # ── Hospeda em disco ─────────────────────────────────────────
    buf = _BIO()
    canvas.save(buf, "JPEG", quality=92, optimize=True)
    img_bytes = buf.getvalue()
    canvas.close()

    key = f"story_{int(time.time()*1000)}"
    _os.makedirs(_SLIDE_DIR, exist_ok=True)
    fpath = _os.path.join(_SLIDE_DIR, key + ".jpg")
    with open(fpath, "wb") as _f:
        _f.write(img_bytes)
    del img_bytes, buf

    host = (cfg("bot_url","") or os.environ.get("BOT_URL","https://shopee-bot-jt11.onrender.com")).rstrip("/")
    return f"{host}/slide/{key}"


def instagram_story_post(image_url, access_token, ig_user_id, produto=None):
    """
    Gera imagem 9:16 com arte completa (preço, desconto, CTA) e posta como Story.
    Se produto for fornecido, usa preparar_imagem_story para montar o visual.
    """
    # Gera imagem do story se tiver dados do produto
    if produto:
        story_url = preparar_imagem_story(image_url, produto)
        if story_url:
            image_url = story_url
            log("INFO", f"[IG-STORY] Imagem story gerada: {story_url[:80]}")
        else:
            log("WARN", "[IG-STORY] Falha ao gerar imagem story, usando foto original")

    log("INFO", f"[IG-STORY] Iniciando story | img={image_url[:70]}")
    try:
        # Cria container com media_type=STORIES
        r1 = requests.post(
            f"https://graph.facebook.com/v19.0/{ig_user_id}/media",
            data={
                "image_url":    image_url,
                "media_type":   "STORIES",
                "access_token": access_token,
            },
            timeout=45
        )
        d1 = r1.json()
        log("INFO", f"[IG-STORY] criar_container: {str(d1)[:150]}")
        if "error" in d1:
            err = d1["error"].get("message","Erro desconhecido")
            log("ERROR", f"[IG-STORY] Erro container: {err}")
            return False, err
        creation_id = d1.get("id","")
        if not creation_id:
            return False, "Sem creation_id"

        # Aguarda FINISHED
        pronto, motivo = _ig_aguardar_container(creation_id, access_token)
        if not pronto:
            return False, motivo

        # Publica
        r2 = requests.post(
            f"https://graph.facebook.com/v19.0/{ig_user_id}/media_publish",
            data={"creation_id": creation_id, "access_token": access_token},
            timeout=45
        )
        d2 = r2.json()
        log("INFO", f"[IG-STORY] publicar: {str(d2)[:150]}")
        if "error" in d2:
            err = d2["error"].get("message","Erro ao publicar")
            log("ERROR", f"[IG-STORY] Erro publicar: {err}")
            return False, err
        post_id = d2.get("id","")
        log("INFO", f"[IG-STORY] ✅ Story publicado! ID={post_id}")
        return True, post_id
    except Exception as e:
        log("ERROR", f"[IG-STORY] Exceção: {str(e)}")
        return False, str(e)


def _ig_aguardar_item_carrossel(item_id, access_token, max_tentativas=8):
    """Aguarda item de carrossel ficar FINISHED. Polling com backoff curto."""
    for t in range(max_tentativas):
        try:
            r = requests.get(
                f"https://graph.facebook.com/v19.0/{item_id}",
                params={"fields": "status_code", "access_token": access_token},
                timeout=12)
            status = r.json().get("status_code", "")
            if status == "FINISHED":
                return True
            if status == "ERROR":
                log("WARN", f"[IG] Item {item_id} ERROR")
                return False
            # Backoff: 2s, 3s, 4s, 5s, 5s, 5s, 5s, 5s
            wait = min(2 + t, 5)
            time.sleep(wait)
        except Exception as e:
            log("WARN", f"[IG] poll item: {str(e)[:40]}")
            time.sleep(3)
    return False


def instagram_carousel_post(image_urls, caption, access_token, ig_user_id):
    """
    Carrossel Instagram — arquitetura otimizada:
    1. Cria todos os containers de item em PARALELO (ThreadPoolExecutor)
    2. Aguarda status FINISHED em paralelo
    3. Cria container de carrossel e publica
    Fallback automático para post simples em qualquer falha.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    try:
        valid_urls = [u.strip() for u in image_urls
                      if u and u.strip().startswith("http")][:10]
        if not valid_urls:
            return False, "Sem URLs válidas"

        log("INFO", f"[IG] Carrossel: {len(valid_urls)} foto(s)")

        if len(valid_urls) == 1:
            log("INFO", "[IG] 1 foto → post simples")
            return instagram_post(valid_urls[0], caption, access_token, ig_user_id)

        # ── PASSO 1: cria containers de item em PARALELO ──────────────
        def _criar_item(img_url):
            try:
                r = requests.post(
                    f"https://graph.facebook.com/v19.0/{ig_user_id}/media",
                    data={"image_url":        img_url,
                          "is_carousel_item": "true",
                          "access_token":     access_token},
                    timeout=30)
                d = r.json()
                if "error" in d:
                    log("WARN", f"[IG] Item rejeitado: {d['error'].get('message','')[:50]}")
                    return None
                return d.get("id")
            except Exception as ex:
                log("WARN", f"[IG] Criar item: {str(ex)[:40]}")
                return None

        log("INFO", "[IG] Criando containers em paralelo...")
        item_ids = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futuros = [ex.submit(_criar_item, u) for u in valid_urls]
            for fut in as_completed(futuros):
                cid = fut.result()
                if cid:
                    item_ids.append(cid)

        log("INFO", f"[IG] {len(item_ids)} containers criados")

        # ── PASSO 2: aguarda FINISHED em PARALELO ────────────────────
        if item_ids:
            aprovados = []
            with ThreadPoolExecutor(max_workers=4) as ex:
                fut_map = {ex.submit(_ig_aguardar_item_carrossel, cid, access_token): cid
                           for cid in item_ids}
                for fut in as_completed(fut_map):
                    if fut.result():
                        aprovados.append(fut_map[fut])
        else:
            aprovados = []

        log("INFO", f"[IG] {len(aprovados)} item(s) aprovados (FINISHED)")

        if len(aprovados) < 2:
            log("INFO", "[IG] < 2 aprovados → post simples")
            return instagram_post(valid_urls[0], caption, access_token, ig_user_id)

        # ── PASSO 3: container de carrossel ───────────────────────────
        log("INFO", f"[IG] Criando carrossel com {len(aprovados)} fotos...")
        r2 = requests.post(
            f"https://graph.facebook.com/v19.0/{ig_user_id}/media",
            data={"media_type":   "CAROUSEL",
                  "children":     ",".join(aprovados),
                  "caption":      caption,
                  "access_token": access_token},
            timeout=30)
        d2 = r2.json()
        if "error" in d2:
            err = d2["error"].get("message", "")
            log("WARN", f"[IG] Carrossel container: {err[:60]} → simples")
            return instagram_post(valid_urls[0], caption, access_token, ig_user_id)

        carousel_id = d2.get("id")
        if not carousel_id:
            return instagram_post(valid_urls[0], caption, access_token, ig_user_id)

        # ── PASSO 4: aguarda carrossel FINISHED ───────────────────────
        ok_wait, motivo = _ig_aguardar_container(carousel_id, access_token)
        if not ok_wait:
            log("WARN", f"[IG] Carrossel não ficou pronto: {motivo} → simples")
            return instagram_post(valid_urls[0], caption, access_token, ig_user_id)

        # ── PASSO 5: publica ──────────────────────────────────────────
        log("INFO", f"[IG] Publicando carrossel {carousel_id}...")
        r3 = requests.post(
            f"https://graph.facebook.com/v19.0/{ig_user_id}/media_publish",
            data={"creation_id":  carousel_id,
                  "access_token": access_token},
            timeout=30)
        d3 = r3.json()
        if "error" in d3:
            err = d3["error"].get("message", "")
            log("ERROR", f"[IG] Publicar carrossel: {err[:70]}")
            return False, err

        post_id = d3.get("id", "")
        log("INFO", f"[IG] ✅ Carrossel publicado! ID={post_id} ({len(aprovados)} fotos)")
        return True, post_id

    except Exception as e:
        log("ERROR", f"[IG] Exceção carrossel: {str(e)[:80]}")
        return False, str(e)


# ════════════════════════════════════════════════════════
#  INSTAGRAM REELS — vídeo de produto (Ken Burns + ffmpeg)
# ════════════════════════════════════════════════════════
def criar_video_produto(img_url, produto, duracao=12):
    """
    Cria um vídeo de 12 segundos a partir da foto do produto.
    Aplica efeito Ken Burns (zoom+pan suave) via ffmpeg.
    Adiciona overlay de preço no canto inferior.
    Retorna caminho do arquivo .mp4 temporário, ou None se falhar.
    """
    import subprocess, tempfile, os, requests
    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO as _BIO

    try:
        # Baixa imagem
        r = requests.get(img_url, timeout=8)
        if r.status_code != 200 or len(r.content) < 2000:
            return None

        # Prepara frame 1080×1080 com overlay de preço
        img = Image.open(_BIO(r.content)).convert("RGB")
        W, H = 1080, 1920   # 9:16 correto para Reels
        canvas = Image.new("RGB", (W, H), (15, 15, 15))
        # Foto ocupa a parte central (quadrado 1080x1080)
        FOTO_SZ = 1080
        w, h = img.size
        esc = min(FOTO_SZ/w, FOTO_SZ/h)
        nw, nh = max(1, int(w*esc)), max(1, int(h*esc))
        foto_y = (H - FOTO_SZ) // 2
        canvas.paste(img.resize((nw, nh), Image.LANCZOS),
                     ((FOTO_SZ-nw)//2, foto_y + (FOTO_SZ-nh)//2))

        d   = ImageDraw.Draw(canvas)
        LARANJA = (238, 77, 45)
        BRANCO  = (255, 255, 255)
        PRETO   = (18, 18, 18)
        VERDE   = (0, 229, 180)
        try:
            fnt_b  = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 32)
            fnt_p  = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 80)
            fnt_sm = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 44)
            fnt_xs = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 36)
        except:
            fnt_b = fnt_p = fnt_sm = fnt_xs = ImageFont.load_default()

        # ── Topo: faixa laranja com nome do produto ───────────────
        d.rectangle([0, 0, W, 160], fill=LARANJA)
        nome = (produto.get("name") or "Produto Shopee")[:40]
        try:
            bb_n = d.textbbox((0,0), nome, font=fnt_b)
            nx = (W - (bb_n[2]-bb_n[0])) // 2
        except: nx = 40
        d.text((nx, 55), nome, font=fnt_b, fill=BRANCO)

        # ── Preço e desconto no rodapé ────────────────────────────
        preco_atual = float(produto.get("price", 0) or 0)
        preco_orig  = float(produto.get("original_price", 0) or preco_atual * 1.4)
        if preco_orig <= preco_atual: preco_orig = preco_atual * 1.4
        desconto = int(round((1 - preco_atual / preco_orig) * 100)) if preco_orig > 0 else 0

        preco_txt = f"R$ {preco_atual:,.2f}".replace(",","X").replace(".",",").replace("X",".")
        orig_txt  = f"R$ {preco_orig:,.2f}".replace(",","X").replace(".",",").replace("X",".")

        d.rectangle([0, H-280, W, H], fill=(18,18,18))

        # Badge % OFF
        if desconto > 0:
            pct_txt = f"  {desconto}% OFF  "
            try:
                bb_off = d.textbbox((0,0), pct_txt, font=fnt_sm)
                d.rounded_rectangle([30, H-270, 30+(bb_off[2]-bb_off[0])+20, H-200],
                                    radius=12, fill=(220,30,30))
                d.text((40, H-268), pct_txt, font=fnt_sm, fill=BRANCO)
            except: pass

        # Preço original riscado
        try:
            bb_o = d.textbbox((0,0), f"de {orig_txt}", font=fnt_xs)
            d.text((30, H-190), f"de {orig_txt}", font=fnt_xs, fill=(130,130,130))
            my2 = H-190 + (bb_o[3]-bb_o[1])//2
            d.line([(30, my2), (30+bb_o[2]-bb_o[0], my2)], fill=(130,130,130), width=3)
        except: pass

        # Preço atual
        try:
            bb_pr = d.textbbox((0,0), f"por {preco_txt}", font=fnt_p)
            px3 = (W - (bb_pr[2]-bb_pr[0])) // 2
        except: px3 = 30
        d.text((px3, H-155), f"por {preco_txt}", font=fnt_p, fill=LARANJA)

        # Link na bio
        lnb = "LINK NA BIO @brizzah.br"
        try:
            bb_lnb = d.textbbox((0,0), lnb, font=fnt_sm)
            lx = (W - (bb_lnb[2]-bb_lnb[0])) // 2
        except: lx = 40
        d.text((lx, H-50), lnb, font=fnt_sm, fill=VERDE)

        # Salva frame preparado
        tmp_frame = tempfile.mktemp(suffix=".jpg")
        canvas.save(tmp_frame, "JPEG", quality=95)

        # ffmpeg: Ken Burns (zoom 1.0→1.25 em <duracao> segundos)
        tmp_video = tempfile.mktemp(suffix=".mp4")
        fps  = 25
        total_frames = duracao * fps
        cmd = [
            "ffmpeg", "-y",
            "-loop", "1",
            "-i", tmp_frame,
            "-vf", "scale=1080:1920:force_original_aspect_ratio=decrease,pad=1080:1920:(ow-iw)/2:(oh-ih)/2:color=black",
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-tune", "stillimage",
            "-crf", "30",
            "-pix_fmt", "yuv420p",
            "-t", str(duracao),
            "-r", "24",
            "-movflags", "+faststart",
            tmp_video,
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=90)
        os.unlink(tmp_frame)

        if result.returncode != 0:
            log("WARN", f"[REELS] ffmpeg erro: {result.stderr[-100:]}")
            return None

        size_mb = os.path.getsize(tmp_video) / (1024 * 1024)
        log("INFO", f"[REELS] Vídeo criado: {size_mb:.1f}MB")
        return tmp_video

    except Exception as e:
        log("WARN", f"[REELS] criar_video_produto: {str(e)[:80]}")
        return None


def instagram_reels_post(img_url, caption, access_token, ig_user_id,
                         produto=None, bot_url=""):
    """
    Posta um Reel no Instagram a partir da foto do produto.
    Fluxo: cria vídeo local → hospeda como /slide/<key> →
           Graph API: cria container REELS → aguarda FINISHED → publica.
    Fallback para post de imagem simples se ffmpeg não disponível.
    """
    import os, requests as _req, time as _time

    # ── ETAPA 1: Cria o vídeo ───────────────────────────────────────
    prod = produto or {}
    video_path = criar_video_produto(img_url, prod)

    if not video_path:
        log("WARN", "[REELS] Vídeo não criado, usando post de imagem")
        return instagram_post(img_url, caption, access_token, ig_user_id)

    try:
        # ── ETAPA 2: Re-hospeda o vídeo via /slide/<key> ─────────────
        with open(video_path, "rb") as f:
            video_bytes = f.read()
        os.unlink(video_path)

        key = f"reel_{int(_time.time())}"
        # Salva em disco (não usa _slide_store em memória)
        import os as _os
        _os.makedirs(_SLIDE_DIR, exist_ok=True)
        _vpath = _os.path.join(_SLIDE_DIR, key + ".mp4")
        with open(_vpath, "wb") as _vf:
            _vf.write(video_bytes)
        del video_bytes

        host = (bot_url or cfg("bot_url", "") or
                os.environ.get("BOT_URL", "https://shopee-bot-jt11.onrender.com")).rstrip("/")
        video_url = f"{host}/slide/{key}"
        log("INFO", f"[REELS] Hospedado: {video_url}")

        # ── ETAPA 3: Cria container no Instagram ─────────────────────
        r1 = _req.post(
            f"https://graph.facebook.com/v19.0/{ig_user_id}/media",
            data={
                "media_type":    "REELS",
                "video_url":     video_url,
                "caption":       caption,
                "share_to_feed": "true",
                "access_token":  access_token,
            },
            timeout=30,
        )
        d1 = r1.json()
        if "error" in d1:
            err = d1["error"].get("message", str(d1["error"]))
            log("WARN", f"[REELS] Container: {err[:80]}")
            return instagram_post(img_url, caption, access_token, ig_user_id)

        creation_id = d1.get("id")
        log("INFO", f"[REELS] Container criado: {creation_id}")

        # ── ETAPA 4: Aguarda FINISHED ─────────────────────────────────
        for tentativa in range(15):
            _time.sleep(5)
            rs = _req.get(
                f"https://graph.facebook.com/v19.0/{creation_id}",
                params={"fields": "status_code,status", "access_token": access_token},
                timeout=15,
            )
            status = rs.json().get("status_code", "")
            log("INFO", f"[REELS] Status #{tentativa+1}: {status}")
            if status == "FINISHED":
                break
            if status in ("ERROR", "EXPIRED"):
                log("WARN", f"[REELS] Processamento falhou: {status}")
                return instagram_post(img_url, caption, access_token, ig_user_id)

        # ── ETAPA 5: Publica ─────────────────────────────────────────
        r2 = _req.post(
            f"https://graph.facebook.com/v19.0/{ig_user_id}/media_publish",
            data={"creation_id": creation_id, "access_token": access_token},
            timeout=30,
        )
        d2 = r2.json()
        if "error" in d2:
            err = d2["error"].get("message", str(d2["error"]))
            log("WARN", f"[REELS] Publicação: {err[:80]}")
            return instagram_post(img_url, caption, access_token, ig_user_id)

        post_id = d2.get("id", "?")
        log("INFO", f"✅ [REELS] Publicado! ID={post_id}")
        return True, post_id

    except Exception as e:
        log("WARN", f"[REELS] Erro: {str(e)[:80]}")
        return instagram_post(img_url, caption, access_token, ig_user_id)


# ════════════════════════════════════════════════════════
#  CARROSSEL DE NICHO — 5 produtos reais, 5 fotos diferentes
# ════════════════════════════════════════════════════════

def buscar_video_shopee(shop_id, item_id):
    """Tenta buscar a URL do video de um produto Shopee via API publica."""
    try:
        import requests as _req
        hdrs = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 11) AppleWebKit/537.36 Chrome/120.0.0.0",
            "Referer": f"https://shopee.com.br/product/{shop_id}/{item_id}",
        }
        r = _req.get(
            f"https://shopee.com.br/api/v4/item/get?itemid={item_id}&shopid={shop_id}",
            headers=hdrs, timeout=10)
        data = r.json()
        item = (data.get("data") or data.get("item") or {})
        videos = item.get("video_info_list") or item.get("video_infos") or []
        if videos:
            vi = videos[0]
            url = vi.get("default_format",{}).get("video_url","") or vi.get("url","")
            if url:
                log("INFO", f"[VIDEO] Shopee video encontrado: {url[:60]}")
                return url
        # fallback: tenta CDN direto
        cdn = f"https://cf.shopee.com.br/file/{item.get('video',{}).get('video_id','')}"
        if len(cdn) > 50: return cdn
    except Exception as e:
        log("WARN", f"[VIDEO] Erro buscar video: {str(e)[:60]}")
    return ""


def postar_video_instagram(video_url, caption, ig_token, ig_uid, tipo="REELS"):
    """
    Posta um video (MP4) no Instagram como Reels, Feed ou Story.
    tipo: REELS | FEED | STORIES
    """
    try:
        log("INFO", f"[IG-VIDEO] Iniciando post tipo={tipo} url={video_url[:60]}")
        media_type = "REELS" if tipo == "REELS" else tipo
        # Cria container
        r1 = requests.post(
            f"https://graph.facebook.com/v18.0/{ig_uid}/media",
            data={
                "video_url":   video_url,
                "media_type":  media_type,
                "caption":     caption[:2200],
                "access_token": ig_token,
                **({"share_to_feed": "true"} if tipo == "REELS" else {}),
            }, timeout=60)
        d1 = r1.json()
        if "error" in d1:
            log("ERROR", f"[IG-VIDEO] Erro criar container: {d1['error'].get('message','')[:80]}")
            return False, d1["error"].get("message","")
        creation_id = d1.get("id")
        if not creation_id:
            return False, "Container sem ID"
        log("INFO", f"[IG-VIDEO] Container criado: {creation_id} — aguardando processamento...")
        # Aguarda processamento (videos levam mais tempo)
        import time as _t
        for tentativa in range(20):
            _t.sleep(8)
            r_check = requests.get(
                f"https://graph.facebook.com/v18.0/{creation_id}",
                params={"fields":"status_code,status","access_token":ig_token}, timeout=15)
            status = r_check.json().get("status_code","")
            log("INFO", f"[IG-VIDEO] Status #{tentativa+1}: {status}")
            if status == "FINISHED": break
            if status in ("ERROR","EXPIRED"):
                return False, f"Processamento falhou: {status}"
        # Publica
        r2 = requests.post(
            f"https://graph.facebook.com/v18.0/{ig_uid}/media_publish",
            data={"creation_id": creation_id, "access_token": ig_token}, timeout=30)
        d2 = r2.json()
        if "id" in d2:
            log("INFO", f"[IG-VIDEO] ✅ Video publicado! ID={d2['id']} tipo={tipo}")
            return True, d2["id"]
        err = d2.get("error",{}).get("message","Erro desconhecido")
        log("ERROR", f"[IG-VIDEO] ❌ Falha publicar: {err[:80]}")
        return False, err
    except Exception as e:
        log("ERROR", f"[IG-VIDEO] Excecao: {str(e)[:80]}")
        return False, str(e)


def gerar_carrossel_nicho(keyword, n=5):
    """
    Busca N produtos diferentes do mesmo nicho/keyword via API de afiliados.
    Para cada produto, baixa sua foto real da Shopee e monta um slide profissional:
      - Foto real do produto (imagem diferente em cada slide)
      - Nome, preço, vendidos
      - Badge shopee + numeração X/N
    Retorna lista de bytes (slides prontos para re-hospedar).
    """
    import requests as _req
    from PIL import Image, ImageDraw, ImageFont
    from io import BytesIO as _BIO

    # Busca N+2 produtos (margem para falhas de download)
    produtos_raw = shopee_api_buscar_produtos(keyword, limit=n + 2)
    if not produtos_raw:
        return []

    W, H       = 1080, 1080
    LARANJA    = (238, 77, 45)
    BRANCO     = (255, 255, 255)
    PRETO      = (18, 18, 18)
    CINZA      = (120, 120, 120)
    BG         = (252, 252, 252)
    PAINEL_H   = int(H * 0.28)   # 28% inferior para info do produto
    PROD_H     = H - PAINEL_H

    FONT_B = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    FONT_R = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"

    def fnt(path, sz):
        try:   return ImageFont.truetype(path, sz)
        except: return ImageFont.load_default()

    slides = []
    affiliate_id = cfg("shopee_affiliate_id", "")

    for p in produtos_raw:
        if len(slides) >= n:
            break

        img_url = p.get("image_url", "")
        if not img_url:
            continue

        try:
            ri = _req.get(img_url, timeout=6)
            if ri.status_code != 200 or len(ri.content) < 3000:
                continue
            prod_img = Image.open(_BIO(ri.content)).convert("RGB")
        except Exception:
            continue

        # ── Canvas ──────────────────────────────────────────────────
        canvas = Image.new("RGB", (W, H), BG)

        # Área do produto: centralizada nos PROD_H superiores
        pw, ph = prod_img.size
        esc    = min(W / pw, PROD_H / ph)
        nw, nh = max(1, int(pw * esc)), max(1, int(ph * esc))
        canvas.paste(prod_img.resize((nw, nh), Image.LANCZOS),
                     ((W - nw) // 2, (PROD_H - nh) // 2))

        d = ImageDraw.Draw(canvas)

        # ── Linha separadora ─────────────────────────────────────────
        d.rectangle([0, PROD_H, W, PROD_H + 2], fill=(215, 215, 215))

        # ── Painel inferior ──────────────────────────────────────────
        nome  = (p.get("name") or "Produto Shopee")[:38]
        preco = float(p.get("price") or 0)
        sold  = int(p.get("sold") or 0)
        preco_txt = f"R$ {preco:.2f}".replace(".", ",")

        # Nome (até 2 linhas)
        n1 = nome[:28]
        n2 = nome[28:] if len(nome) > 28 else ""
        yp = PROD_H + 14
        d.text((20, yp),       n1, font=fnt(FONT_B, 30), fill=PRETO)
        if n2:
            d.text((20, yp + 38), n2, font=fnt(FONT_R, 28), fill=CINZA)

        # Preço
        d.text((20, yp + (80 if n2 else 50)), preco_txt,
               font=fnt(FONT_B, 52), fill=LARANJA)

        # Vendidos — canto superior direito do painel
        if sold > 0:
            sold_txt = f"🔥 {sold:,}".replace(",", ".") + " vendidos"
            f_s = fnt(FONT_R, 22)
            bb  = d.textbbox((0, 0), sold_txt, font=f_s)
            d.text((W - (bb[2]-bb[0]) - 18, PROD_H + 14),
                   sold_txt, font=f_s, fill=CINZA)

        # Badge shopee (topo-esq)
        f_b = fnt(FONT_B, 22)
        bb_b = d.textbbox((0, 0), "shopee", font=f_b)
        tw_b, th_b = bb_b[2]-bb_b[0], bb_b[3]-bb_b[1]
        d.rounded_rectangle([20, 20, 20+tw_b+24, 20+th_b+14],
                            radius=11, fill=LARANJA)
        d.text((32, 27), "shopee", font=f_b, fill=BRANCO)

        # Numeração X/N (topo-dir)
        num_txt = f"{len(slides)+1}/{n}"
        f_n = fnt(FONT_B, 28)
        bb_n = d.textbbox((0, 0), num_txt, font=f_n)
        d.text((W - (bb_n[2]-bb_n[0]) - 20, 22), num_txt, font=f_n, fill=LARANJA)

        # Salva slide
        buf = _BIO()
        canvas.save(buf, "JPEG", quality=93, optimize=True)
        slides.append(buf.getvalue())

    log("INFO", f"[NICHO] {len(slides)} slides com fotos reais gerados para '{keyword}'")
    return slides


def preparar_carrossel_nicho(keyword, n=5):
    """
    Gera o carrossel de nicho e re-hospeda os slides.
    Retorna lista de URLs públicas prontas para o Instagram.
    """
    import time as _time
    slides_bytes = gerar_carrossel_nicho(keyword, n)
    if not slides_bytes:
        return []

    host = (cfg("bot_url", "") or os.environ.get("BOT_URL", "")
            or "https://shopee-bot-jt11.onrender.com").rstrip("/")

    urls = []
    for i, data in enumerate(slides_bytes):
        key = f"nicho_{keyword[:10]}_{int(_time.time())}_{i:02d}"
        # Salva em disco
        import os as _os2
        _os2.makedirs(_SLIDE_DIR, exist_ok=True)
        _cp = _os2.path.join(_SLIDE_DIR, key + (".mp4" if data[:4] in (b"\x00\x00\x00\x18", b"\x00\x00\x00 ") else ".jpg"))
        with open(_cp, "wb") as _cf: _cf.write(data)
        urls.append(f"{host}/slide/{key}")

    log("INFO", f"[NICHO] {len(urls)} URLs públicas prontas")
    return urls


# ════════════════════════════════════════════════════════
#  TELEGRAM — Bot API
# ════════════════════════════════════════════════════════
def telegram_post(image_url, caption, bot_token, chat_id):
    try:
        # Envia foto com legenda
        url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
        resp = requests.post(url, data={
            "chat_id": chat_id,
            "photo": image_url,
            "caption": caption[:1024],
            "parse_mode": "HTML"
        }, timeout=30)
        data = resp.json()
        if data.get("ok"):
            msg_id = data.get("result", {}).get("message_id", "?")
            log("INFO", f"Telegram: postado! ID={msg_id}")
            return True, str(msg_id)
        else:
            err = data.get("description", "Erro Telegram")
            log("ERROR", f"Telegram erro: {err[:80]}")
            return False, err
    except Exception as e:
        log("ERROR", f"Telegram excecao: {str(e)[:80]}")
        return False, str(e)

# ════════════════════════════════════════════════════════
#  WHATSAPP — Z-API
# ════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════
# INTEGRAÇÕES MULTI-PLATAFORMA: Amazon, Mercado Livre, Netshoes
# ═══════════════════════════════════════════════════════════════

def amazon_gerar_link(asin_ou_url, tag=None):
    """Gera link de afiliado Amazon com tag configurável."""
    tag = tag or cfg("amazon_affiliate_tag","brizzah-20")
    if not tag: return asin_ou_url
    import re as _re
    if "amazon.com.br" in asin_ou_url:
        url = asin_ou_url.split("?")[0].rstrip("/")
        return f"{url}?tag={tag}"
    return f"https://www.amazon.com.br/dp/{asin_ou_url}?tag={tag}"

def amazon_buscar_produtos(keyword, limit=5):
    """
    Gera cards Amazon com link de busca afiliado.
    Amazon bloqueia scraping de IPs de servidor — usamos links de busca
    que ainda geram comissão quando o usuário compra após clicar.
    """
    tag  = cfg("amazon_affiliate_tag","brizzah-20")
    if not tag: return []
    import urllib.parse as _up
    q    = _up.quote(keyword)
    link = f"https://www.amazon.com.br/s?k={q}&tag={tag}"
    # Retorna 1 card representando a busca naquele nicho
    log("INFO", f"[AMAZON] Link de busca gerado para '{keyword}': {link[:60]}")
    return [{
        "name":          f"Ver ofertas de {keyword} na Amazon 🛒",
        "price":         0.01,
        "original_price":0.01,
        "image_url":     "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a9/Amazon_logo.svg/600px-Amazon_logo.svg.png",
        "product_url":   link,
        "affiliate_url": link,
        "shop_name":     "Amazon",
        "source":        "amazon",
        "sold":          0,
        "rating":        4.8,
        "is_search_link": True,
    }]

def mercadolivre_gerar_link(url, affiliate_id=None):
    """Gera link de afiliado Mercado Livre."""
    aff = affiliate_id or cfg("ml_affiliate_id","ad20260407202239")
    if not aff: return url
    import urllib.parse as _up
    return f"https://www.mercadolivre.com.br/afiliados?aff_id={aff}&url={_up.quote(url)}"

def mercadolivre_buscar_produtos(keyword, limit=5):
    """
    Gera cards ML com link de busca afiliado.
    A API do ML exige autenticação OAuth e bloqueia IPs de servidor.
    Links de busca afiliados ainda geram comissão.
    """
    aff = cfg("ml_affiliate_id","ad20260407202239")
    import urllib.parse as _up
    q    = _up.quote(keyword)
    link_busca = f"https://lista.mercadolivre.com.br/{q.replace('%20','-')}"
    if aff:
        link_aff = f"https://www.mercadolivre.com.br/afiliados?aff_id={aff}&url={_up.quote(link_busca)}"
    else:
        link_aff = link_busca
    log("INFO", f"[ML] Link de busca gerado para '{keyword}'")
    return [{
        "name":          f"Ver ofertas de {keyword} no Mercado Livre 🟡",
        "price":         0.01,
        "original_price":0.01,
        "image_url":     "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f2/Mercado_Livre_logo_%282020%29.svg/512px-Mercado_Livre_logo_%282020%29.svg.png",
        "product_url":   link_busca,
        "affiliate_url": link_aff,
        "shop_name":     "Mercado Livre",
        "source":        "mercadolivre",
        "sold":          0,
        "rating":        4.8,
        "is_search_link": True,
    }]

def netshoes_gerar_link(url, affiliate_id=None):
    """Gera link de afiliado Netshoes via Rakuten Advertising (siteId=4686648)."""
    aff = affiliate_id or cfg("netshoes_affiliate_id","4686648")
    if not aff: return url
    import urllib.parse as _up
    # Rakuten format
    return f"https://click.linksynergy.com/deeplink?id={aff}&mid=42238&murl={_up.quote(url)}"

def netshoes_buscar_produtos(keyword, limit=5):
    """Busca produtos na Netshoes via API."""
    try:
        import requests as _r
        hdrs = {"User-Agent":"Mozilla/5.0","Accept":"application/json"}
        url = f"https://api.netshoes.com.br/api/v2/products?query={keyword}&page=1&size={limit*2}&sort=relevance"
        r = _r.get(url, headers=hdrs, timeout=10)
        data = r.json()
        aff = cfg("netshoes_affiliate_id","")
        produtos = []
        for item in data.get("items",data.get("products",[]))[:limit*2]:
            preco = float(item.get("price",{}).get("value",0) or item.get("salePrice",0) or 0)
            if preco < 10: continue
            link_orig = f"https://www.netshoes.com.br/{item.get('slug','')}?campaign={item.get('id','')}"
            link = netshoes_gerar_link(link_orig, aff) if aff else link_orig
            imgs = item.get("images",[{}])
            img = (imgs[0].get("url","") if imgs else "") or item.get("images",[""])[0]
            produtos.append({
                "name":     item.get("name","")[:80],
                "price":    preco,
                "original_price": float(item.get("originalPrice",preco*1.3) or preco*1.3),
                "image_url":img,
                "product_url":link_orig,
                "affiliate_url":link,
                "shop_name":"Netshoes",
                "source":   "netshoes",
                "sold":     0,
                "rating":   float(item.get("rating",4.5) or 4.5),
            })
            if len(produtos)>=limit: break
        log("INFO",f"[NETSHOES] {len(produtos)} produtos para '{keyword}'")
        return produtos
    except Exception as e:
        log("WARN",f"[NETSHOES] Erro: {str(e)[:60]}")
        return []


def escolher_plataformas_por_nicho(nicho):
    """Define prioridade de plataformas por nicho."""
    nicho = (nicho or "").lower()
    if any(x in nicho for x in ["tenis", "corrida", "esportivo", "camisa time", "chuteira"]):
        return ["netshoes", "shopee", "amazon"]
    if any(x in nicho for x in ["fone", "smartwatch", "notebook", "celular", "eletronico", "gamer", "tablet"]):
        return ["amazon", "shopee", "mercadolivre"]
    if any(x in nicho for x in ["vestido", "blusa", "camiseta", "moda", "bolsa", "calca", "bermuda", "sandalia"]):
        return ["shopee", "netshoes"]
    if any(x in nicho for x in ["perfume", "maquiagem", "skincare", "beleza"]):
        return ["shopee", "amazon"]
    return ["shopee", "netshoes"]


def buscar_produtos_plataforma(keyword, plataforma, limit=12):
    """Busca produtos em uma única plataforma."""
    plataforma = (plataforma or "").lower()
    try:
        if plataforma == "shopee":
            return shopee_api_buscar_produtos(keyword, limit=limit) or []
        if plataforma == "netshoes":
            return netshoes_buscar_produtos(keyword, limit=limit) or []
        if plataforma == "amazon":
            return amazon_buscar_produtos(keyword, limit=limit) or []
        if plataforma in ("mercadolivre", "ml"):
            return mercadolivre_buscar_produtos(keyword, limit=limit) or []
        if plataforma == "todas":
            return buscar_multiplas_plataformas(keyword, limit_cada=max(2, limit // 3)) or []
    except Exception as e:
        log("WARN", f"[MANUAL] busca plataforma={plataforma}: {str(e)[:60]}")
    return []


def buscar_multiplas_plataformas(keyword, limit_cada=3):
    """
    Busca produtos priorizando a plataforma mais adequada para o nicho.
    Shopee continua principal. Netshoes entra forte para tênis/moda esportiva.
    Amazon e ML entram como apoio controlado.
    """
    resultados = []
    plataformas = escolher_plataformas_por_nicho(keyword)

    if "shopee" in plataformas:
        try:
            sp = shopee_api_buscar_produtos(keyword, limit=limit_cada + 8)
            if sp:
                resultados.extend([p for p in sp if not p.get("is_search_link")][:limit_cada + 2])
        except Exception as e:
            log("WARN", f"[BUSCA] Shopee: {str(e)[:60]}")

    if "netshoes" in plataformas and (cfg("netshoes_affiliate_id","") or cfg("netshoes_ativo","false")=="true"):
        try:
            ns = netshoes_buscar_produtos(keyword, limit=max(2, limit_cada))
            if ns:
                resultados.extend([p for p in ns if not p.get("is_search_link")])
        except Exception as e:
            log("WARN", f"[BUSCA] Netshoes: {str(e)[:60]}")

    # Amazon e ML ficam como apoio. Só entram se o nicho pedir ou se faltarem produtos fortes.
    if ("amazon" in plataformas) and cfg("amazon_affiliate_tag","brizzah-20"):
        try:
            am = amazon_buscar_produtos(keyword, limit=1)
            if am:
                resultados.extend(am)
        except Exception as e:
            log("WARN", f"[BUSCA] Amazon: {str(e)[:60]}")

    if ("mercadolivre" in plataformas or "ml" in plataformas) and (cfg("ml_affiliate_id","ad20260407202239") or cfg("ml_ativo","false")=="true"):
        try:
            ml = mercadolivre_buscar_produtos(keyword, limit=1)
            if ml:
                resultados.extend(ml)
        except Exception as e:
            log("WARN", f"[BUSCA] Mercado Livre: {str(e)[:60]}")

    resultados.sort(key=lambda x: (1 if x.get("is_search_link") else 0, -int(x.get("sold",0) or 0)))
    return resultados


def gerar_imagem_amazon_banner(categoria, preco_min=None, preco_max=None):
    """Gera banner Amazon 1080x1080 com PIL para postar no WA/IG."""
    try:
        from PIL import Image, ImageDraw, ImageFont
        import os, io

        W, H = 1080, 1080
        img = Image.new("RGB", (W, H), "#FF9900")  # laranja Amazon

        # Gradiente manual
        draw = ImageDraw.Draw(img)
        for y in range(H):
            fator = y / H
            r = int(255 * (1 - fator * 0.3))
            g = int(153 * (1 - fator * 0.2))
            b = int(0   + fator * 30)
            draw.line([(0, y), (W, y)], fill=(r, g, b))

        # Fontes
        font_dir = "/workspace/fonts"
        try:
            f_grande = ImageFont.truetype(f"{font_dir}/DejaVuSans-Bold.ttf", 90)
            f_medio  = ImageFont.truetype(f"{font_dir}/DejaVuSans-Bold.ttf", 60)
            f_pequeno= ImageFont.truetype(f"{font_dir}/DejaVuSans.ttf", 45)
        except:
            f_grande = f_medio = f_pequeno = ImageFont.load_default()

        BRANCO = (255, 255, 255)
        PRETO  = (20, 20, 20)

        # Fundo escuro inferior
        draw.rectangle([(0, H-280), (W, H)], fill=(20, 20, 20))

        # Logo Amazon simulado
        draw.rounded_rectangle([(W//2-200, 60), (W//2+200, 180)],
                                radius=20, fill=(255,255,255))
        draw.text((W//2, 120), "amazon", fill=PRETO, font=f_grande, anchor="mm")

        # Emoji + categoria
        cat_upper = categoria.upper()[:20]
        draw.text((W//2, 320), "🔥 OFERTAS", fill=BRANCO, font=f_grande, anchor="mm")
        draw.text((W//2, 430), cat_upper,    fill=BRANCO, font=f_medio,  anchor="mm")

        # Preço se informado
        if preco_min and preco_max:
            draw.text((W//2, 560),
                      f"A partir de R$ {preco_min:.2f}",
                      fill=BRANCO, font=f_medio, anchor="mm")

        # CTA
        draw.rounded_rectangle([(W//2-300, 650), (W//2+300, 750)],
                                radius=30, fill=(255,255,255))
        draw.text((W//2, 700), "CLIQUE E COMPRE", fill=PRETO, font=f_medio, anchor="mm")

        # Rodapé
        draw.text((W//2, H-200), "🛒 Link na bio @brizzah.br",
                  fill=(200,200,200), font=f_pequeno, anchor="mm")
        draw.text((W//2, H-140), "Links com cashback • Frete grátis Prime",
                  fill=(150,150,150), font=f_pequeno, anchor="mm")
        draw.text((W//2, H-80),  "amazon.com.br  •  tag: brizzah-20",
                  fill=(100,100,100), font=f_pequeno, anchor="mm")

        # Salva
        os.makedirs(_SLIDE_DIR, exist_ok=True)
        fname = f"amz_banner_{int(time.time())}.jpg"
        fpath = os.path.join(_SLIDE_DIR, fname)
        img.save(fpath, "JPEG", quality=88)

        bot_url = (cfg("bot_url","") or os.environ.get("BOT_URL","")).rstrip("/")
        return f"{bot_url}/slide/{fname}"
    except Exception as e:
        log("WARN", f"[AMAZON-IMG] Erro ao gerar banner: {str(e)[:60]}")
        return ""


def postar_amazon_wa(categoria, keyword, wa_inst, wa_token, wa_group):
    """Posta oferta Amazon no WA com banner gerado localmente."""
    import urllib.parse as _up
    tag  = cfg("amazon_affiliate_tag","brizzah-20")
    q    = _up.quote(keyword)
    link = f"https://www.amazon.com.br/s?k={q}&tag={tag}"

    # Gera banner local
    img_url = gerar_imagem_amazon_banner(categoria)

    # Caption
    cat = categoria.upper()
    caption = (
        f"🛒 *OFERTAS AMAZON — {cat}*\n\n"
        f"Encontrei os melhores produtos de *{categoria}* com ótimos preços na Amazon! 🔥\n\n"
        f"✅ Entrega rápida\n"
        f"✅ Garantia Amazon\n"
        f"✅ Melhor preço\n\n"
        f"🔗 Ver todas as ofertas:\n{link}\n\n"
        f"💡 _Clique no link e aproveite!_"
    )

    if img_url:
        ok, res = whatsapp_post(img_url, caption, wa_inst, wa_token, wa_group)
    else:
        # Fallback: texto puro
        ok, res = whatsapp_post("", caption, wa_inst, wa_token, wa_group)

    log("INFO", f"[AMAZON-WA] {'✅' if ok else '❌'} | {categoria} | {res[:40]}")
    return ok, res


def whatsapp_post(image_url, caption, instance_id, token, group_id):
    """
    Posta no grupo WhatsApp via Evolution API.
    Com imagem: sendMedia | Sem imagem: sendText
    """
    try:
        # Normaliza a URL da imagem
        if image_url and image_url.startswith("//"):
            image_url = "https:" + image_url
        elif image_url and not image_url.startswith("http"):
            image_url = "https://cf.shopee.com.br/file/" + image_url

        # Normaliza group_id
        group_id = group_id.strip()
        log("INFO", f"[WA] Enviando | inst={instance_id} | grupo={group_id[:25]} | img={image_url[:60]}")

        headers_evo = {"apikey": token, "Content-Type": "application/json"}

        # Sem imagem → sendText (evita "Input Buffer is empty")
        if not image_url:
            url_evo = f"https://evolution-api-lad2.onrender.com/message/sendText/{instance_id}"
            payload_evo = {
                "number":  group_id,
                "text":    caption[:4096]
            }
            resp = requests.post(url_evo, json=payload_evo, headers=headers_evo, timeout=35)
        else:
            url_evo = f"https://evolution-api-lad2.onrender.com/message/sendMedia/{instance_id}"
            payload_evo = {
                "number":    group_id,
                "mediatype": "image",
                "media":     image_url,
                "caption":   caption[:1000]
            }
            resp = requests.post(url_evo, json=payload_evo, headers=headers_evo, timeout=35)

        log("INFO", f"[WA] HTTP {resp.status_code} | {resp.text[:150]}")

        try:
            data = resp.json()
        except Exception:
            data = {}

        # Sucesso: status 200/201 e sem campo "error" no body
        if resp.status_code in (200, 201):
            err = data.get("error") or data.get("message","")
            if not err or "success" in str(err).lower():
                log("INFO", "WhatsApp: postado com sucesso!")
                return True, "ok"
            log("ERROR", f"[WA] API retornou erro: {str(err)[:100]}")
            return False, str(err)[:100]

        err_msg = data.get("message") or data.get("error") or resp.text[:100]
        log("ERROR", f"[WA] Falhou HTTP {resp.status_code}: {err_msg[:100]}")
        return False, f"HTTP {resp.status_code}: {err_msg[:80]}"

    except requests.exceptions.Timeout:
        log("ERROR", "[WA] Timeout ao enviar mensagem")
        return False, "Timeout"
    except Exception as e:
        log("ERROR", f"[WA] Excecao: {str(e)[:80]}")
        return False, str(e)[:80]


@app.route("/wa_teste")
@login_required
def wa_teste():
    """Diagnóstico rápido do WhatsApp — testa conexão e envia mensagem teste."""
    wa_inst  = cfg("whatsapp_instance_id","")
    wa_token = cfg("whatsapp_token","")
    wa_group = cfg("whatsapp_group_id","")
    resultados = []

    def chk(label, ok, detalhe=""):
        resultados.append((label, ok, detalhe))

    # 1. Credenciais
    chk("Instance ID configurado",  bool(wa_inst),  wa_inst or "VAZIO")
    chk("Token configurado",        bool(wa_token), "***" if wa_token else "VAZIO")
    chk("Group ID configurado",     bool(wa_group), wa_group or "VAZIO")

    # 2. Evolution API online
    try:
        r = requests.get("https://evolution-api-lad2.onrender.com/instance/fetchInstances",
                         headers={"apikey": wa_token}, timeout=12)
        chk("Evolution API online", r.status_code in (200,201), f"HTTP {r.status_code}")
        try:
            instancias = r.json()
            nomes = [i.get("instance",{}).get("instanceName","?") for i in (instancias if isinstance(instancias,list) else [])]
            chk("Instância encontrada", wa_inst in nomes, f"Instâncias: {nomes}")
        except: chk("Parse instâncias", False, r.text[:80])
    except Exception as e:
        chk("Evolution API online", False, str(e)[:80])

    # 3. Status da conexão WhatsApp
    try:
        r2 = requests.get(f"https://evolution-api-lad2.onrender.com/instance/connectionState/{wa_inst}",
                          headers={"apikey": wa_token}, timeout=12)
        d2 = r2.json()
        state = d2.get("instance",{}).get("state","") or d2.get("state","")
        chk("WhatsApp conectado (open)", state == "open", f"state={state}")
    except Exception as e:
        chk("Status conexão", False, str(e)[:60])

    # 4. Envia mensagem de teste se tudo OK
    tudo_ok = all(ok for _,ok,_ in resultados)
    test_result = ""
    if tudo_ok and request.args.get("testar") == "1":
        ok_wa, res = whatsapp_post(
            "https://cf.shopee.com.br/file/br-11134207-7r98o-m1v6prcfut3i36",
            "✅ Teste Brizzah Bot — conexão funcionando!",
            wa_inst, wa_token, wa_group
        )
        test_result = f"{'✅ ENVIADO!' if ok_wa else '❌ FALHOU: '+str(res)}"

    # HTML
    linhas = ""
    for label, ok, det in resultados:
        cor = "#e8f5e9" if ok else "#ffebee"
        ico = "✅" if ok else "❌"
        linhas += f"<div style='background:{cor};padding:10px 14px;border-radius:8px;margin-bottom:6px'>{ico} <b>{label}</b><br><small style='color:#666'>{det}</small></div>"

    btn = "<a href='/wa_teste?testar=1' style='display:block;background:#25D366;color:#fff;text-align:center;padding:14px;border-radius:10px;font-weight:700;text-decoration:none;margin-top:12px'>📲 Enviar Mensagem de Teste</a>" if tudo_ok else ""
    tr  = f"<div style='background:#e3f2fd;padding:12px;border-radius:8px;margin-top:12px;font-weight:700'>{test_result}</div>" if test_result else ""

    return f"""<!DOCTYPE html><html><head><meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Diagnóstico WhatsApp</title>{CSS}</head><body>
<div class='header'><h1>🔧 Diagnóstico WhatsApp</h1><a href='/wa_manual'>← Voltar</a></div>
<div class='content'>{linhas}{btn}{tr}
<div style='margin-top:16px;background:#fff8e1;border-left:4px solid #f9a825;padding:12px;border-radius:8px;font-size:12px'>
Se o WhatsApp não estiver conectado (state != open), escaneie o QR Code em
<a href='https://evolution-api-lad2.onrender.com' target='_blank'>evolution-api-lad2.onrender.com</a>
</div>
</div></body></html>"""


def _fonte_emoji(produto):
    """Retorna emoji e nome da loja conforme a fonte do produto."""
    src = (produto.get("source","") or "").lower()
    shop = (produto.get("shop_name","") or "").lower()
    if src == "amazon" or "amazon" in shop:
        return "🛒", "Amazon"
    if src == "mercadolivre" or "mercado" in shop:
        return "🟡", "Mercado Livre"
    if src == "netshoes" or "netshoes" in shop:
        return "👟", "Netshoes"
    return "🛍️", "Shopee"

def gerar_caption_instagram_estilo(produto):
    """Caption mais curta e comercial, no estilo de perfil de promoções."""
    import random as _r
    nome = (produto.get("name", "") or "")[:72]
    preco = _safe_float(produto.get("price"), 0.0)
    orig  = _brz_preco_antigo_confiavel(preco, produto.get("original_price") or produto.get("old_price"), produto.get("source")) if '_brz_preco_antigo_confiavel' in globals() else 0
    desconto = int((1 - (preco / orig)) * 100) if orig > 0 and preco > 0 else 0
    pt = f"R$ {preco:,.2f}".replace(",","X").replace(".",",").replace("X",".")
    ot = f"R$ {orig:,.2f}".replace(",","X").replace(".",",").replace("X",".") if orig else ""
    gancho = _r.choice(["oferta", "vale_apena", "bombando"])
    if gancho == "vale_apena":
        linhas = [
            f"Vale a pena esse {nome}? 👀",
            "",
            (f"De {ot} por {pt}." if orig else f"Hoje por {pt}."),
            "Produto original e com preço forte agora.",
            "",
            "🔗 Link na bio @brizzah.br"
        ]
    elif gancho == "bombando":
        linhas = [
            f"🔥 Esse {nome} está bombando",
            "",
            (f"De {ot} por {pt}" if orig else f"Hoje por {pt}"),
            f"{desconto}% OFF" if desconto >= 10 else "Preço muito bom hoje",
            "",
            "🔗 Veja na vitrine da bio @brizzah.br"
        ]
    else:
        linhas = [
            f"🔥 {nome}",
            "",
            f"De {ot} por {pt}",
            f"{desconto}% OFF" if desconto >= 10 else "Oferta do dia",
            "",
            "🔗 Link na bio @brizzah.br"
        ]
    return "\n".join(linhas)

def formatar_mensagem(produto, template=None):
    # Se tiver template personalizado, usa ele
    if not template:
        template = cfg("message_template", "")

    # Se não tiver template, usa caption mais curta e comercial para Instagram
    if not template or template.strip() == "":
        return gerar_caption_instagram_estilo(produto)

    preco_fmt = f"{produto.get('price', 0):.2f}".replace(".", ",")
    rating    = produto.get("rating", 0)
    sold      = produto.get("sold", 0)
    loja      = produto.get("shop_name", "")
    # Usa APENAS o nome do produto para hashtags — nunca o nicho fixo
    hashtags  = gerar_hashtags(produto.get("name",""), "")
    estrelas  = f"⭐ {rating}/5\n" if rating and float(rating) > 0 else ""
    vendidos  = f"🛒 {sold}+ vendidos\n" if sold and int(sold) > 0 else ""

    return (template
        .replace("{nome}",      produto.get("name", ""))
        .replace("{preco}",     preco_fmt)
        .replace("{comissao}",  str(produto.get("commission", 0)))
        .replace("{link}",      produto.get("affiliate_url") or produto.get("product_url", ""))
        .replace("{categoria}", keyword)
        .replace("{estrelas}",  estrelas)
        .replace("{vendidos}",  vendidos)
        .replace("{loja}",      loja)
        .replace("{hashtags}",  hashtags))

# ════════════════════════════════════════════════════════
#  CICLO PRINCIPAL
# ════════════════════════════════════════════════════════
def _detectar_contexto_temporal():
    """
    Detecta o momento atual: hora, dia da semana e data comemorativa.
    Retorna dict com contexto completo para seleção inteligente de nicho.
    """
    from datetime import datetime, timedelta
    try:
        from zoneinfo import ZoneInfo
        agora = datetime.now(ZoneInfo("America/Sao_Paulo"))
    except Exception:
        agora = datetime.now()

    hora    = agora.hour
    dia_sem = agora.weekday()   # 0=Seg … 6=Dom
    mes     = agora.month
    dia     = agora.day

    # ── Período do dia ───────────────────────────────────────────
    if 5 <= hora < 9:    periodo = "manha_cedo"
    elif 9 <= hora < 12: periodo = "manha"
    elif 12 <= hora < 14:periodo = "almoco"
    elif 14 <= hora < 18:periodo = "tarde"
    elif 18 <= hora < 21:periodo = "noite_cedo"
    else:                periodo = "noite"

    # ── Tipo do dia ──────────────────────────────────────────────
    tipo_dia = "fds" if dia_sem >= 5 else "semana"

    # ── Datas comemorativas (30 dias de antecedência) ────────────
    DATAS = [
        # (mes, dia, nome, nichos_prioritarios)
        (1,  1,  "Ano Novo",           ["kit presente","decoracao","moda","relogio"]),
        (2,  14, "Dia dos Namorados BR",["perfume","joias","kit presente","moda","beleza"]),
        (3,  8,  "Dia da Mulher",       ["beleza","skincare","perfume","moda","kit presente"]),
        (4,  21, "Tiradentes",          ["casa","decoracao","fitness","livro"]),
        (5,  1,  "Dia do Trabalho",     ["fitness","eletronico","gadget","mochila"]),
        (5,  11, "Dia das Maes",        ["beleza","perfume","kit presente","casa","joias","skincare"]),
        (6,  12, "Dia dos Namorados",   ["perfume","joias","kit presente","moda","beleza"]),
        (6,  13, "Dia dos Namorados",   ["perfume","joias","kit presente","moda","beleza"]),
        (7,  1,  "Ferias Julho",        ["infantil","brinquedo","viagem","fitness"]),
        (8,  11, "Dia dos Pais",        ["eletronico","ferramenta","kit presente","relogio","perfume masculino"]),
        (9,  7,  "Independencia",       ["decoracao","verde amarelo","esporte"]),
        (10, 12, "Dia das Criancas",    ["brinquedo infantil","kit escolar","infantil","jogo"]),
        (10, 15, "Dia dos Professores", ["livro","papelaria","kit escolar","caneta"]),
        (11, 2,  "Finados",             ["casa","decoracao","flores"]),
        (11, 15, "Proclamacao",         ["esporte","fitness","eletronico"]),
        (11, 28, "Black Friday",        ["eletronico","smartwatch","fone bluetooth","kit presente","moda","beleza"]),
        (11, 29, "Black Friday",        ["eletronico","smartwatch","fone bluetooth","kit presente","moda","beleza"]),
        (12, 1,  "Natal prep",          ["decoracao natal","kit presente","brinquedo infantil","perfume"]),
        (12, 10, "Natal prep",          ["kit presente","decoracao natal","roupa","perfume","brinquedo"]),
        (12, 20, "Natal",              ["kit presente","decoracao natal","roupa","perfume","brinquedo infantil"]),
        (12, 24, "Natal",              ["kit presente","decoracao natal","roupa","perfume","brinquedo infantil"]),
        (12, 25, "Natal",              ["kit presente","decoracao natal","roupa","perfume","brinquedo infantil"]),
        (12, 31, "Reveillon",           ["roupa festiva","decoracao","kit presente","perfume"]),
    ]

    data_comemorativa = None
    nichos_data       = []
    for (m, d, nome, nichos) in DATAS:
        # Verifica se está dentro de 7 dias antes da data
        from datetime import date
        alvo = date(agora.year, m, d)
        hoje = agora.date()
        delta = (alvo - hoje).days
        if -1 <= delta <= 7:
            data_comemorativa = nome
            nichos_data       = nichos
            break

    ctx = {
        "hora":              hora,
        "periodo":           periodo,
        "dia_semana":        dia_sem,
        "tipo_dia":          tipo_dia,
        "mes":               mes,
        "dia":               dia,
        "data_comemorativa": data_comemorativa,
        "nichos_data":       nichos_data,
    }
    log("INFO", f"[CTX] {periodo} | {tipo_dia} | data={data_comemorativa or 'normal'}")
    return ctx


def _nicho_inteligente_contextual(ctx):
    """
    Escolhe o nicho ideal com base no contexto temporal.
    Prioridade: data comemorativa > período do dia > dia da semana.
    Garante rotação para não repetir.
    """
    import random as _r

    # ── 1. Data comemorativa ativa — máxima prioridade ────────────
    if ctx.get("data_comemorativa") and ctx.get("nichos_data"):
        pool = ctx["nichos_data"]
        nicho = _r.choice(pool)
        log("INFO", f"[NICHO] DATA COMEMORATIVA '{ctx['data_comemorativa']}' → '{nicho}'")
        return nicho, ctx["data_comemorativa"]

    # ── 2. Por período do dia ─────────────────────────────────────
    periodo  = ctx["periodo"]
    tipo_dia = ctx["tipo_dia"]

    
    NICHOS_PERIODO = {
        "manha_cedo": [
            "camiseta feminina","camiseta masculina","tenis feminino",
            "tenis masculino","fone bluetooth","smartwatch",
        ],
        "manha": [
            "smartwatch","fone bluetooth","carregador rapido",
            "camisa masculina","blusa feminina","bolsa feminina",
        ],
        "almoco": [
            "vestido feminino","blusa feminina","sandalia feminina",
            "tenis feminino","perfume feminino",
        ],
        "tarde": [
            "tenis nike","tenis adidas","smartwatch","fone sem fio",
            "camiseta masculina","conjunto feminino",
        ],
        "noite_cedo": [
            "vestido feminino","tenis feminino","tenis masculino",
            "perfume feminino","maquiagem","fone bluetooth",
        ],
        "noite": [
            "smartwatch","tenis nike","tenis adidas",
            "camisa masculina","bolsa feminina",
        ],
    }

    # ── 3. Ajuste por dia da semana ───────────────────────────────
    
    AJUSTE_DIA = {
        0: ["camisa masculina","smartwatch","fone bluetooth"],   # Seg
        1: ["tenis feminino","tenis masculino","legging fitness"],   # Ter
        2: ["blusa feminina","camiseta masculina","bolsa feminina"], # Qua
        3: ["maquiagem","perfume feminino","skincare"],             # Qui
        4: ["vestido","tenis nike","tenis adidas"],                 # Sex
        5: ["tenis masculino","camisa masculina","smartwatch"],     # Sab
        6: ["vestido feminino","bolsa feminina","perfume feminino"],# Dom
    }

    # ── Monta pool combinado ──────────────────────────────────────
    pool_periodo = NICHOS_PERIODO.get(periodo, NICHOS_PERIODO["tarde"])
    pool_dia     = AJUSTE_DIA.get(ctx["dia_semana"], [])

    # Pesos: 60% período, 40% dia da semana
    pool_total = pool_periodo * 3 + pool_dia * 2

    # Evita repetir o último nicho
    try:
        with get_db() as c:
            ultimo = (c.execute("SELECT value FROM config WHERE key='_nicho_ultimo'").fetchone() or {})
            ultimo_nicho = ultimo.get("value","") if ultimo else ""
    except Exception:
        ultimo_nicho = ""

    candidatos = [n for n in pool_total if n != ultimo_nicho]
    if not candidatos:
        candidatos = pool_total

    nicho = _r.choice(candidatos)
    log("INFO", f"[NICHO] {periodo}/{tipo_dia} → '{nicho}'")
    return nicho, None


def _proximo_nicho():
    """
    Rotação inteligente de nichos para posts automáticos.
    Garante que nunca 2 posts seguidos sejam do mesmo nicho.
    Cicla pelos nichos em ordem, usando um contador persistente no banco.
    """
    import random as _r

    # Pool completo de nichos — bem diversificado
    NICHOS_POOL = [
        # MODA FEMININA
        "vestido feminino","conjunto feminino","blusa feminina",
        "vestido midi","vestido longo","saia feminina",
        "calca feminina","moda feminina","top cropped",
        "macacão feminino","blusão feminino",
        # MODA MASCULINA
        "camiseta masculina","calca masculina","bermuda masculina",
        "camisa masculina","kit roupa masculina","moletom masculino",
        # CALCADOS
        "tenis feminino","tenis masculino","sandalia feminina",
        "chinelo masculino","sapato feminino",
        # ELETRONICOS
        "celular smartphone","fone bluetooth","fone sem fio",
        "smartwatch","carregador rapido","cabo usb tipo c",
        "caixinha som bluetooth","suporte celular","powerbank",
        "fone gamer","notebook","tablet",
        # CASA E UTILIDADES
        "organizador casa","airfryer","panela antiaderente",
        "kit cozinha","utensilio cozinha","jogo tapete",
        "luminaria led","decoracao sala","porta tempero",
        "travesseiro","jogo roupa cama","toalha",
        # BELEZA FEMININA
        "skincare","kit skincare","serum facial",
        "perfume feminino","creme hidratante","kit cabelo",
        "maquiagem","batom","base maquiagem",
        # ACESSORIOS E BOLSAS
        "bolsa feminina","mochila","carteira feminina",
        "oculos sol","relogio feminino","bijuteria",
        # FITNESS
        "legging fitness","conjunto academia","tenis corrida",
        "garrafa termica","suplemento proteina",
        # MAIS VENDIDOS E RECOMENDADOS SHOPEE
        "mais vendido shopee","tendencia shopee","viral shopee",
        "melhor avaliado shopee","produto recomendado shopee",
        "oferta do dia shopee","achado shopee","promocao shopee",
        "mais vendido semana","produto em alta shopee",
        # PET
        "racao cachorro","cama pet","brinquedo cachorro",
        # NICHOS AMAZON/ML
        "notebook barato","mouse sem fio",
        "teclado gamer","monitor","impressora",
        "cafeteira","aspirador robo",
        "tenis adidas","tenis nike",
        "kit academia","raquete",
    ]
    try:
        with get_db() as c:
            row = c.execute("SELECT value FROM config WHERE key='_nicho_idx'").fetchone()
            idx = int(row["value"]) if row else 0
            proximo_idx = (idx + 1) % len(NICHOS_POOL)
            c.execute("INSERT OR REPLACE INTO config (key,value) VALUES ('_nicho_idx',?)",
                      (str(proximo_idx),))
        nicho = NICHOS_POOL[idx]
        log("INFO", f"[CICLO] Nicho rotativo: {idx+1}/{len(NICHOS_POOL)} → '{nicho}'")
        return nicho
    except Exception as e:
        log("WARN", f"[CICLO] Erro rotação nicho: {e} — usando keyword config")
        return cfg("niche_keyword", "kit presente")


def _normalizar_texto(txt):
    txt = (txt or "").strip().lower()
    txt = ''.join(c for c in unicodedata.normalize('NFD', txt) if unicodedata.category(c) != 'Mn')
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _produto_tokens(txt):
    return [t for t in re.sub(r"[^a-z0-9 ]", " ", _normalizar_texto(txt)).split() if len(t) >= 3]


PALAVRAS_BLOQUEADAS_TOP = [
    "kit", "combo", "lote", "atacado", "revenda", "dropshipping",
    "10 unidades", "20 unidades", "30 unidades", "50 unidades", "100 unidades",
    "x10", "x20", "c/ 10", "c/10", "c/20", "pacote com", "sortido", "surpresa",
    "sem escolha", "aleatorio", "refil", "granel", "miniatura"
]

MODA_KEYWORDS_TOP = [
    "vestido","blusa","camiseta","calca","saia","cropped","conjunto","moletom","jaqueta",
    "cardigan","body","regata","bermuda","camisa","tenis","sandalia","chinelo","bota",
    "rasteirinha","bolsa","mochila","oculos","relogio","cinto"
]

TECH_KEYWORDS_TOP = [
    "fone","fones","bluetooth","headphone","headset","earbuds","smartwatch","carregador",
    "cabo","caixa de som","suporte celular","pelicula","teclado","mouse","power bank",
    "tripé","tripe","webcam","microfone","hub usb","tablet","notebook","celular"
]

KEYWORDS_OPORTUNIDADE_TOP = [
    "oferta","promo","promocao","coupon","cupom","frete gratis","liquidacao","queima","black"
]

KEYWORDS_UTILIDADE_TOP = [
    "vestido","blusa","camiseta","calca","calça","conjunto","tenis","tênis","sandalia",
    "bolsa","mochila","jaqueta","oculos","óculos","fone","bluetooth","smartwatch",
    "carregador","cabo","caixa de som","power bank","suporte celular","mouse","teclado",
    "microfone","webcam","air fryer","airfryer","cafeteira","organizador"
]

KEYWORDS_SEM_USO_TOP = [
    "adesivo","refil","miniatura","aromatizador","unha postica","unha postiça","esponja magica",
    "esponja mágica","capa avulsa","pelicula hidrogel","película hidrogel","peca reposicao",
    "peça reposição","display celular","placa mae","placa mãe","parafuso","fita led crua",
    "componente eletronico","componente eletrônico","resistencia","resistência","amostra"
]


def detectar_categoria_produto(nome):
    n = _normalizar_texto(nome)
    if any(k in n for k in MODA_KEYWORDS_TOP):
        return "moda"
    if any(k in n for k in TECH_KEYWORDS_TOP):
        return "tech"
    return "geral"


def produto_bloqueado_top(produto):
    nome = _normalizar_texto(produto.get("name"))
    preco = _safe_float(produto.get("price"), 0.0)
    sold = _safe_int(produto.get("sold"), 0)
    rating = _safe_float(produto.get("rating"), 0.0)

    if not nome or len(nome) < 8:
        return True
    if any(p in nome for p in PALAVRAS_BLOQUEADAS_TOP):
        return True
    if any(k in nome for k in ["kit ", "combo", "atacado", "lote", "10 pecas", "20 pecas", "3 pares"]):
        return True
    if preco < 50 or preco > 300:
        return True
    if sold < 100:
        return True
    if rating > 0 and rating < 4.5:
        return True
    if nome.count("|") >= 3 or nome.count("/") >= 5:
        return True
    return False


def calcular_score_produto(produto):
    """
    Score de alta conversão focado em saída real: intenção de compra +
    prova social + preço que converte + oportunidade.
    """
    try:
        desconto = float(produto.get("discount_pct") or produto.get("discount", 0) or 0)
        if not desconto and produto.get("price") and produto.get("original_price"):
            p, o = float(produto["price"]), float(produto["original_price"])
            if o > p:
                desconto = (1-p/o)*100

        rating = _safe_float(produto.get("rating"), 0.0)
        vendidos = _safe_int(produto.get("sold"), 0)
        preco = _safe_float(produto.get("price"), 0.0)
        nome = _normalizar_texto(produto.get("name", ""))
        fonte = _normalizar_texto(produto.get("source", ""))
        categoria = detectar_categoria_produto(nome)

        if produto_bloqueado_top(produto):
            return -999

        score = 0.0
        score += min(vendidos, 5000) * 0.025
        score += rating * 22
        score += min(desconto, 65) * 0.9

        if categoria == "moda":
            score += 28
            if 25 <= preco <= 120:
                score += 22
            elif 15 <= preco <= 160:
                score += 10
            else:
                score -= 8
        elif categoria == "tech":
            score += 24
            if 25 <= preco <= 180:
                score += 24
            elif 18 <= preco <= 250:
                score += 12
            else:
                score -= 10
        else:
            if 20 <= preco <= 150:
                score += 8

        if any(m in nome for m in ["nike","adidas","puma","mizuno","olympikus","fila","new balance","asics","under armour","jbl","xiaomi","samsung","apple","lenovo","philips","anker","baseus"]):
            score += 42

        if any(k in nome for k in KEYWORDS_UTILIDADE_TOP):
            score += 16

        if any(k in nome for k in KEYWORDS_OPORTUNIDADE_TOP):
            score += 4

        if vendidos >= 500:
            score += 12
        if vendidos >= 2000:
            score += 10

        if fonte == "netshoes":
            score += 24
        elif fonte == "mercadolivre":
            score += 20
        elif fonte == "shopee":
            score += 10
        elif fonte == "amazon":
            score += 4

        if any(k in nome for k in KEYWORDS_SEM_USO_TOP):
            score -= 120

        if len(nome) > 120:
            score -= 5

        return round(score, 2)
    except Exception:
        return 0


def chave_similaridade_produto(produto):
    toks = _produto_tokens(produto.get("name", ""))
    return " ".join(toks[:6]).strip()


def remover_repetidos_recentes(produtos, horas=48):
    try:
        with get_db() as c:
            rows = c.execute(
                "SELECT name FROM products WHERE posted_at >= datetime('now', ?)",
                (f'-{int(horas)} hours',)
            ).fetchall()
        recentes = {_normalizar_texto(r["name"])[:80] for r in rows}
    except Exception:
        recentes = set()

    saida = []
    for p in produtos or []:
        nome_norm = _normalizar_texto(p.get("name"))[:80]
        if nome_norm and nome_norm not in recentes:
            saida.append(p)
    return saida


def filtrar_produtos_top(produtos, nicho_alvo="", limite=20):
    base = []
    for p in produtos or []:
        if not p or produto_bloqueado_top(p):
            continue
        p["_categoria_inteligente"] = detectar_categoria_produto(p.get("name"))
        p["_score_top"] = calcular_score_produto(p)
        base.append(p)

    nicho_tokens = set(_produto_tokens(nicho_alvo))
    base.sort(key=lambda x: (
        x.get("_score_top", 0) + (10 if nicho_tokens and nicho_tokens.intersection(_produto_tokens(x.get("name", ""))) else 0),
        _safe_int(x.get("sold"), 0),
        _safe_float(x.get("rating"), 0.0)
    ), reverse=True)

    usados = set()
    escolhidos = []
    categoria_count = defaultdict(int)

    for p in base:
        chave = chave_similaridade_produto(p)
        cat = p.get("_categoria_inteligente", "geral")
        if not chave or chave in usados:
            continue
        if categoria_count[cat] >= max(3, limite // 3):
            continue
        usados.add(chave)
        categoria_count[cat] += 1
        escolhidos.append(p)
        if len(escolhidos) >= limite:
            break

    return escolhidos


def buscar_pool_produtos_top(nicho_base=""):
    nicho_base = (nicho_base or "").strip()
    nichos = [
        nicho_base, "moda feminina", "looks femininos", "vestidos femininos",
        "blusas femininas", "tenis feminino", "bolsas femininas",
        "moda masculina", "camisetas masculinas", "tenis casual",
        "eletronicos", "fone bluetooth", "smartwatch", "carregador turbo",
        "acessorios celular"
    ]

    vistos, nichos_validos = set(), []
    for n in nichos:
        n = (n or "").strip()
        if n and n not in vistos:
            vistos.add(n)
            nichos_validos.append(n)

    todos = []
    for n in nichos_validos[:8]:
        try:
            itens = shopee_api_top100(nicho=n, sort_type=2) or []
            for p in itens[:35]:
                p["_nicho_origem"] = n
                todos.append(p)
        except Exception as e:
            log("WARN", f"[TOP_POOL] falha no nicho '{n}': {str(e)[:80]}")

    unicos, ids, nomes = [], set(), set()
    for p in todos:
        pid = str(p.get("item_id") or "").strip()
        nome = _normalizar_texto(p.get("name"))
        chave = pid or nome[:100]
        if chave and chave not in ids and nome not in nomes:
            ids.add(chave)
            nomes.add(nome)
            unicos.append(p)

    log("INFO", f"[TOP_POOL] pool bruto={len(todos)} | unicos={len(unicos)}")
    return unicos


def selecionar_produtos_top(produtos, nicho_alvo="", limite=12, horas_repeticao=72):
    produtos = remover_repetidos_recentes(produtos or [], horas=horas_repeticao)
    return filtrar_produtos_top(produtos, nicho_alvo=nicho_alvo, limite=limite)


def gerar_hash_produto(produto):
    """Hash MD5 único por produto (nome + preço)."""
    import hashlib as _hl
    base = f"{produto.get('name','')}_{produto.get('price',0)}"
    return _hl.md5(base.encode()).hexdigest()

def registrar_click(product_id, product_hash="", canal="link"):
    """Registra clique em produto para análise de conversão."""
    try:
        with get_db() as c:
            c.execute("INSERT INTO clicks (product_id,product_hash,canal) VALUES (?,?,?)",
                      (product_id, product_hash, canal))
    except Exception as e:
        pass  # não quebra o fluxo por erro de tracking

def buscar_top_clicks(limite=5):
    """Retorna produtos com mais cliques nas últimas 2 semanas."""
    try:
        with get_db() as c:
            return c.execute("""
                SELECT p.id, p.name, p.price, p.image_url, p.affiliate_url,
                       COUNT(c.id) as total_clicks
                FROM products p
                LEFT JOIN clicks c ON c.product_id=p.id
                    AND c.created_at >= datetime('now','-14 days')
                GROUP BY p.id
                HAVING total_clicks > 0
                ORDER BY total_clicks DESC LIMIT ?""", (limite,)).fetchall()
    except:
        return []

def gerar_link_rastreado(product_id, product_hash=""):
    """Gera link /r/<id> que rastreia clique antes de redirecionar."""
    bot_url = (cfg("bot_url","") or os.environ.get("BOT_URL","")).rstrip("/")
    return f"{bot_url}/r/{product_id}" if bot_url else ""


def registrar_performance(produto, score):
    """Registra/atualiza produto na tabela de performance."""
    try:
        h = gerar_hash_produto(produto)
        with get_db() as c:
            c.execute("""INSERT INTO performance
                (product_hash,name,price,source,image_url,affiliate_url,posts,score)
                VALUES(?,?,?,?,?,?,1,?)
                ON CONFLICT(product_hash) DO UPDATE SET
                posts=posts+1, last_posted=CURRENT_TIMESTAMP, score=?""",
                (h, produto.get("name","")[:80], produto.get("price",0),
                 produto.get("source","shopee"),
                 produto.get("image_url",""), produto.get("affiliate_url",""),
                 score, score))
    except Exception as e:
        log("WARN", f"[PERF] {str(e)[:50]}")

def buscar_produtos_para_repost(limite=3):
    """Retorna produtos com alta performance para repostar."""
    try:
        with get_db() as c:
            rows = c.execute("""
                SELECT * FROM performance
                WHERE posts >= 2
                AND score >= 50
                AND (last_posted IS NULL OR
                     last_posted < datetime('now','-3 days'))
                ORDER BY score DESC LIMIT ?""", (limite,)).fetchall()
        return [dict(r) for r in rows]
    except:
        return []


def _gerar_caption_wa(produto):
    """Caption curta para WhatsApp: preço riscado, desconto, link direto."""
    import random as _r
    nome  = (produto.get("name","") or "")[:70]
    preco = float(produto.get("price",0) or 0)
    orig  = _brz_preco_antigo_confiavel(preco, produto.get("original_price",0) or produto.get("old_price",0), produto.get("source"))
    desc  = int((1-preco/orig)*100) if orig>0 else 0
    link  = produto.get("affiliate_url") or produto.get("product_url","")
    sold  = int(produto.get("sold",0) or 0)
    stars = float(produto.get("rating",0) or 0)
    try: _e, _n = _fonte_emoji(produto)
    except: _e, _n = "🛍️","Shopee"
    pt   = f"R$ {preco:,.2f}".replace(",","X").replace(".",",").replace("X",".")
    ot   = f"R$ {orig:,.2f}".replace(",","X").replace(".",",").replace("X",".")
    econ = f"R$ {orig-preco:,.2f}".replace(",","X").replace(".",",").replace("X",".")
    p = []
    p.append(f"{_e} *{nome}*")
    p.append("")
    if desc >= 10:
        p.append(f"~~{ot}~~ → *{pt}* 🔥 *{desc}% OFF*")
        p.append(f"💰 Você economiza: *{econ}*")
    else:
        p.append(f"💰 *{pt}*")
    if stars > 0: p.append(f"⭐ {stars:.1f}/5")
    if sold > 0:  p.append(f"🛒 {sold:,}+ vendidos".replace(",","."))
    try: p.append(f"💬 _{adicionar_opiniao(produto)}_")
    except: pass
    p.append("")
    p.append(f"🔗 {link}")
    p.append("")
    p.append("_@brizzah.br — achadinhos todo dia!_ 🔥")
    return "\n".join(p)


def executar_ciclo():
    log("INFO", "═══ INICIANDO CICLO ═══")
    # Libera memória antes do ciclo
    try:
        import gc; gc.collect()
        # Remove arquivos de slide com mais de 2h
        _now = time.time()
        if os.path.isdir(_SLIDE_DIR):
            for _fn in os.listdir(_SLIDE_DIR):
                _fp = os.path.join(_SLIDE_DIR, _fn)
                try:
                    if os.path.isfile(_fp) and _now - os.path.getmtime(_fp) > 7200:
                        os.remove(_fp)
                except Exception:
                    pass
        with get_db() as _c:
            _c.execute("DELETE FROM config WHERE key LIKE '_img_%' AND key < '_img_' || CAST(strftime('%s','now','-1 hours') || '000' AS TEXT)")
            _c.execute("DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT 300)")
    except Exception:
        pass

    # ── Detecta contexto temporal (hora, dia, datas comemorativas) ─
    ctx = _detectar_contexto_temporal()

    # ── Nicho inteligente ─────────────────────────────────────────
    keyword_config = cfg("niche_keyword", "").strip()
    fixar_nicho    = cfg("fixar_nicho_keyword", "false") == "true"

    if keyword_config and fixar_nicho:
        # Usuário fixou um nicho específico — respeita
        keyword        = keyword_config
        data_especial  = None
        log("INFO", f"[CICLO] Nicho FIXO: '{keyword}'")
    else:
        # Sistema inteligente contextual — prioridade máxima
        keyword, data_especial = _nicho_inteligente_contextual(ctx)

    # Salva para diagnóstico no dashboard
    try:
        with get_db() as c:
            c.execute("INSERT OR REPLACE INTO config (key,value) VALUES ('_nicho_ultimo',?)", (keyword,))
            if data_especial:
                c.execute("INSERT OR REPLACE INTO config (key,value) VALUES ('_data_especial',?)", (data_especial,))
    except Exception:
        pass

    log("INFO", f"[CICLO] keyword='{keyword}' | data_especial={data_especial} | periodo={ctx['periodo']} | dia_sem={ctx['dia_semana']}")

    limit          = min(int(cfg("products_per_cycle", "1")), 2)
    affiliate      = cfg("shopee_affiliate_id", "")
    ig_token       = cfg("instagram_access_token", "")
    ig_uid         = cfg("instagram_user_id", "")
    post_ig        = cfg("post_instagram", "true") == "true"
    modo_carrossel = cfg("modo_carrossel", "produto")
    min_comm       = float(cfg("min_commission", "0") or "0")
    tg_token       = cfg("telegram_bot_token", "")
    tg_chat        = cfg("telegram_chat_id", "")
    post_tg        = cfg("post_telegram", "false") == "true"
    wa_inst        = cfg("whatsapp_instance_id", "")
    wa_token       = cfg("whatsapp_token", "")
    wa_group       = cfg("whatsapp_group_id", "")
    post_wa        = cfg("post_whatsapp", "false") == "true"

    # Injeta contexto no produto para uso no caption
    _ctx_ciclo = {"data_especial": data_especial, "periodo": ctx["periodo"], "tipo_dia": ctx["tipo_dia"]}

    log("INFO", f"[CICLO] limit={limit} | ig={'OK' if ig_token and ig_uid else 'VAZIO'}")

    # ── Prioridade 1: produtos externos premium ─────────────────────
    try:
        # Rodízio real: quando a próxima fonte da vez é Shopee, não deixa a fila externa
        # (Netshoes/ML/Amazon) dominar o ciclo. Assim o automático também posta Shopee.
        _fonte_da_vez = _brz_proxima_ordem_fontes()[0] if '_brz_proxima_ordem_fontes' in globals() else ''
        if _fonte_da_vez == "shopee":
            externos = []
            log("INFO", "[CICLO] Fonte da vez: Shopee — pulando fila externa e buscando produtos Shopee")
        else:
            externos = buscar_external_products_aprovados(limit=20)
            externos = [p for p in externos if not ja_postado_recentemente_externo(p["id"], horas=6)]
        if externos:
            externos = sorted(externos, key=score_externo, reverse=True)
            escolhidos = externos[:limit]
            log("INFO", f"[CICLO] Usando produtos externos: {len(escolhidos)} selecionados")
            postados_ext = 0
            host = (cfg("bot_url","") or os.environ.get("BOT_URL","https://shopee-bot-jt11.onrender.com")).rstrip("/")
            for ext in escolhidos:
                produto = {
                    "name": ext.get("name") or "Oferta premium",
                    "price": _safe_float(ext.get("price", 0), 0),
                    "old_price": _brz_preco_antigo_confiavel(_safe_float(ext.get("price", 0), 0), ext.get("old_price", 0), ext.get("source")),
                    "image_url": _fix_img(ext.get("image_url") or ""),
                    "product_url": ext.get("product_url") or "",
                    # Link visível direto da loja/afiliado, como nos grupos profissionais.
                    "affiliate_url": ext.get("affiliate_url") or ext.get("product_url") or "",
                    "source": ext.get("source") or "externo",
                    "coupon": ext.get("coupon") or "",
                }
                # WhatsApp exibe link direto do marketplace/afiliado em vez de link interno do robô.
                link_wa = produto.get("affiliate_url") or produto.get("product_url") or ""
                img_url = produto.get("image_url") or ""
                legenda_ig = _caption_ig_externo(produto)
                legenda_wa = _caption_wa_externo({**produto, "affiliate_url": link_wa})
                channels_ok = []
                channels_fail = []
                if post_ig and ig_token and ig_uid and img_url:
                    try:
                        ok, result = instagram_post(img_url, legenda_ig, ig_token, ig_uid)
                        if ok: channels_ok.append("instagram")
                        else: channels_fail.append(f"instagram({str(result)[:30]})")
                    except Exception as e:
                        channels_fail.append(f"instagram({str(e)[:30]})")
                if post_wa and wa_inst and wa_token and wa_group:
                    try:
                        # Se não houver imagem, envia texto puro no WhatsApp em vez de falhar.
                        ok, result = whatsapp_post(img_url or "", legenda_wa, wa_inst, wa_token, wa_group)
                        if ok: channels_ok.append("whatsapp")
                        else: channels_fail.append(f"whatsapp({str(result)[:30]})")
                    except Exception as e:
                        channels_fail.append(f"whatsapp({str(e)[:30]})")
                status_ext = "success" if len(channels_ok) == 2 else ("partial" if channels_ok else "failed")
                with get_db() as c:
                    c.execute("UPDATE external_products SET last_posted=?, old_price=? WHERE id=?", (datetime.now().isoformat(timespec="seconds"), _brz_preco_antigo_confiavel(produto["price"], produto.get("old_price"), produto.get("source")), ext["id"]))
                    _brz_marcar_fonte_postada(ext.get("source"))
                    c.execute("INSERT INTO products(name,price,commission,image_url,product_url,affiliate_url,channels,status,item_id,shop_id,shop_name,rating,sold) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                        produto["name"], produto["price"], 0, img_url, ext.get("product_url") or "", produto["affiliate_url"], ",".join(channels_ok), status_ext, f"ext_{ext['id']}", "", ext.get("source") or "Externo", 0, int(ext.get("clicks",0) or 0)
                    ))
                log("INFO", f"[EXT] {ext['source']} | {ext['name'][:50]} | status={status_ext}")
                postados_ext += 1
            if postados_ext > 0:
                return postados_ext
    except Exception as e:
        log("WARN", f"[CICLO] externos falharam: {str(e)[:100]}")

    # ── Busca em múltiplas plataformas + pool Shopee campeão ────
    log("INFO", f"[CICLO] Buscando '{keyword}' em todas as plataformas ativas...")
    produtos = buscar_multiplas_plataformas(keyword, limit_cada=max(limit + 5, 6))

    try:
        pool_shopee = buscar_pool_produtos_top(nicho_base=keyword)
        if pool_shopee:
            produtos.extend(pool_shopee[:40])
    except Exception as e:
        log("WARN", f"[CICLO] pool Shopee falhou: {str(e)[:80]}")

    # ── 2ª tentativa: keyword alternativa ────────────────────────
    if not produtos:
        log("WARN", f"[CICLO] Sem resultado para '{keyword}', tentando variante...")
        kw_alt = keyword.split()[0] if keyword.split() else keyword
        produtos = buscar_multiplas_plataformas(kw_alt, limit_cada=max(limit + 5, 6))

    # ── 3ª tentativa: scraping Shopee fallback ───────────────────
    if not produtos:
        log("WARN", "[CICLO] Tentando scraping Shopee...")
        produtos = shopee_search(keyword, limit=limit + 12)

    if not produtos:
        log("WARN", f"[CICLO] Nenhum produto encontrado para '{keyword}'")
        return 0

    if min_comm > 0:
        produtos = [p for p in produtos if p.get("commission", 0) >= min_comm]

    produtos = [p for p in produtos if p.get("image_url") and p.get("name") and not p.get("is_search_link")]
    produtos = selecionar_produtos_top(produtos, nicho_alvo=keyword, limite=max(limit * 6, 12), horas_repeticao=72)

    if not produtos:
        log("WARN", f"[CICLO] Produtos encontrados, mas nenhum passou no filtro TOP para '{keyword}'")
        return 0

    import random as _rnd

    # ── Filtro de produtos irrelevantes ────────────────────────
    BLOQUEADOS = [
        # Religioso / didático
        "biblico","biblia","devocional","salmo","evangelho",
        "livro","literatura","romance","novel","mangá","manga","hq","quadrinho",
        "caderno","caneta","lapis","agenda","papelaria","pasta escolar",
        # Elétrica / eletrônica industrial
        "disjuntor","tomada","fio eletrico","cabo eletrico","interruptor",
        "lampada","resistencia","rele","modulo placa","arduino","esp32",
        "sensor","termometro","termostato","componente eletronico",
        # Ferramentas / peças reposição
        "parafuso","ferramenta","chave de fenda","alicate","broca","solda",
        "peca reposicao","peças reposição","reposicao","spare part",
        "tela celular","display celular","bateria celular","conector celular",
        "flex cabo","touch screen","lcd display","motherboard","placa mae",
        "carcaca celular","tampa celular","botao celular",
        # Festa / fantasia
        "festa","balao","enfeite festa","decoracao festa","fantasia",
        "carnaval","halloween","junina","chapeu festa",
        # Medicamentos / suplementos isolados
        "medicamento","remedio","farmaco","caps medic",
        # Jogos e brinquedos infantis (baixo apelo para adultos)
        "jogo de tabuleiro","jogo infantil","brinquedo bebe",
        "boneca barbie","carrinho brinquedo","lego","quebra cabeca infantil",
        "massinha","slime","brinquedo educativo","jogo de carta infantil",
        "pokemon carta","yu-gi-oh","magic the gathering",
        # Lingerie de baixo apelo comercial
        "calcinha fio dental","fio dental calcinha","tanga micro",
        "calcinha comestivel","fantasia erotica","lingerie adulto",
        # Outros sem apelo visual/comercial
        "papel","saco plastico","embalagem","etiqueta","lacre",
        "cadarco","palmilha avulsa","meia avulsa solta",
        "pilha","bateria aa","bateria aaa",
    ]

    def _produto_ok(p):
        nm = (p.get("name") or "").lower()
        src = (p.get("source","") or "").lower()
        pr = float(p.get("price",0) or 0)
        vendas = int(p.get("sold",0) or 0)
        rating = float(p.get("rating",0) or 0)

        categorias_permitidas = [
            "vestido","blusa","camiseta","calca","bermuda","conjunto",
            "tenis","sandalia","sapato","bolsa","mochila","carteira",
            "smartwatch","fone","bluetooth","carregador",
            "celular","notebook","tablet","perfume","maquiagem","skincare",
            "camisa","relogio","oculos","headset","teclado","mouse","monitor"
        ]
        bloqueados_extras = [
            "aromatizador","suporte manicure","unha","adesivo","embalagem",
            "lacre","papel","saco plastico","refil","cadarco","palmilha avulsa",
            "organizador pequeno","fantasia erotica","calcinha comestivel"
        ]

        if any(b in nm for b in bloqueados_extras):
            return False

        # Mantém blacklist antiga para Shopee
        if src not in ("amazon","mercadolivre","netshoes"):
            for b in BLOQUEADOS:
                if b in nm:
                    return False

        if not any(c in nm for c in categorias_permitidas):
            return False

        if pr < 39 or pr > 2500:
            return False

        if rating and rating < 4.4:
            return False

        if "tenis" in nm:
            if pr < 120 or (src == "shopee" and vendas < 30):
                return False
        elif any(k in nm for k in ["fone","smartwatch","celular","notebook","tablet","carregador","headset","mouse","teclado","monitor"]):
            if pr < 80 or (src == "shopee" and vendas < 50):
                return False
        elif any(k in nm for k in ["vestido","blusa","camiseta","calca","bermuda","conjunto","camisa","bolsa","mochila"]):
            if src == "shopee" and vendas < 80:
                return False
        else:
            if src == "shopee" and vendas < 40:
                return False

        if src == "netshoes" and pr < 120:
            return False

        if src in ("amazon","mercadolivre"):
            if not any(k in nm for k in [
                "fone","smartwatch","notebook","celular","headset",
                "teclado","mouse","monitor","tablet"
            ]):
                return False
            if pr < 80:
                return False

        return True

    produtos = [p for p in produtos if _produto_ok(p)]

    # ── Filtro robusto de duplicatas (72h por item_id + nome) ──
    with get_db() as c:
        recentes_ids = {r["item_id"] for r in c.execute(
            "SELECT item_id FROM products WHERE posted_at >= datetime('now','-72 hours','localtime')"
        ).fetchall() if r["item_id"]}
        recentes_nomes = {r["name"][:40].lower() for r in c.execute(
            "SELECT name FROM products WHERE posted_at >= datetime('now','-72 hours','localtime')"
        ).fetchall()}

    log("INFO", f"Histórico 72h: {len(recentes_ids)} IDs | {len(recentes_nomes)} nomes únicos")

    # Também coleta image_urls recentes para evitar mesma foto
    with get_db() as _ci:
        recentes_imgs = {
            str(r["image_url"] or "").split("/")[-1][:25]
            for r in _ci.execute(
                "SELECT image_url FROM products WHERE posted_at >= datetime('now','-24 hours','localtime')"
            ).fetchall() if r["image_url"]
        }

    produtos_novos = []
    for p in produtos:
        item_id  = str(p.get("item_id","")).strip()
        nome_key = (p.get("name") or "")[:40].lower()
        img_key  = str(p.get("image_url") or "").split("/")[-1][:25]
        if item_id and item_id in recentes_ids:
            continue
        if nome_key in recentes_nomes:
            continue
        if img_key and img_key in recentes_imgs:
            continue
        produtos_novos.append(p)

    log("INFO", f"Produtos disponíveis após filtro: {len(produtos_novos)} (de {len(produtos)} buscados)")

    # Curadoria profissional: prioriza os melhores e só varia dentro do topo.
    produtos_novos = sorted(produtos_novos, key=lambda x: calcular_score_produto(x), reverse=True)
    top_pool = produtos_novos[:10]
    if len(top_pool) > 3:
        import random as _rsel
        produtos_novos = [_rsel.choice(top_pool[:3])] + top_pool[3:10]
    else:
        produtos_novos = top_pool

    if not produtos_novos:
        log("WARN", "Todos já postados nas 72h. Ampliando busca com keyword alternativa...")
        # Tenta keyword alternativa para escapar da repetição
        kw_alt = f"promoção {keyword}" if "promoção" not in keyword else f"kit {keyword}"
        produtos_alt = shopee_api_buscar_produtos(kw_alt, limit=limit + 5)
        if produtos_alt:
            _rnd.shuffle(produtos_alt)
            produtos_novos = [p for p in produtos_alt
                              if str(p.get("item_id","")) not in recentes_ids
                              and p["name"][:40].lower() not in recentes_nomes]
        if not produtos_novos:
            log("WARN", "Usando produtos já postados (sem novidades disponíveis)")
            produtos_novos = produtos

    produtos = produtos_novos[:limit]
    sucesso  = 0

    for p in produtos:
        # Se a API oficial já trouxe o link afiliado, usa ele direto
        if not p.get("affiliate_url"):
            p["affiliate_url"] = gerar_link_afiliado(p["product_url"], affiliate)

        # Injeta contexto temporal no produto para caption inteligente
        p["_data_especial"] = data_especial or ""
        p["_periodo"]       = ctx.get("periodo", "")

        channels_ok   = []
        channels_fail = []

        # Caption específica por canal
        caption_ig = formatar_mensagem(p)          # IG: longa, foco na bio/vitrine
        caption_wa = _gerar_caption_wa(p)          # WA: curta, preço + link direto

        # ── Imagem: normaliza URL principal ──────────────────────────
        raw = p.get("image_url", "")
        if raw and raw.startswith("//"):
            raw = "https:" + raw
        if raw and not raw.startswith("http"):
            raw = "https://cf.shopee.com.br/file/" + raw
        img_url = raw

        # ── Foto original do produto (URL direta da CDN Shopee) ─────────
        log("INFO", f"[CICLO] {p['name'][:40]} | preparando imagem...")
        _produto_contexto.set(p)

        if modo_carrossel == "reels":
            image_urls = [img_url] if img_url else []
        else:
            # Usa a URL original do anúncio — sem nenhum processamento
            image_urls = [img_url] if img_url else []
        log("INFO", f"[CICLO] imagem: {image_urls[0][:80] if image_urls else 'NENHUMA'}")



        # ── Instagram ──────────────────────────────────
        if post_ig and ig_token and ig_uid:
            if image_urls:
                log("INFO", f"Tentando postar no Instagram. URL imagem: {image_urls[0][:60]}")
                # Tenta usar video original do produto (Reel)
                vid_url = p.get("video_url","")
                if not vid_url and p.get("shop_id") and p.get("item_id"):
                    vid_url = buscar_video_shopee(p["shop_id"], p["item_id"])
                tipo_post = cfg("tipo_post_instagram","feed")  # feed | reels | stories

                if tipo_post == "stories":
                    # ── STORY ──────────────────────────────────────────
                    if vid_url:
                        log("INFO", "[CICLO] Story com VÍDEO do produto")
                        ok, result = postar_video_instagram(vid_url, caption, ig_token, ig_uid, "STORIES")
                    else:
                        log("INFO", "[CICLO] Story com IMAGEM do produto")
                        ok, result = instagram_story_post(image_urls[0], ig_token, ig_uid, produto=p)
                    if not ok:
                        log("WARN", f"[CICLO] Story falhou ({result[:50]}), fallback feed")
                        ok, result = instagram_post(image_urls[0], caption, ig_token, ig_uid)

                elif tipo_post == "reels" or modo_carrossel == "reels":
                    # ── REELS ───────────────────────────────────────────
                    if vid_url:
                        log("INFO", "[CICLO] Reels com VÍDEO original Shopee")
                        ok, result = postar_video_instagram(vid_url, caption, ig_token, ig_uid, "REELS")
                    else:
                        log("INFO", "[CICLO] Reels com vídeo gerado da imagem")
                        ok, result = instagram_reels_post(
                            img_url, caption, ig_token, ig_uid,
                            produto=p,
                            bot_url=cfg("bot_url", "") or os.environ.get("BOT_URL", "")
                        )
                    if not ok:
                        log("WARN", f"[CICLO] Reels falhou ({result[:50]}), fallback feed")
                        ok, result = instagram_post(image_urls[0], caption, ig_token, ig_uid)

                else:
                    # ── FEED: imagem processada (badge + preço) via /var/data
                    img_proc = preparar_imagem_produto(image_urls[0], p)
                    url_final = img_proc if img_proc else image_urls[0]
                    ok, result = instagram_post(url_final, caption_ig, ig_token, ig_uid)
                if ok:
                    channels_ok.append("instagram")
                    log("INFO", f"✅ Instagram OK! Post ID: {result}")
                else:
                    channels_fail.append(f"instagram({result[:40]})")
                    log("ERROR", f"❌ Instagram FALHOU: {result}")
            else:
                channels_fail.append("instagram(sem imagem)")
                log("ERROR", "❌ Instagram: nenhuma URL de imagem disponível!")
        elif post_ig and not ig_token:
            log("ERROR", "❌ Instagram: TOKEN não configurado! Vá em Configurar Instagram API")
        elif post_ig and not ig_uid:
            log("ERROR", "❌ Instagram: USER ID não configurado! Vá em Configurar Instagram API")
        elif not post_ig:
            log("WARN", "Instagram desativado nas configurações")

        # ── Telegram ───────────────────────────────────
        if post_tg and tg_token and tg_chat:
            if img_url:
                ok, result = telegram_post(img_url, caption, tg_token, tg_chat)
                if ok:
                    channels_ok.append("telegram")
                else:
                    channels_fail.append(f"telegram({result[:30]})")
            else:
                channels_fail.append("telegram(sem imagem)")
        elif post_tg and (not tg_token or not tg_chat):
            log("WARN", "Telegram: token ou chat ID nao configurado!")

        # ── WhatsApp ───────────────────────────────────
        if post_wa and wa_inst and wa_token and wa_group:
            if img_url:
                ok, result = whatsapp_post(img_url, caption_wa, wa_inst, wa_token, wa_group)
                if ok:
                    channels_ok.append("whatsapp")
                else:
                    channels_fail.append(f"whatsapp({result[:30]})")
            else:
                channels_fail.append("whatsapp(sem imagem)")
        elif post_wa and (not wa_inst or not wa_token or not wa_group):
            log("WARN", "WhatsApp: configuracao incompleta!")

        status = "success" if channels_ok else ("partial" if channels_fail else "skipped")
        with get_db() as c:
            c.execute("""INSERT INTO products
                (name, price, commission, image_url, product_url, affiliate_url,
                 channels, status, item_id, shop_id)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (p["name"], p["price"], p["commission"],
                 p.get("image_url",""), p.get("product_url",""), p.get("affiliate_url",""),
                 ",".join(channels_ok + channels_fail), status,
                 str(p.get("item_id","")), str(p.get("shop_id",""))))

        if channels_ok:
            log("INFO", f"✅ Postado: {p['name'][:40]} -> {', '.join(channels_ok)}")
            sucesso += 1
            try:
                _brz_marcar_fonte_postada(p.get("source") or "shopee")
            except Exception:
                pass
            # Registra na tabela de performance para aprendizado
            registrar_performance(p, calcular_score_produto(p))
        else:
            log("WARN", f"Salvo sem postagem: {p['name'][:40]}")

        # ── Pausa obrigatória entre produtos (evita rate limit do Instagram) ──
        if sucesso < len(produtos):
            pausa = 35  # 35s entre cada produto
            log("INFO", f"[CICLO] Aguardando {pausa}s antes do próximo produto (rate limit)...")
            time.sleep(pausa)

    log("INFO", f"Ciclo concluído: {sucesso}/{len(produtos)} produto(s) postado(s)")
    return sucesso


def salvar_produto(produto, status, canal, caption=""):
    """Persiste produto no banco após postagem. Nunca apaga histórico."""
    try:
        with get_db() as c:
            c.execute("""INSERT INTO products
                (name, price, commission, image_url, product_url, affiliate_url,
                 channels, status, item_id, shop_id, shop_name, rating, sold)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    str(produto.get("name",""))[:100],
                    float(produto.get("price",0) or 0),
                    float(produto.get("commission",0) or 0),
                    str(produto.get("image_url","") or ""),
                    str(produto.get("product_url","") or ""),
                    str(produto.get("affiliate_url","") or ""),
                    canal,
                    status,
                    str(produto.get("item_id","") or ""),
                    str(produto.get("shop_id","") or ""),
                    str(produto.get("shop_name","") or ""),
                    float(produto.get("rating",0) or 0),
                    int(produto.get("sold",0) or 0),
                ))
        log("INFO", f"[DB] Produto salvo: {produto.get('name','')[:40]} | {status}")
    except Exception as e:
        log("WARN", f"[DB] Erro ao salvar produto: {str(e)[:80]}")

# ════════════════════════════════════════════════════════
#  CSS
# ════════════════════════════════════════════════════════
CSS = """<meta name='viewport' content='width=device-width,initial-scale=1'>
<meta name='theme-color' content='#ee4d2d'>
<meta name='apple-mobile-web-app-capable' content='yes'>
<meta name='apple-mobile-web-app-status-bar-style' content='black-translucent'>
<meta name='apple-mobile-web-app-title' content='Brizzah Bot'>
<link rel='manifest' href='/manifest.json'>
<link rel='apple-touch-icon' href='/icon.png'>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{background:#f5f5f5;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#333}
.header{background:#ee4d2d;color:#fff;padding:14px 16px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:99}
.header h1{font-size:17px;font-weight:700}.header a{color:#fff;font-size:13px;text-decoration:none}
.content{padding:14px;max-width:500px;margin:0 auto}
.card{background:#fff;border-radius:14px;padding:18px;margin-bottom:14px;box-shadow:0 2px 10px rgba(0,0,0,.07)}
.card h3{font-size:15px;color:#ee4d2d;margin-bottom:12px;font-weight:700}
.stats{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:14px}
.stat{background:#fff;border-radius:14px;padding:14px 8px;text-align:center;box-shadow:0 2px 10px rgba(0,0,0,.07)}
.stat .num{font-size:26px;font-weight:800;color:#ee4d2d}
.stat .lbl{font-size:11px;color:#999;margin-top:3px}
.btn{display:block;width:100%;padding:14px;border:none;border-radius:10px;font-size:15px;font-weight:700;cursor:pointer;margin-bottom:10px;text-align:center;text-decoration:none}
.btn-red{background:#ee4d2d;color:#fff}.btn-blue{background:#1565c0;color:#fff}
.btn-green{background:#2e7d32;color:#fff}.btn-grey{background:#efefef;color:#555}
.btn-orange{background:#e65100;color:#fff}.btn-purple{background:#6a1b9a;color:#fff}
.badge{display:inline-block;padding:4px 12px;border-radius:20px;font-size:12px;font-weight:700}
.badge-on{background:#e8f5e9;color:#2e7d32}.badge-off{background:#fce4ec;color:#c62828}
.badge-warn{background:#fff8e1;color:#f57f17}
.alert-ok{background:#e8f5e9;color:#2e7d32;border-radius:10px;padding:12px 14px;margin-bottom:14px;font-size:14px;font-weight:600}
.alert-err{background:#fce4ec;color:#c62828;border-radius:10px;padding:12px 14px;margin-bottom:14px;font-size:14px;font-weight:600}
.alert-warn{background:#fff8e1;color:#f57f17;border-radius:10px;padding:12px 14px;margin-bottom:14px;font-size:14px;font-weight:600}
label{display:block;font-size:13px;font-weight:600;color:#555;margin-bottom:5px;margin-top:12px}
input,textarea,select{width:100%;padding:12px;border:1.5px solid #e0e0e0;border-radius:9px;font-size:14px;background:#fafafa}
input:focus,textarea:focus,select:focus{border-color:#ee4d2d;outline:none;background:#fff}
.toggle-row{display:flex;justify-content:space-between;align-items:center;padding:10px 0;border-bottom:1px solid #f0f0f0}
.toggle-row:last-child{border-bottom:none}
.toggle-row span{font-size:14px;font-weight:600}.toggle-row small{font-size:12px;color:#999;display:block}
.switch{position:relative;display:inline-block;width:48px;height:26px}
.switch input{opacity:0;width:0;height:0}
.slider{position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background:#ccc;border-radius:26px;transition:.3s}
.slider:before{position:absolute;content:"";height:20px;width:20px;left:3px;bottom:3px;background:#fff;border-radius:50%;transition:.3s}
input:checked+.slider{background:#ee4d2d}
input:checked+.slider:before{transform:translateX(22px)}
.section-title{font-size:12px;font-weight:700;color:#ee4d2d;text-transform:uppercase;letter-spacing:.5px;margin:16px 0 6px}
.log-item{padding:9px 0;border-bottom:1px solid #f5f5f5;font-size:13px}
.log-item:last-child{border-bottom:none}
.log-ok{color:#2e7d32}.log-err{color:#c62828}.log-info{color:#1565c0}.log-warn{color:#f57f17}
.product-item{padding:12px 0;border-bottom:1px solid #f5f5f5;display:flex;gap:10px;align-items:flex-start}
.product-item:last-child{border-bottom:none}
.product-img{width:60px;height:60px;border-radius:8px;object-fit:cover;background:#f5f5f5;flex-shrink:0}
.product-info .name{font-size:13px;font-weight:600;margin-bottom:3px}
.product-info .meta{font-size:12px;color:#999}
.ch-tag{display:inline-block;background:#fff3e0;color:#e65100;font-size:11px;padding:2px 7px;border-radius:10px;margin-right:3px;font-weight:600}
.ch-ok{background:#e8f5e9;color:#2e7d32}.ch-fail{background:#fce4ec;color:#c62828}
.nav-bottom{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:6px}
.empty{text-align:center;color:#bbb;padding:30px 0;font-size:14px}
.info-box{background:#e3f2fd;color:#1565c0;border-radius:10px;padding:12px 14px;margin-bottom:12px;font-size:13px;line-height:1.5}
small.hint{color:#999;font-size:12px;margin-top:4px;display:block;line-height:1.4}
</style>"""

# ════════════════════════════════════════════════════════
#  TEMPLATES HTML
# ════════════════════════════════════════════════════════
LOGIN_HTML = """<!DOCTYPE html><html><head><meta charset='utf-8'>""" + CSS + """
<title>Shopee Affiliate Bot</title>
<style>body{background:#ee4d2d;min-height:100vh;display:flex;align-items:center;justify-content:center}
.lc{background:#fff;border-radius:20px;padding:32px 24px;width:92%;max-width:380px;text-align:center;box-shadow:0 12px 40px rgba(0,0,0,.25)}
.logo{font-size:52px;margin-bottom:8px}h1{color:#ee4d2d;font-size:22px;font-weight:800;margin-bottom:4px}
.sub{color:#aaa;font-size:13px;margin-bottom:20px}
.tags{display:flex;gap:8px;justify-content:center;margin-bottom:20px;flex-wrap:wrap}
.tag{background:#fff3e0;color:#e65100;font-size:12px;padding:4px 10px;border-radius:20px;font-weight:600}
.lbtn{background:#ee4d2d;color:#fff;width:100%;padding:15px;border:none;border-radius:10px;font-size:16px;font-weight:800;cursor:pointer;margin-top:8px}</style></head>
<body><div class='lc'>
  <div class='logo'>&#128722;</div>
  <h1>Shopee Affiliate Bot</h1>
  <p class='sub'>Automacao inteligente de afiliados</p>
  <div class='tags'><span class='tag'>Instagram</span><span class='tag'>Shopee</span><span class='tag'>WhatsApp</span></div>
  {% if error %}<div style='color:#c62828;background:#fce4ec;padding:10px;border-radius:8px;margin-bottom:12px;font-size:13px'>{{ error }}</div>{% endif %}
  <form method='POST'>
    <input style='margin-bottom:10px' type='text' name='name' placeholder='Seu nome' value='Brizzah'>
    <input style='margin-bottom:10px' type='email' name='email' placeholder='E-mail (opcional)'>
    <input style='margin-bottom:4px' type='password' name='password' placeholder='Senha (opcional)'>
    <button class='lbtn' type='submit'>Entrar no Painel</button>
  </form>
</div></body></html>"""

DASHBOARD_HTML = """<!DOCTYPE html><html><head><meta charset='utf-8'>""" + CSS + """
<title>Brizzah Ultra — Painel</title>
<style>
.ultra-hero{background:linear-gradient(135deg,#111827,#0f172a 55%,#064e3b);color:white;border-radius:24px;padding:22px;margin-bottom:16px;box-shadow:0 18px 45px rgba(15,23,42,.22)}
.ultra-hero h2{font-size:24px;margin:0 0 8px;font-weight:900}.ultra-hero p{margin:0;color:#d1fae5;font-size:13px;line-height:1.45}
.ultra-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px;margin-top:14px}.ultra-kpi{background:rgba(255,255,255,.10);border:1px solid rgba(255,255,255,.12);border-radius:16px;padding:12px}.ultra-kpi .n{font-size:22px;font-weight:900}.ultra-kpi .l{font-size:12px;color:#cbd5e1;margin-top:3px}
.ultra-actions{display:grid;grid-template-columns:1fr 1fr;gap:10px}.ultra-actions .btn{margin:0;text-align:center;border-radius:14px;padding:14px 10px;font-size:14px}.ultra-wide{grid-column:1/-1}.pill{display:inline-block;padding:4px 9px;border-radius:999px;font-size:11px;font-weight:800}.pill-ok{background:#dcfce7;color:#166534}.pill-off{background:#fee2e2;color:#991b1b}.pill-warn{background:#fef3c7;color:#92400e}
.ultra-table{width:100%;border-collapse:collapse;font-size:13px}.ultra-table td{padding:9px 0;border-bottom:1px solid #f1f5f9}.ultra-table tr:last-child td{border-bottom:0}.mini{font-size:12px;color:#64748b}.ultra-card-title{display:flex;justify-content:space-between;align-items:center;gap:10px}.ultra-card-title h3{margin:0}.tool-grid{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:12px}.tool-grid .btn{margin:0;text-align:center;font-size:13px;padding:11px 8px;border-radius:12px}.brand-note{background:#f8fafc;border:1px solid #e2e8f0;border-radius:14px;padding:12px;font-size:12px;color:#334155;line-height:1.45;margin-top:10px}
@media(max-width:560px){.ultra-grid,.ultra-actions,.tool-grid{grid-template-columns:1fr}.ultra-hero h2{font-size:21px}}
</style>
</head><body>
<div class='header'><h1>⚡ Brizzah Ultra</h1><span>Olá, {{ name }}!</span></div>
<div class='content'>
  <div class='ultra-hero'>
    <h2>Central de ofertas e vendas</h2>
    <p>Modelo focado em curadoria: links premium de Mercado Livre, Netshoes e Amazon primeiro; Shopee como motor automático de apoio.</p>
    <div class='ultra-grid'>
      <div class='ultra-kpi'><div class='n'>{{ s.today }}</div><div class='l'>posts hoje</div></div>
      <div class='ultra-kpi'><div class='n'>{{ external_count|default(0) }}</div><div class='l'>externos na fila</div></div>
      <div class='ultra-kpi'><div class='n'>{{ external_clicks|default(0) }}</div><div class='l'>cliques externos</div></div>
      <div class='ultra-kpi'><div class='n'>{% if auto_on %}ON{% else %}OFF{% endif %}</div><div class='l'>automático</div></div>
    </div>
  </div>

  {% if msg %}<div class='alert-ok'>{{ msg }}</div>{% endif %}
  {% if err %}<div class='alert-err'>{{ err }}</div>{% endif %}
  {% if warn %}<div class='alert-warn'>{{ warn }}</div>{% endif %}

  <div class='card'>
    <div class='ultra-card-title'><h3>🚀 Ações principais</h3><span class='mini'>uso diário</span></div>
    <div class='ultra-actions' style='margin-top:12px'>
      <form method='POST' action='/bot/run' class='ultra-wide'><button class='btn btn-red' type='submit'>🔴 Buscar e postar agora</button></form>
      <a href='/external/paste_bulk' class='btn btn-green'>➕ Colar links em massa</a>
      <a href='/external/new' class='btn btn-blue'>🛒 Cadastrar produto</a>
      <a href='/external/import' class='btn btn-orange'>📊 Importar CSV</a>
      <a href='/external' class='btn btn-purple'>⭐ Produtos externos</a>
      <a href='/vitrine' class='btn btn-red' target='_blank'>🏪 Abrir vitrine</a>
      <a href='/logs' class='btn btn-grey'>📋 Ver logs</a>
    </div>
    <div class='brand-note'>
      <b>Rotina ideal:</b> cole links bons em <b>Colar links em massa</b>, dê prioridade 9 ou 10, rode um ciclo e confira os logs. Esses produtos entram antes da Shopee.
    </div>
  </div>

  <div class='card'>
    <h3>📡 Saúde dos canais</h3>
    <table class='ultra-table'>
      <tr><td>Instagram</td><td style='text-align:right'><span class='pill {% if ig_ok %}pill-ok{% else %}pill-off{% endif %}'>{% if ig_ok %}OK{% else %}OFF{% endif %}</span></td></tr>
      <tr><td>WhatsApp</td><td style='text-align:right'><span class='pill {% if wa_ok %}pill-ok{% else %}pill-off{% endif %}'>{% if wa_ok %}OK{% else %}OFF{% endif %}</span></td></tr>
      <tr><td>Shopee API</td><td style='text-align:right'><span class='pill {% if sh_ok %}pill-ok{% else %}pill-warn{% endif %}'>{% if sh_ok %}OK{% else %}Configurar{% endif %}</span></td></tr>
      <tr><td>Amazon tag</td><td style='text-align:right'><span class='pill {% if amz_ok %}pill-ok{% else %}pill-warn{% endif %}'>{% if amz_ok %}{{ amz_tag }}{% else %}Configurar{% endif %}</span></td></tr>
      <tr><td>Mercado Livre</td><td style='text-align:right'><span class='pill {% if ml_ok %}pill-ok{% else %}pill-warn{% endif %}'>{% if ml_ok %}OK{% else %}Configurar{% endif %}</span></td></tr>
    </table>
  </div>

  <div class='card'>
    <h3>⏰ Agenda 30/30</h3>
    <div class='toggle-row'>
      <div><span>Modo automático</span><small>{% if auto_on %}Postando nos horários programados{% else %}Desativado{% endif %}</small></div>
      <span class='badge {% if auto_on %}badge-on{% else %}badge-off{% endif %}'>{% if auto_on %}ON{% else %}OFF{% endif %}</span>
    </div>
    <div class='brand-note'><b>Horários:</b> {{ schedule }}</div>
    <a href='/schedule' class='btn btn-green' style='margin-top:12px'>⏰ Configurar horários</a>
  </div>

  {% if logs %}
  <div class='card'>
    <h3>📋 Últimos sinais do robô</h3>
    {% for l in logs %}
    <div class='log-item {% if "postado" in l.message.lower() or "concluido" in l.message.lower() or "success" in l.message.lower() %}log-ok{% elif "erro" in l.message.lower() or "error" in l.message.lower() or "invalido" in l.message.lower() %}log-err{% elif "warn" in l.message.lower() or "falhou" in l.message.lower() %}log-warn{% else %}log-info{% endif %}'>
      {{ l.message }}<span style='color:#bbb;font-size:11px;float:right'>{{ l.created_at[-8:-3] }}</span>
    </div>
    {% endfor %}
  </div>
  {% endif %}

  <details class='card'>
    <summary style='cursor:pointer;font-weight:800'>🧰 Ferramentas avançadas</summary>
    <div class='tool-grid'>
      <a href='/config' class='btn btn-blue'>⚙️ Configurações</a>
      <a href='/ig_setup' class='btn btn-purple'>📸 Instagram API</a>
      <a href='/diagnostico' class='btn btn-red'>🔧 Diagnóstico</a>
      <a href='/top100' class='btn btn-orange'>🏆 Top 100</a>
      <a href='/products' class='btn btn-orange'>🛍️ Produtos</a>
      <a href='/relatorio' class='btn btn-blue'>📊 Relatório</a>
      <a href='/wa_teste' class='btn btn-grey'>🔧 Testar WA</a>
      <a href='/wa_diagnostico' class='btn btn-green'>📱 Diagnóstico WA</a>
      <a href='/config_plataformas' class='btn btn-blue'>🏪 Plataformas</a>
      <a href='/diagnostico_plataformas' class='btn btn-blue'>🔍 Testar plataformas</a>
      <a href='/top_performers' class='btn btn-red'>🏆 Top Performers</a>
      <a href='/env_status' class='btn btn-blue'>🔐 Ambiente</a>
    </div>
  </details>
  <a href='/logout' class='btn btn-grey' style='margin-top:4px'>Sair</a>
</div></body></html>"""

CONFIG_HTML = """<!DOCTYPE html><html><head><meta charset='utf-8'>""" + CSS + """
<title>Configuracoes</title></head><body>
<div class='header'><h1>Configuracoes</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>
  {% if saved %}<div class='alert-ok'>Configuracoes salvas!</div>{% endif %}
  <form method='POST'>

    <div class='section-title'>🔑 API Oficial Shopee Afiliados</div>
    <div class='card'>
      <div class='info-box' style='background:#e8f5e9;color:#2e7d32'>✅ Com App ID e Secret o bot busca produtos 100% reais!</div>
      <label>App ID</label>
      <input type='text' name='shopee_app_id' placeholder='Ex: seu App ID Shopee' value='{{ cfg.get("shopee_app_id","") }}'>
      <label>Secret Key</label>
      <input type='password' name='shopee_secret' placeholder='Sua chave secreta' value='{{ cfg.get("shopee_secret","") }}'>
      <small class='hint'>Credenciais do portal affiliate.shopee.com.br → API</small>
    </div>

    <div class='section-title'>Shopee Afiliados</div>
    <div class='card'>
      <label>Seu ID de Afiliado Shopee</label>
      <input type='text' name='shopee_affiliate_id' placeholder='Ex: brizzah123' value='{{ cfg.get("shopee_affiliate_id","") }}'>
      <label>Palavra-chave / Nicho (quando fixo)</label>
      <input type='text' name='niche_keyword' placeholder='Ex: maquiagem, fone, celular' value='{{ cfg.get("niche_keyword","") }}'>
      <div style='margin-top:6px;display:flex;align-items:center;gap:8px'>
        <input type='checkbox' name='fixar_nicho_keyword' id='fixar_nicho'
               value='true' {% if cfg.get("fixar_nicho_keyword")=="true" %}checked{% endif %}>
        <label for='fixar_nicho' style='font-size:13px;font-weight:400;margin:0'>
          🔒 Fixar este nicho (desativa rotação automática)
        </label>
      </div>
      <small class='hint'>
        ⚡ Sem fixar: o bot rotaciona automaticamente entre 20+ nichos (skincare, fitness, casa, pet,
        eletrônicos, moda...) para não repetir sempre maquiagem.
        Ative "Fixar" só se quiser um nicho único.
      </small>
      <label>Comissao minima (%)</label>
      <input type='number' name='min_commission' placeholder='0' min='0' max='100' value='{{ cfg.get("min_commission","0") }}'>
      <label>Produtos por ciclo</label>
      <input type='number' name='products_per_cycle' placeholder='3' min='1' max='10' value='{{ cfg.get("products_per_cycle","3") }}'>
    </div>

    <div class='section-title'>Instagram</div>
    <div class='card'>
      <div class='toggle-row'>
        <div><span>Ativar Instagram</span><small>Postar produtos no Instagram</small></div>
        <label class='switch'><input type='checkbox' name='post_instagram' {% if cfg.get('post_instagram','true')=='true' %}checked{% endif %}><span class='slider'></span></label>
      </div>
      <div class='info-box'>Configure o token em Configurar Instagram API no painel.</div>
      <label>Tipo de Post</label>
      <select name='instagram_post_type'>
        <option value='feed' {% if cfg.get('instagram_post_type','feed')=='feed' %}selected{% endif %}>Feed</option>
        <option value='story' {% if cfg.get('instagram_post_type')=='story' %}selected{% endif %}>Stories</option>
      </select>
      <label>🎨 Modo do Carrossel</label>
      <select name='modo_carrossel'>
        <option value='produto' {% if cfg.get('modo_carrossel','produto')=='produto' %}selected{% endif %}>📸 Produto — 7 templates profissionais (1 produto)</option>
        <option value='nicho' {% if cfg.get('modo_carrossel')=='nicho' %}selected{% endif %}>🛍️ Nicho — 5 produtos reais com fotos diferentes</option>
        <option value='reels' {% if cfg.get('modo_carrossel')=='reels' %}selected{% endif %}>🎬 Reels — vídeo com efeito Ken Burns</option>
      </select>
      <small class='hint'>
        <b>📸 Produto:</b> 7 templates de alta conversão com identidade visual @brizzah.br:<br>
        &nbsp;&nbsp;① Preço Chocante · ② Lifestyle Elegante · ③ Urgência + Estoque<br>
        &nbsp;&nbsp;④ Economia (você economiza R$X) · ⑤ Prova Social · ⑥ Split Minimalista · ⑦ CTA Narrativo<br>
        <b>🛍️ Nicho:</b> 5 produtos diferentes com fotos reais — visual catalog que gera mais cliques.<br>
        <b>🎬 Reels:</b> Vídeo 12s com zoom suave — melhor alcance orgânico no Instagram.
      </small>
    </div>

    <div class='section-title'>Telegram</div>
    <div class='card'>
      <div class='toggle-row'>
        <div><span>Ativar Telegram</span><small>Postar em canal ou grupo</small></div>
        <label class='switch'><input type='checkbox' name='post_telegram' {% if cfg.get('post_telegram','false')=='true' %}checked{% endif %}><span class='slider'></span></label>
      </div>
      <label>Token do Bot Telegram</label>
      <input type='text' name='telegram_bot_token' placeholder='123456789:AAF...' value='{{ cfg.get("telegram_bot_token","") }}'>
      <small class='hint'>Obtenha no @BotFather do Telegram.</small>
      <label>Chat ID do Canal ou Grupo</label>
      <input type='text' name='telegram_chat_id' placeholder='-100123456789 ou @seucanal' value='{{ cfg.get("telegram_chat_id","") }}'>
      <small class='hint'>Use @userinfobot para descobrir o Chat ID.</small>
    </div>

    <div class='section-title'>WhatsApp</div>
    <div class='card'>
      <div class='toggle-row'>
        <div><span>Ativar WhatsApp</span><small>Postar em grupos via Z-API</small></div>
        <label class='switch'><input type='checkbox' name='post_whatsapp' {% if cfg.get('post_whatsapp','false')=='true' %}checked{% endif %}><span class='slider'></span></label>
      </div>
      <div class='info-box'>Requer conta gratuita em <b>z-api.io</b></div>
      <label>Instance ID (Z-API)</label>
      <input type='text' name='whatsapp_instance_id' placeholder='Ex: 3EB0...' value='{{ cfg.get("whatsapp_instance_id","") }}'>
      <label>Token (Z-API)</label>
      <input type='text' name='whatsapp_token' placeholder='Seu token Z-API' value='{{ cfg.get("whatsapp_token","") }}'>
      <label>ID do Grupo WhatsApp</label>
      <input type='text' name='whatsapp_group_id' placeholder='5511999999999-1234567890@g.us' value='{{ cfg.get("whatsapp_group_id","") }}'>
      <small class='hint'>Formato: numero-timestamp@g.us</small>
    </div>

    <div class='section-title'>Mensagem</div>
    <div class='card'>
      <label style='font-size:13px;font-weight:600'>Tipo de Post Instagram</label>
      <select name='tipo_post_instagram' style='width:100%;padding:10px;border-radius:8px;border:1px solid #ddd;margin-bottom:12px;font-size:14px;background:#fff'>
        <option value='feed' {{ 'selected' if cfg.get('tipo_post_instagram','feed')=='feed' else '' }}>📸 Feed (foto normal)</option>
        <option value='reels' {{ 'selected' if cfg.get('tipo_post_instagram','feed')=='reels' else '' }}>🎬 Reels (video produto ou animado)</option>
        <option value='stories' {{ 'selected' if cfg.get('tipo_post_instagram','feed')=='stories' else '' }}>📱 Stories (video no story)</option>
      </select>
      <label>Modelo da mensagem</label>
      <textarea name='message_template' rows='6'>{{ cfg.get("message_template","") }}</textarea>
      <small class='hint'>Use: {nome} {preco} {comissao} {link} {categoria}</small>
    </div>

    <button class='btn btn-green' type='submit'>Salvar Configuracoes</button>
    <a href='/dashboard' class='btn btn-grey'>Voltar</a>
  </form>
</div></body></html>"""

IG_SETUP_HTML = """<!DOCTYPE html><html><head><meta charset='utf-8'>""" + CSS + """
<title>Configurar Instagram</title></head><body>
<div class='header'><h1>Instagram API</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>
  {% if saved %}<div class='alert-ok'>Token Instagram salvo!</div>{% endif %}
  {% if tested is not none %}
    {% if tested %}<div class='alert-ok'>Token valido! Instagram conectado!</div>
    {% else %}<div class='alert-err'>Token invalido ou sem permissao. Verifique.</div>{% endif %}
  {% endif %}

  <form method='POST'>
    <div class='card'>
      <h3>Credenciais da API</h3>
      <label>Access Token</label>
      <input type='text' name='instagram_access_token' placeholder='EAAxxxxx ou IGAAxxxxx...' value='{{ cfg.get("instagram_access_token","") }}'>
      <small class='hint'>Token gerado no Meta for Developers.</small>
      <label>ID do Usuario Instagram</label>
      <input type='text' name='instagram_user_id' placeholder='Ex: 17841400000000000' value='{{ cfg.get("instagram_user_id","") }}'>
      <small class='hint'>Numero com 17 digitos.</small>
    </div>
    <button class='btn btn-purple' type='submit' name='action' value='save'>Salvar Token</button>
    <button class='btn btn-green' type='submit' name='action' value='test'>Testar Conexao</button>
    <a href='/dashboard' class='btn btn-grey'>Voltar</a>
  </form>
</div></body></html>"""

SCHEDULE_HTML = """<!DOCTYPE html><html><head><meta charset='utf-8'>""" + CSS + """
<title>Agendamento</title></head><body>
<div class='header'><h1>⏰ Agendamento</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>
  {% if saved %}<div class='alert-ok'>✅ Agendamento salvo!</div>{% endif %}
  <form method='POST'>
    <div class='card'>
      <h3>🤖 Modo Automático</h3>
      <div class='toggle-row'>
        <div><span>Ativar Postagem Automática</span><small>Bot posta sozinho nos horários</small></div>
        <label class='switch'><input type='checkbox' name='auto_enabled' {% if auto_on %}checked{% endif %}><span class='slider'></span></label>
      </div>
    </div>
    <div class='card'>
      <h3>🕐 Horários de Postagem</h3>
      <small class='hint' style='margin-bottom:12px;display:block'>Agora aceita horários cheios e de meia em meia hora (Brasília).</small>
      {% set horarios_ativos = schedule.split(',') %}
      <label>Lista rápida de horários</label>
      <input type='text' name='schedule_text' value='{{ schedule }}' placeholder='Ex: 08:00,08:30,09:00,09:30,12:00,18:30'>
      <small class='hint' style='margin:8px 0 14px;display:block'>Você pode digitar manualmente ou usar os botões abaixo.</small>
      {% for h in horarios_disponiveis %}
      <div class='toggle-row'>
        <div><span>{{ h }}</span><small>{% if h in ['08:00','08:30','12:00','18:00','18:30','21:00'] %}⭐ Recomendado{% endif %}</small></div>
        <label class='switch'><input type='checkbox' name='horario_{{ h }}' {% if h in horarios_ativos %}checked{% endif %}><span class='slider'></span></label>
      </div>
      {% endfor %}
    </div>
    <div class='card'>
      <h3>📅 Nichos por Horário</h3>
      <label>Nicho da manhã (06h-11h)</label>
      <input type='text' name='niche_manha' placeholder='Ex: skincare' value='{{ cfg.get("niche_manha","beleza") }}'>
      <label>Nicho do almoço (12h-14h)</label>
      <input type='text' name='niche_almoco' placeholder='Ex: casa' value='{{ cfg.get("niche_almoco","promocao") }}'>
      <label>Nicho da tarde (15h-17h)</label>
      <input type='text' name='niche_tarde' placeholder='Ex: fitness' value='{{ cfg.get("niche_tarde","moda") }}'>
      <label>Nicho da noite (18h-23h)</label>
      <input type='text' name='niche_noite' placeholder='Ex: eletronicos' value='{{ cfg.get("niche_noite","oferta") }}'>
    </div>
    <button class='btn btn-green' type='submit'>💾 Salvar Agendamento</button>
  </form>

  <!-- BLOCO CRON EXTERNO -->
  <div class='card' style='background:#fff8e1;border-left:4px solid #f39c12'>
    <h3>🔄 Agendamento Garantido (cron-job.org)</h3>
    <p style='font-size:13px;color:#555;margin-bottom:12px;line-height:1.6'>
      ⚠️ O Render gratuito <b>dorme após 15 min</b> sem uso e o agendamento interno para.<br>
      Para garantir 100% dos posts no horário, configure um <b>cron externo gratuito</b>:
    </p>
    <div style='background:#fff;border-radius:8px;padding:12px;margin-bottom:10px'>
      <div style='font-size:12px;font-weight:700;color:#333;margin-bottom:6px'>1️⃣ Acesse <b>cron-job.org</b> (gratuito)</div>
      <div style='font-size:12px;font-weight:700;color:#333;margin-bottom:6px'>2️⃣ Crie um novo cron job com esta URL:</div>
      <input type='text' value='{{ bot_url }}/trigger' readonly
             style='background:#f0f0f0;font-size:11px;font-family:monospace'
             onclick='this.select()'>
      <div style='font-size:12px;font-weight:700;color:#333;margin-top:8px;margin-bottom:6px'>3️⃣ Intervalo: <b>a cada 1 minuto</b></div>
      <div style='font-size:12px;color:#666'>✅ O bot só posta quando for o horário certo — sem spam!</div>
    </div>
    <div style='background:#fff;border-radius:8px;padding:12px'>
      <div style='font-size:12px;font-weight:700;color:#333;margin-bottom:6px'>🔒 Também configure keep-alive (evita o sleep):</div>
      <input type='text' value='{{ bot_url }}/ping' readonly
             style='background:#f0f0f0;font-size:11px;font-family:monospace'
             onclick='this.select()'>
      <div style='font-size:12px;color:#666;margin-top:6px'>Intervalo: a cada <b>10 minutos</b></div>
    </div>
  </div>

  <a href='/dashboard' class='btn btn-grey'>Voltar ao Painel</a>
</div></body></html>"""



PRODUCTS_HTML = """<!DOCTYPE html><html><head><meta charset='utf-8'>""" + CSS + """
<title>Produtos</title></head><body>
<div class='header'><h1>Produtos Postados</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>
  {% if products %}
  <div class='card'>
    {% for p in products %}
    <div class='product-item'>
      {% if p.image_url %}<img class='product-img' src='{{ p.image_url }}' onerror="this.style.display='none'">{% endif %}
      <div class='product-info' style='flex:1'>
        <div class='name'>{{ p.name }}</div>
        <div class='meta'>R$ {{ "%.2f"|format(p.price|float) }} | {{ p.commission }}%</div>
        <div class='meta' style='margin-top:4px'>
          {% for ch in p.channels.split(',') if ch %}
            <span class='ch-tag {% if "instagram" in ch and "(" not in ch %}ch-ok{% elif "(" in ch %}ch-fail{% endif %}'>
              {{ ch.split("(")[0] }}
            </span>
          {% endfor %}
          <span style='font-size:11px;color:#bbb'>{{ p.posted_at }}</span>
        </div>
        {% if p.affiliate_url %}
        <div style='margin-top:5px'><a href='{{ p.affiliate_url }}' target='_blank' style='color:#ee4d2d;font-size:12px'>Ver produto</a></div>
        {% endif %}
      </div>
    </div>
    {% endfor %}
  </div>
  {% else %}
  <div class='card'><div class='empty'>Nenhum produto ainda. Toque em Buscar e Postar Agora!</div></div>
  {% endif %}
  <a href='/dashboard' class='btn btn-grey'>Voltar ao Painel</a>
</div></body></html>"""

LOGS_HTML = """<!DOCTYPE html><html><head><meta charset='utf-8'>""" + CSS + """
<title>Logs</title></head><body>
<div class='header'><h1>Logs do Sistema</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>
  <div class='card'>
    {% if logs %}{% for l in logs %}
    <div class='log-item'>
      <span style='font-size:11px;color:#bbb'>{{ l.created_at }}</span><br>{{ l.message }}
    </div>
    {% endfor %}{% else %}
    <div class='empty'>Nenhum log ainda.</div>{% endif %}
  </div>
  <a href='/dashboard' class='btn btn-grey'>Voltar ao Painel</a>
</div></body></html>"""

# ════════════════════════════════════════════════════════
#  ROTAS
# ════════════════════════════════════════════════════════
@app.route("/", methods=["GET","POST"])
@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        name = request.form.get("name","").strip() or "Afiliado"
        email = request.form.get("email","").strip()
        pwd   = request.form.get("password","").strip()
        session["user"] = name
        if email: cfg_set("shopee_email", email)
        if pwd:   cfg_set("shopee_pass", pwd)
        cfg_set("user_name", name)
        log("INFO", f"Login: {name}")
        return redirect(url_for("dashboard"))
    return render_template_string(LOGIN_HTML, error=None)

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

@app.route("/dashboard")
@login_required
def dashboard():
    with get_db() as c:
        logs_list = c.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 7").fetchall()
        try:
            external_count = c.execute("SELECT COUNT(*) n FROM external_products WHERE status='approved' AND is_active=1").fetchone()["n"]
        except Exception:
            external_count = 0
        try:
            external_clicks = c.execute("SELECT COUNT(*) n FROM external_clicks").fetchone()["n"]
        except Exception:
            external_clicks = 0
    nicho_atual = cfg("_nicho_ultimo", "") or "automático (skincare, casa, fitness...)"
    return render_template_string(DASHBOARD_HTML,
        name=session["user"], s=get_stats(), logs=logs_list,
        external_count=external_count, external_clicks=external_clicks,
        ig_ok=bool(cfg("instagram_access_token") and cfg("instagram_user_id")),
        tg_ok=bool(cfg("telegram_bot_token") and cfg("telegram_chat_id") and cfg("post_telegram")=="true"),
        wa_ok=bool(cfg("whatsapp_instance_id") and cfg("whatsapp_token") and cfg("post_whatsapp")=="true"),
        sh_ok=bool(cfg("shopee_affiliate_id")),
        amz_ok=bool(cfg("amazon_affiliate_tag","brizzah-20")),
        amz_tag=cfg("amazon_affiliate_tag","brizzah-20"),
        ml_ok=bool(cfg("ml_affiliate_id","ad20260407202239")),
        auto_on=cfg("auto_enabled","false")=="true",
        schedule=cfg("auto_schedule","07:00,07:30,08:00,08:30,09:00,09:30,10:00,10:30,11:00,11:30,12:00,12:30,13:00,13:30,14:00,14:30,15:00,15:30,16:00,16:30,17:00,17:30,18:00,18:30,19:00,19:30,20:00,20:30,21:00,21:30,22:00,22:30,23:00,23:30"),
        nicho_atual=nicho_atual,
        msg=request.args.get("msg"),
        err=request.args.get("err"),
        warn=request.args.get("warn"))

@app.route("/config", methods=["GET","POST"])
@login_required
def config():
    saved = False
    if request.method == "POST":
        for f in ["shopee_app_id","shopee_secret",
                  "shopee_affiliate_id","niche_keyword","min_commission","fixar_nicho_keyword",
                  "tipo_post_instagram",
                  "products_per_cycle","message_template","instagram_post_type",
                  "modo_carrossel",
                  "telegram_bot_token","telegram_chat_id",
                  "whatsapp_instance_id","whatsapp_token","whatsapp_group_id"]:
            cfg_set(f, request.form.get(f,""))
        for t in ["post_instagram","post_telegram","post_whatsapp"]:
            cfg_set(t, "true" if request.form.get(t) else "false")
        log("INFO", "Configuracoes atualizadas")
        saved = True
    cfg_all = {}
    with get_db() as c:
        for row in c.execute("SELECT key,value FROM config").fetchall():
            cfg_all[row["key"]] = row["value"]
    return render_template_string(CONFIG_HTML, cfg=cfg_all, saved=saved)

@app.route("/ig_setup", methods=["GET","POST"])
@login_required
def ig_setup():
    saved = False
    tested = None
    if request.method == "POST":
        action = request.form.get("action","save")
        token = request.form.get("instagram_access_token","").strip()
        uid   = request.form.get("instagram_user_id","").strip()
        cfg_set("instagram_access_token", token)
        cfg_set("instagram_user_id", uid)
        if action == "test" and token and uid:
            try:
                tested = False
                username = "?"

                # Tenta endpoint do Facebook Graph API (token EAA)
                r1 = requests.get(
                    f"https://graph.facebook.com/v19.0/{uid}",
                    params={"fields": "id,name,username", "access_token": token},
                    timeout=10)
                d1 = r1.json()
                if "id" in d1:
                    tested = True
                    username = d1.get("username") or d1.get("name", "?")

                # Tenta endpoint do Instagram Graph API (token IGAA ou EAA)
                if not tested:
                    r2 = requests.get(
                        "https://graph.instagram.com/v19.0/me",
                        params={"fields": "id,username", "access_token": token},
                        timeout=10)
                    d2 = r2.json()
                    if "id" in d2:
                        tested = True
                        username = d2.get("username", "?")

                # Tenta verificar token via debug
                if not tested:
                    r3 = requests.get(
                        "https://graph.facebook.com/v19.0/me",
                        params={"fields": "id,name", "access_token": token},
                        timeout=10)
                    d3 = r3.json()
                    if "id" in d3:
                        tested = True
                        username = d3.get("name", "?")

                if tested:
                    log("INFO", f"Instagram conectado: @{username}")
                else:
                    err = d1.get("error", {}).get("message", "Token invalido")
                    log("ERROR", f"Instagram token invalido: {err[:60]}")

            except Exception as e:
                tested = False
                log("ERROR", f"Instagram teste falhou: {str(e)[:60]}")
        else:
            saved = True
            log("INFO", "Token Instagram salvo")
    cfg_all = {}
    with get_db() as c:
        for row in c.execute("SELECT key,value FROM config").fetchall():
            cfg_all[row["key"]] = row["value"]
    return render_template_string(IG_SETUP_HTML, cfg=cfg_all, saved=saved, tested=tested)

@app.route("/products")
@login_required
def products():
    with get_db() as c:
        prods = c.execute("SELECT * FROM products ORDER BY id DESC LIMIT 100").fetchall()
    return render_template_string(PRODUCTS_HTML, products=prods)

@app.route("/logs")
@login_required
def logs_page():
    with get_db() as c:
        logs_list = c.execute("SELECT * FROM logs ORDER BY id DESC LIMIT 200").fetchall()
    return render_template_string(LOGS_HTML, logs=logs_list)

@app.route("/bot/run", methods=["POST"])
@login_required
def bot_run():
    # niche_keyword não é mais obrigatório — o bot usa rotação automática de nichos
    def run_async():
        try:
            executar_ciclo()
        except Exception as e:
            log("ERROR", f"Erro no ciclo: {str(e)[:80]}")
    t = threading.Thread(target=run_async)
    t.daemon = True
    t.start()
    return redirect(url_for("dashboard") + "?msg=Bot iniciado! Aguarde 30s e verifique os Logs.")

@app.route("/schedule", methods=["GET","POST"])
@login_required
def schedule():
    saved = False
    if request.method == "POST":
        cfg_set("auto_enabled", "true" if request.form.get("auto_enabled") else "false")
        horarios = []
        for h in gerar_grade_horarios(6, 23):
            if request.form.get(f"horario_{h}"):
                horarios.append(h)
        schedule_text = request.form.get("schedule_text", "").strip()
        if schedule_text:
            horarios = normalizar_horarios_agendamento(schedule_text)
        else:
            horarios = normalizar_horarios_agendamento(",".join(horarios))
        cfg_set("auto_schedule", ",".join(horarios) if horarios else "08:00,12:00,18:00,21:00")
        for f in ["niche_manha","niche_almoco","niche_tarde","niche_noite"]:
            cfg_set(f, request.form.get(f,""))
        log("INFO", f"Agendamento salvo: {cfg('auto_schedule')}")
        saved = True
    cfg_all = {}
    with get_db() as c:
        for row in c.execute("SELECT key,value FROM config").fetchall():
            cfg_all[row["key"]] = row["value"]
    bot_url = request.host_url.rstrip("/")
    return render_template_string(SCHEDULE_HTML,
        cfg=cfg_all,
        auto_on=cfg("auto_enabled","false")=="true",
        schedule=cfg("auto_schedule","07:00,07:30,08:00,08:30,09:00,09:30,10:00,10:30,11:00,11:30,12:00,12:30,13:00,13:30,14:00,14:30,15:00,15:30,16:00,16:30,17:00,17:30,18:00,18:30,19:00,19:30,20:00,20:30,21:00,21:30,22:00,22:30,23:00,23:30"),
        horarios_disponiveis=gerar_grade_horarios(6, 23),
        bot_url=bot_url,
        saved=saved)

@app.route("/bot/ping")
@app.route("/ping")
def bot_ping():
    # Aproveita o ping para pré-aquecer vitrine se cache vazio
    if not _vitrine_cache["dados"]:
        try:
            nicho = cfg("niche_keyword","geral") or "geral"
            dados = shopee_api_top100(nicho=nicho, sort_type=2)
            if dados:
                import time as _tp
                _vitrine_cache.update({"dados":dados,"ts":_tp.time(),"nicho":nicho})
        except Exception: pass
    return jsonify({"status":"ok","time":datetime.now().isoformat()})

# ── Trigger externo (cron-job.org) ─────────────────────────
@app.route("/trigger", methods=["GET","POST"])
def trigger_externo():
    """
    Endpoint chamado por cron-job.org a cada minuto.
    Substitui o agendador interno (que para quando Render dorme).
    Protegido por chave secreta opcional.
    """
    chave_esperada = cfg("cron_secret","") or os.environ.get("CRON_SECRET","")
    if chave_esperada:
        chave_enviada = request.args.get("key","") or request.form.get("key","")
        if chave_enviada != chave_esperada:
            return jsonify({"error":"Unauthorized"}), 401

    auto_on = cfg("auto_enabled","false") == "true"
    if not auto_on:
        return jsonify({"status":"skip","reason":"auto_enabled=false"})

    horarios_raw = cfg("auto_schedule","07:00,07:30,08:00,08:30,09:00,09:30,10:00,10:30,11:00,11:30,12:00,12:30,13:00,13:30,14:00,14:30,15:00,15:30,16:00,16:30,17:00,17:30,18:00,18:30,19:00,19:30,20:00,20:30,21:00,21:30,22:00,22:30,23:00,23:30")
    horarios     = normalizar_horarios_agendamento(horarios_raw)
    agora_dt     = _agora_brasil()
    agora        = agora_dt.strftime("%H:%M")
    chave_hoje   = agora_dt.strftime("%Y-%m-%d") + "_" + agora
    ultimo       = cfg("last_auto_run","")

    if agora not in horarios:
        return jsonify({"status":"skip","reason":f"{agora} não é horário de post","horarios":horarios})

    if chave_hoje == ultimo:
        return jsonify({"status":"skip","reason":"já executado neste minuto"})

    cfg_set("last_auto_run", chave_hoje)
    log("INFO", f"▶ Trigger externo disparado: {agora}")

    def run():
        try:
            resultado = executar_ciclo()
            log("INFO", f"✅ Trigger externo: {resultado} produto(s) postado(s)")
        except Exception as e:
            log("ERROR", f"Trigger externo erro: {str(e)[:80]}")

    threading.Thread(target=run, daemon=True).start()
    return jsonify({"status":"ok","horario":agora,"message":"Ciclo iniciado!"})


@app.route("/manifest.json")
def manifest():
    data = {
        "name": "Brizzah Bot",
        "short_name": "Brizzah",
        "description": "Bot de afiliados Shopee",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#ee4d2d",
        "theme_color": "#ee4d2d",
        "orientation": "portrait",
        "icons": [
            {"src": "/icon.png", "sizes": "192x192", "type": "image/png"},
            {"src": "/icon.png", "sizes": "512x512", "type": "image/png"}
        ]
    }
    return Response(json.dumps(data), mimetype="application/json")

@app.route("/icon.png")
def icon():
    # Ícone SVG convertido para resposta simples
    svg = """<svg xmlns='http://www.w3.org/2000/svg' width='192' height='192'>
    <rect width='192' height='192' rx='32' fill='#ee4d2d'/>
    <text x='96' y='130' font-size='100' text-anchor='middle' fill='white'>🛍</text>
    </svg>"""
    return Response(svg, mimetype="image/svg+xml")


@app.route("/api/status")
def api_status():
    return jsonify({"stats": get_stats(),
                    "ig": bool(cfg("instagram_access_token")),
                    "sh": bool(cfg("shopee_affiliate_id"))})

@app.route("/slide/<key>")
def serve_slide(key):
    """Serve slides: tenta disco primeiro, depois banco (fallback legado)"""
    try:
        # 1. Tenta arquivo em disco (nova abordagem, eficiente)
        for ext, mime in [(".jpg", "image/jpeg"), (".mp4", "video/mp4")]:
            fpath = os.path.join(_SLIDE_DIR, key + ext)
            if os.path.exists(fpath):
                if ext == ".mp4":
                    # Vídeo: suporte a Range requests (Instagram exige)
                    file_size = os.path.getsize(fpath)
                    range_header = request.headers.get("Range", None)
                    if range_header:
                        byte_start, byte_end = 0, file_size - 1
                        match = re.search(r"bytes=(\d+)-(\d*)", range_header)
                        if match:
                            byte_start = int(match.group(1))
                            byte_end = int(match.group(2)) if match.group(2) else file_size - 1
                        length = byte_end - byte_start + 1
                        with open(fpath, "rb") as _vf:
                            _vf.seek(byte_start)
                            data = _vf.read(length)
                        resp = Response(data, 206, mimetype="video/mp4",
                                        content_type="video/mp4")
                        resp.headers["Content-Range"] = f"bytes {byte_start}-{byte_end}/{file_size}"
                        resp.headers["Accept-Ranges"] = "bytes"
                        resp.headers["Content-Length"] = str(length)
                        return resp
                    return send_file(fpath, mimetype="video/mp4",
                                     max_age=3600, conditional=True,
                                     as_attachment=False)
                return send_file(fpath, mimetype=mime,
                                 max_age=3600, conditional=True)
        # 2. Fallback: banco de dados (compatibilidade com posts antigos)
        with get_db() as c:
            row = c.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
        if not row:
            return "Slide não encontrado", 404
        img_bytes = base64.b64decode(row["value"])
        mime = "video/mp4" if img_bytes[4:8] == b"ftyp" else "image/jpeg"
        return Response(img_bytes, mimetype=mime,
                        headers={"Cache-Control": "public, max-age=3600"})
    except Exception as e:
        return str(e), 500


@app.route("/exportar_produtos")
@login_required
def exportar_produtos():
    """Exporta todos os produtos postados como JSON — use para backup antes de redeploy."""
    with get_db() as c:
        rows = c.execute("SELECT * FROM products ORDER BY id ASC").fetchall()
    dados = [dict(r) for r in rows]
    resp = jsonify({"total": len(dados), "produtos": dados})
    resp.headers["Content-Disposition"] = "attachment; filename=brizzah_produtos_backup.json"
    return resp

@app.route("/importar_produtos", methods=["POST"])
@login_required
def importar_produtos():
    """Importa produtos de um JSON de backup — restaura histórico após redeploy."""
    try:
        dados = request.get_json(force=True)
        produtos = dados.get("produtos", dados) if isinstance(dados, dict) else dados
        ct = 0
        with get_db() as c:
            for p in produtos:
                try:
                    c.execute("""INSERT OR IGNORE INTO products
                        (name,price,commission,rating,sold,image_url,product_url,
                         affiliate_url,shop_id,item_id,shop_name,status,channels,posted_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (p.get("name",""), p.get("price",0), p.get("commission",0),
                         p.get("rating",0), p.get("sold",0), p.get("image_url",""),
                         p.get("product_url",""), p.get("affiliate_url",""),
                         p.get("shop_id",""), p.get("item_id",""), p.get("shop_name",""),
                         p.get("status","success"), p.get("channels","instagram"),
                         p.get("posted_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))))
                    ct += 1
                except Exception as e:
                    pass
        return jsonify({"ok": True, "importados": ct, "total": len(produtos)})
    except Exception as e:
        return jsonify({"ok": False, "erro": str(e)}), 400

@app.route("/external/<int:pid>/archive", methods=["POST"])
@login_required
def external_archive(pid):
    with get_db() as c:
        c.execute("UPDATE external_products SET status='archived', is_active=0 WHERE id=?", (pid,))
    return redirect("/external")


@app.route("/external/<int:pid>/delete", methods=["POST"])
@login_required
def external_delete(pid):
    with get_db() as c:
        c.execute("DELETE FROM external_products WHERE id=?", (pid,))
    return redirect("/external")


@app.route("/external/clear_queue", methods=["POST"])
@login_required
def external_clear_queue():
    # Arquivar é mais seguro do que deletar: some da fila, mas mantém histórico.
    with get_db() as c:
        c.execute("UPDATE external_products SET status='archived', is_active=0 WHERE status='approved' AND is_active=1")
    return redirect("/external")


@app.route("/external")
@login_required
def external_list():
    with get_db() as c:
        rows = c.execute("SELECT * FROM external_products ORDER BY id DESC LIMIT 300").fetchall()
        active_count = c.execute("SELECT COUNT(*) n FROM external_products WHERE status='approved' AND is_active=1").fetchone()["n"]
    cards = ""
    for r in rows:
        status = (r['status'] or '')
        active = int(r['is_active'] or 0)
        preco = _format_brl(r['price']) if _safe_float(r['price'],0) else "—"
        nome = _corrigir_portugues_produto(r['name'] or 'Oferta selecionada')
        actions = ""
        if status == 'approved' and active:
            actions = f"""
            <form method='post' action='/external/{r['id']}/archive' style='display:inline' onsubmit="return confirm('Arquivar este item e tirar da fila?')">
              <button class='mini warn'>Arquivar</button>
            </form>
            <form method='post' action='/external/{r['id']}/delete' style='display:inline' onsubmit="return confirm('Excluir definitivamente?')">
              <button class='mini danger'>Excluir</button>
            </form>
            """
        else:
            actions = f"""
            <form method='post' action='/external/{r['id']}/delete' style='display:inline' onsubmit="return confirm('Excluir definitivamente?')">
              <button class='mini danger'>Excluir</button>
            </form>
            """
        cards += f"""
        <tr>
          <td><b>{r['id']}</b></td>
          <td>{r['source']}</td>
          <td>{nome}</td>
          <td>{preco}</td>
          <td>{r['priority']}</td>
          <td>{status}{'' if active else ' / inativo'}</td>
          <td>{actions}</td>
        </tr>
        """
    return render_template_string("""<!DOCTYPE html><html><head><meta charset='utf-8'>""" + CSS + """
    <title>Produtos Externos</title>
    <style>
      .tablex{width:100%;border-collapse:collapse;font-size:13px;min-width:760px}.tablex td,.tablex th{padding:10px;border-bottom:1px solid #eee;text-align:left;vertical-align:top}.hero{background:#111827;color:white;border-radius:18px;padding:18px;margin-bottom:14px}.quick{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px}.quick .btn{margin:0;text-align:center}.tablewrap{overflow-x:auto}.mini{border:0;border-radius:8px;padding:7px 9px;margin:2px;font-weight:800}.danger{background:#ef4444;color:#fff}.warn{background:#f59e0b;color:#111}.clearbox{margin-top:10px;padding:12px;background:#fff7ed;border-radius:14px}.clearbox button{background:#ef4444;color:#fff;border:0;border-radius:10px;padding:11px 14px;font-weight:900}@media(max-width:560px){.quick{grid-template-columns:1fr}.tablex{font-size:12px}}
    </style>
    </head><body><div class='header'><h1>⭐ Produtos Externos</h1><a href='/dashboard'>Voltar</a></div><div class='content'>
      <div class='hero'><h2>Fila premium</h2><p>Mercado Livre, Netshoes, Amazon e Shopee entram aqui e são priorizados antes da Shopee automática.</p><p><b>Ativos na fila:</b> """ + str(active_count) + """</p></div>
      <div class='card quick'>
        <a class='btn btn-green' href='/external/paste_bulk'>➕ Colar links</a>
        <a class='btn btn-blue' href='/external/new'>🛒 Novo produto</a>
        <a class='btn btn-orange' href='/external/import'>📊 Importar CSV</a>
      </div>
      <div class='clearbox'>
        <form method='post' action='/external/clear_queue' onsubmit="return confirm('Arquivar TODA a fila ativa de externos? Isso tira os links antigos da postagem.')">
          <button type='submit'>🧹 Limpar fila ativa de externos</button>
        </form>
      </div>
      <div class='card tablewrap'><table class='tablex'><tr><th>ID</th><th>Fonte</th><th>Produto</th><th>Preço</th><th>Prior.</th><th>Status</th><th>Ações</th></tr>""" + cards + """</table></div>
    </div></body></html>""")


@app.route("/external/new", methods=["GET", "POST"])
@login_required
def external_new():
    if request.method == "POST":
        source = request.form.get("source", "").strip()
        name = request.form.get("name", "").strip()
        price = request.form.get("price", "0").strip()
        old_price = request.form.get("old_price", "0").strip()
        image_url = request.form.get("image_url", "").strip()
        product_url = request.form.get("product_url", "").strip()
        affiliate_url = request.form.get("affiliate_url", "").strip()
        priority = int(request.form.get("priority", 5) or 5)
        affiliate_url = ajustar_link_afiliado_externo(source, affiliate_url or product_url)
        with get_db() as c:
            c.execute("""
                INSERT INTO external_products
                (source,name,price,old_price,image_url,product_url,affiliate_url,priority,status,is_active)
                VALUES (?,?,?,?,?,?,?,?,?,1)
            """, (source, name, price, old_price, image_url, product_url, affiliate_url, priority, "approved"))
        return redirect("/external")
    return """
    <h2>Novo Produto Externo</h2>
    <form method='post'>
      Fonte: <input name='source'><br><br>
      Nome: <input name='name' style='width:400px'><br><br>
      Preço: <input name='price'><br><br>
      Preço antigo: <input name='old_price'><br><br>
      Imagem: <input name='image_url' style='width:500px'><br><br>
      URL produto: <input name='product_url' style='width:500px'><br><br>
      Link afiliado: <input name='affiliate_url' style='width:500px'><br><br>
      Prioridade: <input name='priority' value='8'><br><br>
      <button>Salvar</button>
    </form>
    """


@app.route("/external/import", methods=["GET", "POST"])
@login_required
def external_import():
    if request.method == "POST":
        import csv, io
        file = request.files.get("file")
        if not file:
            return "Arquivo não enviado", 400
        stream = io.StringIO(file.stream.read().decode("utf-8"))
        reader = csv.DictReader(stream)
        added = 0
        with get_db() as c:
            for row in reader:
                source = (row.get("source") or "").strip()
                product_url = (row.get("product_url") or "").strip()
                affiliate_url = ajustar_link_afiliado_externo(source, (row.get("affiliate_url") or "").strip() or product_url)
                c.execute("""
                    INSERT INTO external_products
                    (source,name,price,old_price,image_url,product_url,affiliate_url,category,brand,coupon,priority,status,is_active)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,'approved',1)
                """, (
                    source, row.get("name",""), row.get("price",0), row.get("old_price",0), row.get("image_url",""),
                    product_url, affiliate_url, row.get("category",""), row.get("brand",""), row.get("coupon",""), row.get("priority",8)
                ))
                added += 1
        return f"Importado com sucesso: {added}"
    return """
    <h2>Importar CSV</h2>
    <form method='post' enctype='multipart/form-data'>
      <input type='file' name='file'>
      <button>Importar</button>
    </form>
    """


@app.route("/external/paste_bulk", methods=["GET", "POST"])
@login_required
def external_paste_bulk():
    if request.method == "POST":
        source = (request.form.get("source") or "").strip().lower()
        try:
            priority = int(request.form.get("priority") or 8)
        except Exception:
            priority = 8
        raw_links = request.form.get("links", "") or ""
        links = raw_links.splitlines()
        added = 0
        skipped = 0
        errors = []
        with get_db() as c:
            for idx, raw in enumerate(links, start=1):
                link = _limpar_url_compartilhamento(raw)
                if not link:
                    skipped += 1
                    continue
                if not (link.startswith("http://") or link.startswith("https://")):
                    skipped += 1
                    errors.append(f"Linha {idx}: link inválido")
                    continue
                try:
                    exists = c.execute("SELECT id FROM external_products WHERE product_url=? OR affiliate_url=? LIMIT 1", (link, link)).fetchone()
                    if exists:
                        skipped += 1
                        continue
                    dados = extrair_dados_link_externo(link, source)
                    aff = ajustar_link_afiliado_externo(source, link)
                    nome = _corrigir_portugues_produto(_nome_apresentavel_externo(dados))
                    c.execute("""
                        INSERT INTO external_products
                        (source,name,price,old_price,image_url,product_url,affiliate_url,coupon,priority,status,is_active)
                        VALUES (?,?,?,?,?,?,?,?,?,'approved',1)
                    """, (
                        source,
                        nome,
                        _safe_float(dados.get("price"), 0),
                        _safe_float(dados.get("old_price"), 0),
                        dados.get("image_url") or "",
                        dados.get("product_url") or link,
                        aff,
                        dados.get("coupon") or "",
                        priority
                    ))
                    added += 1
                except Exception as e:
                    skipped += 1
                    errors.append(f"Linha {idx}: {str(e)[:90]}")
                    try: log("WARN", f"[PASTE_BULK] linha {idx} falhou: {str(e)[:120]}")
                    except Exception: pass
        detalhe = ""
        if errors:
            detalhe = "<h3>Detalhes</h3><ul>" + "".join(f"<li>{e}</li>" for e in errors[:25]) + "</ul>"
        return f"""
        <h2>Importação concluída</h2>
        <p><b>{added}</b> link(s) adicionados.</p>
        <p><b>{skipped}</b> ignorado(s), duplicado(s) ou com erro.</p>
        {detalhe}
        <p><a href='/external'>Ver Produtos Externos</a></p>
        <p><a href='/external/paste_bulk'>Colar mais links</a></p>
        """
    return """
    <h2>Colar links em massa</h2>
    <form method='post'>
      Fonte:
      <select name='source'>
        <option value='mercadolivre'>Mercado Livre</option>
        <option value='netshoes'>Netshoes</option>
        <option value='amazon'>Amazon</option>
        <option value='shopee'>Shopee</option>
      </select><br><br>
      Prioridade: <input name='priority' value='8'><br><br>
      Cole os links afiliados ou links normais (1 por linha):<br>
      <textarea name='links' rows='14' style='width:100%'></textarea><br><br>
      <button>Salvar</button>
    </form>
    """


@app.route("/go/external/<int:pid>")
def go_external(pid):
    with get_db() as c:
        p = c.execute("SELECT * FROM external_products WHERE id=?", (pid,)).fetchone()
        if not p:
            return "Produto não encontrado", 404
        src = request.args.get("src", "unknown")
        c.execute("INSERT INTO external_clicks (external_product_id, source) VALUES (?,?)", (pid, src))
        c.execute("UPDATE external_products SET clicks = COALESCE(clicks,0) + 1 WHERE id=?", (pid,))
        url = p["affiliate_url"] or p["product_url"]
    return redirect(url)


@app.route("/vitrine")
@app.route("/linktree")
def vitrine():
    """
    Vitrine pública estável para o link da bio.
    Nunca deve derrubar com erro 500 por falha temporária de API.
    """
    affiliate_id = cfg("shopee_affiliate_id", "")
    nicho = cfg("niche_keyword", "geral") or "geral"

    def _safe_float(v, default=0.0):
        try:
            return float(v or 0)
        except Exception:
            return default

    def _safe_int(v, default=0):
        try:
            return int(v or 0)
        except Exception:
            return default

    def _fix_img(img):
        img = (img or "").strip()
        if not img:
            return ""
        if img.startswith("//"):
            return "https:" + img
        if not img.startswith("http"):
            return "https://cf.shopee.com.br/file/" + img
        return img

    def _row_to_dict(r):
        return {
            "name": (r["name"] or "Produto Brizzah").strip(),
            "price": _safe_float(r["price"]),
            "image_url": _fix_img(r["image_url"]),
            "affiliate_url": (r["affiliate_url"] or r["product_url"] or "").strip(),
            "sold": _safe_int(r["sold"] if "sold" in r.keys() else 0),
            "rating": _safe_float(r["rating"] if "rating" in r.keys() else 0, 0.0),
            "commission": _safe_float(r["commission"]),
            "source": ((r["shop_name"] if "shop_name" in r.keys() else "") or "").strip(),
        }

    erro_api = ""
    produtos_postados = []
    produtos_top100 = []
    produtos_externos = []

    try:
        with get_db() as c:
            rows_postados = c.execute(
                "SELECT * FROM products WHERE image_url IS NOT NULL AND image_url != '' AND name IS NOT NULL AND name != '' AND channels LIKE '%instagram%' ORDER BY id DESC LIMIT 60"
            ).fetchall()
            if not rows_postados:
                rows_postados = c.execute(
                    "SELECT * FROM products WHERE image_url IS NOT NULL AND image_url != '' AND name IS NOT NULL AND name != '' ORDER BY id DESC LIMIT 60"
                ).fetchall()
        produtos_postados = [_row_to_dict(r) for r in rows_postados]
    except Exception as e:
        log("ERROR", f"[VITRINE] erro ao carregar postados: {str(e)[:120]}")
        produtos_postados = []

    try:
        with get_db() as c:
            rows_ext = c.execute("""
                SELECT * FROM external_products
                WHERE status='approved' AND is_active=1
                ORDER BY priority DESC, id DESC
                LIMIT 6
            """).fetchall()
        host = (cfg("bot_url","") or os.environ.get("BOT_URL","https://shopee-bot-jt11.onrender.com")).rstrip("/")
        produtos_externos = [{
            "name": (r["name"] or "Oferta Premium").strip(),
            "price": _safe_float(r["price"]),
            "image_url": _fix_img(r["image_url"]),
            "affiliate_url": f"{host}/go/external/{r['id']}?src=vitrine",
            "sold": _safe_int(r["clicks"] if "clicks" in r.keys() else 0),
            "rating": 5.0,
            "commission": 0.0,
            "source": (r["source"] or "Externo").title(),
        } for r in rows_ext]
    except Exception as e:
        log("WARN", f"[VITRINE] erro ao carregar externos: {str(e)[:120]}")
        produtos_externos = []

    try:
        agora = time.time()
        usar_cache = (
            _vitrine_cache.get("dados") and
            _vitrine_cache.get("nicho") == nicho and
            agora - float(_vitrine_cache.get("ts", 0) or 0) < _VITRINE_CACHE_TTL
        )
        if usar_cache:
            produtos_api = _vitrine_cache.get("dados", []) or []
        else:
            produtos_api = buscar_pool_produtos_top(nicho_base=nicho)
            if produtos_api:
                _vitrine_cache.update({"dados": produtos_api, "ts": agora, "nicho": nicho})

        produtos_api = selecionar_produtos_top(produtos_api, nicho_alvo=nicho, limite=24, horas_repeticao=24)

        for p in (produtos_api or [])[:24]:
            url_orig = (p.get("affiliate_url") or p.get("product_url") or "").strip()
            url_aff = gerar_link_afiliado(url_orig, affiliate_id) if url_orig else ""
            produtos_top100.append({
                "name": (p.get("name") or "Produto Shopee").strip(),
                "price": _safe_float(p.get("price")),
                "image_url": _fix_img(p.get("image_url")),
                "affiliate_url": url_aff or url_orig,
                "sold": _safe_int(p.get("sold")),
                "rating": _safe_float(p.get("rating"), 0.0),
                "commission": _safe_float(p.get("commission_rate")),
                "source": "Shopee",
            })
    except Exception as e:
        erro_api = "As ofertas automáticas estão sendo atualizadas agora."
        log("ERROR", f"[VITRINE] erro ao carregar Shopee: {str(e)[:120]}")
        produtos_top100 = _vitrine_cache.get("dados", [])[:24] if _vitrine_cache.get("dados") else []
        produtos_top100 = [{
            "name": (p.get("name") or "Produto Shopee").strip(),
            "price": _safe_float(p.get("price")),
            "image_url": _fix_img(p.get("image_url")),
            "affiliate_url": (p.get("affiliate_url") or p.get("product_url") or "").strip(),
            "sold": _safe_int(p.get("sold")),
            "rating": _safe_float(p.get("rating"), 0.0),
            "commission": _safe_float(p.get("commission_rate") or p.get("commission")),
            "source": "Shopee",
        } for p in produtos_top100]

    if not produtos_top100 and produtos_postados:
        erro_api = erro_api or "Mostrando os produtos já publicados enquanto novas ofertas são atualizadas."

    template = """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Brizzah | Achados do dia</title>
  <meta name="description" content="Achados da Brizzah com ofertas atualizadas e produtos já postados no Instagram.">
  <style>
    :root{--bg:#f6f5f2;--card:#ffffff;--text:#202020;--muted:#777;--brand:#22c55e;--accent:#ee4d2d;--border:#e8e6e1}
    *{box-sizing:border-box} body{margin:0;font-family:Arial,sans-serif;background:var(--bg);color:var(--text)}
    .wrap{max-width:1120px;margin:0 auto;padding:18px}
    .hero{background:linear-gradient(135deg,#171717,#2f2f2f);color:#fff;border-radius:22px;padding:24px 18px 22px;margin-bottom:18px}
    .hero h1{margin:0 0 8px;font-size:28px}.hero p{margin:0;color:#e8e8e8;line-height:1.45}
    .hero .cta{display:inline-block;margin-top:14px;background:var(--brand);color:#fff;text-decoration:none;padding:12px 16px;border-radius:14px;font-weight:700}
    .note{background:#fff7ed;color:#9a3412;border:1px solid #fdba74;padding:12px 14px;border-radius:14px;margin:0 0 18px}
    .section{margin:20px 0 8px}.section h2{margin:0 0 6px;font-size:22px}.section p{margin:0;color:var(--muted)}
    .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:14px;margin-top:16px}
    .card{background:var(--card);border:1px solid var(--border);border-radius:18px;overflow:hidden;box-shadow:0 2px 10px rgba(0,0,0,.04)}
    .imgwrap{aspect-ratio:1/1;background:#fff;display:flex;align-items:center;justify-content:center;position:relative}
    .imgwrap img{width:100%;height:100%;object-fit:cover}
    .badge{position:absolute;top:10px;left:10px;background:#171717;color:#fff;font-size:12px;font-weight:700;padding:6px 10px;border-radius:999px}
    .badge2{position:absolute;top:10px;right:10px;background:var(--brand);color:#fff;font-size:12px;font-weight:700;padding:6px 10px;border-radius:999px}
    .body{padding:12px}.name{font-size:14px;font-weight:700;line-height:1.4;min-height:40px}
    .meta{display:flex;gap:8px;flex-wrap:wrap;margin:8px 0;color:var(--muted);font-size:12px}
    .price{font-size:24px;font-weight:800;color:var(--accent);margin:8px 0}
    .btn{display:block;text-align:center;background:#171717;color:#fff;text-decoration:none;padding:11px 12px;border-radius:12px;font-weight:700}
    .btn.alt{background:var(--brand)}
    .empty{background:#fff;border:1px dashed var(--border);padding:24px;border-radius:16px;color:var(--muted);text-align:center;margin-top:14px}
    .toplinks{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}.toplinks a{background:#fff;color:#171717;text-decoration:none;padding:10px 14px;border-radius:12px;font-weight:700;border:1px solid var(--border)}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <h1>Achados da Brizzah ✨</h1>
      <p>Os melhores achados do perfil em um só lugar. Esta página foi feita para abrir rápido, funcionar bem no Linktree e continuar estável mesmo quando alguma integração estiver atualizando.</p>
      <div class="toplinks">
        <a href="https://www.instagram.com/brizzah.br" target="_blank" rel="noopener">Instagram @brizzah.br</a>
        <a href="#ofertas">Ofertas do dia</a>
        <a href="#postados">Produtos já postados</a>
      </div>
    </div>

    {% if erro_api %}<div class="note">{{ erro_api }}</div>{% endif %}

    <div class="section" id="ofertas">
      <h2>Ofertas do dia</h2>
      <p>Achados atuais para clicar e conferir.</p>
    </div>
    {% if produtos_top100 %}
      <div class="grid">
        {% for p in produtos_top100 %}
          <div class="card">
            <div class="imgwrap">
              {% if p.image_url %}<img src="{{ p.image_url }}" alt="{{ p.name }}" loading="lazy">{% else %}<div>🛍️</div>{% endif %}
              <div class="badge">{{ p.source or 'Oferta' }}</div>
              {% if p.commission and p.commission > 0 %}<div class="badge2">-{{ '%.0f'|format(p.commission) }}%</div>{% endif %}
            </div>
            <div class="body">
              <div class="name">{{ p.name[:72] }}</div>
              <div class="meta">
                {% if p.sold %}<span>🔥 {{ p.sold }} vendidos</span>{% endif %}
                {% if p.rating %}<span>⭐ {{ '%.1f'|format(p.rating) }}</span>{% endif %}
              </div>
              <div class="price">R$ {{ '%.2f'|format(p.price or 0) | replace('.', ',') }}</div>
              {% if p.affiliate_url %}
                <a class="btn" href="{{ p.affiliate_url }}" target="_blank" rel="noopener nofollow">Ver oferta</a>
              {% else %}
                <a class="btn alt" href="https://www.instagram.com/brizzah.br" target="_blank" rel="noopener">Ver no Instagram</a>
              {% endif %}
            </div>
          </div>
        {% endfor %}
      </div>
    {% else %}
      <div class="empty">As ofertas estão sendo atualizadas agora. Tente novamente em alguns instantes.</div>
    {% endif %}

    <div class="section" id="postados" style="margin-top:28px">
      <h2>Produtos já postados no Instagram</h2>
      <p>Seleção do que já saiu no perfil da Brizzah.</p>
    </div>
    {% if produtos_postados %}
      <div class="grid">
        {% for p in produtos_postados[:24] %}
          <div class="card">
            <div class="imgwrap">
              {% if p.image_url %}<img src="{{ p.image_url }}" alt="{{ p.name }}" loading="lazy">{% else %}<div>📸</div>{% endif %}
              <div class="badge">Instagram</div>
            </div>
            <div class="body">
              <div class="name">{{ p.name[:72] }}</div>
              <div class="meta">
                {% if p.sold %}<span>🔥 {{ p.sold }} vendidos</span>{% endif %}
                {% if p.rating %}<span>⭐ {{ '%.1f'|format(p.rating) }}</span>{% endif %}
              </div>
              <div class="price">R$ {{ '%.2f'|format(p.price or 0) | replace('.', ',') }}</div>
              {% if p.affiliate_url %}
                <a class="btn alt" href="{{ p.affiliate_url }}" target="_blank" rel="noopener nofollow">Abrir produto</a>
              {% else %}
                <a class="btn alt" href="https://www.instagram.com/brizzah.br" target="_blank" rel="noopener">Ver perfil</a>
              {% endif %}
            </div>
          </div>
        {% endfor %}
      </div>
    {% else %}
      <div class="empty">Ainda não há produtos postados suficientes para mostrar aqui.</div>
    {% endif %}
  </div>
</body>
</html>
    """
    return render_template_string(template, produtos_top100=produtos_top100, produtos_postados=produtos_postados, erro_api=erro_api)

@app.route("/vitrine_produtos")
def vitrine_produtos():
    import json as _json
    plat = request.args.get("plataforma","").lower()
    nicho_atual = cfg("_nicho_ultimo","") or cfg("niche_keyword","moda feminina") or "moda feminina"
    if plat == "amazon":
        produtos = amazon_buscar_produtos(nicho_atual, limit=20) or amazon_buscar_produtos("moda feminina", limit=20)
    elif plat == "mercadolivre":
        produtos = mercadolivre_buscar_produtos(nicho_atual, limit=20) or mercadolivre_buscar_produtos("moda feminina", limit=20)
    else:
        return _json.dumps({"cards":[],"total":0}), 200, {"Content-Type":"application/json"}
    def _card(p, lbl):
        nome  = (p.get("name","") or "")[:70].replace("&","&amp;").replace("<","&lt;")
        link  = (p.get("affiliate_url") or p.get("product_url","") or "").replace("'","")
        img   = (p.get("image_url","") or "").replace("'","")
        if img.startswith("//"): img = "https:"+img
        try: pn = float(p.get("price",0) or 0)
        except: pn = 0.0
        # Card especial para link de busca (Amazon/ML sem produto específico)
        if p.get("is_search_link"):
            btn = f"<a href=\'{link}\' target=\'_blank\' rel=\'noopener nofollow\' class=\'vt-btn\' style=\'background:#ff9900\'>" + ("🛒 Buscar na Amazon" if "amazon" in (p.get("source","")) else "🟡 Buscar no ML") + "</a>"
            ph_img = "<img src=\'" + img + "\' class=\'vt-img\' style=\'object-fit:contain;padding:20px\' loading=\'lazy\'>"
            return f"<div class=\'vt-card\'><div class=\'vt-img-wrap\'>{ph_img}</div><div class=\'vt-card-body\'><p class=\'vt-nome\'>{nome}</p><p style=\'font-size:11px;color:#888\'>Clique para ver ofertas</p>{btn}</div></div>"
        preco = "R$ {:,.2f}".format(pn).replace(",","X").replace(".",",").replace("X",".")
        try: orig = float(p.get("original_price",0) or pn*1.3)
        except: orig = pn*1.3
        if orig <= pn: orig = pn*1.3
        desc = int((1-pn/orig)*100) if orig>0 else 0
        try: stars = float(p.get("rating",4.5) or 4.5)
        except: stars = 4.5
        desc_b = "<div class=\'vt-badge\'>🔥 -{}%</div>".format(desc) if desc>=10 else ""
        plat_b = "<div class=\'vt-ig-badge\'>{}</div>".format(lbl)
        ph = "https://placehold.co/400x400/f5f5f5/bbb?text=produto"
        img_t  = ("<img src=\'" + img + "\' class=\'vt-img\' loading=\'lazy\' "
                  + "onerror=\"this.src=\'"+ph+"\"\">" if img 
                  else "<div class=\'vt-img-placeholder\'>🛍️</div>")
        btn    = ("<a href=\'" + link + "\' target=\'_blank\' rel=\'noopener nofollow\' class=\'vt-btn\'>🛒 Ver oferta</a>" 
                  if link else "<div class=\'vt-btn-disabled\'>Em breve</div>")
        return ("<div class=\'vt-card\'><div class=\'vt-img-wrap\'>" + plat_b + desc_b + img_t + 
                "</div><div class=\'vt-card-body\'><p class=\'vt-nome\'>" + nome + 
                "</p><div class=\'vt-meta\'><span class=\'vt-stars\'>⭐ {:.1f}</span></div>".format(stars) +
                "<p class=\'vt-preco\'>" + preco + "</p>" + btn + "</div></div>")
    lbl   = "🛒 Amazon" if plat=="amazon" else "🟡 ML"
    cards = [_card(p,lbl) for p in (produtos or []) if p.get("name") and p.get("price")]
    return _json.dumps({"cards":cards,"total":len(cards)}), 200, {"Content-Type":"application/json"}


@app.route("/diagnostico_plataformas")
@login_required
def diagnostico_plataformas():
    """Diagnóstico completo: Shopee, Amazon, ML, Instagram, WhatsApp."""
    import time as _t
    resultados = []

    def ok(plat, msg):   resultados.append(("ok",   plat, msg))
    def err(plat, msg):  resultados.append(("err",  plat, msg))
    def warn(plat, msg): resultados.append(("warn", plat, msg))

    # ── 1. SHOPEE ─────────────────────────────────────────────
    try:
        t0 = _t.time()
        prods = shopee_api_buscar_produtos("camiseta", limit=3)
        dt = round(_t.time()-t0, 1)
        if prods:
            ok("Shopee", f"✅ {len(prods)} produto(s) encontrado(s) em {dt}s — ex: {prods[0]['name'][:40]}")
        else:
            err("Shopee", f"⚠️ API retornou 0 produtos para 'camiseta' ({dt}s) — verifique credenciais")
    except Exception as e:
        err("Shopee", f"❌ Erro na API: {str(e)[:80]}")

    # ── 2. AMAZON ─────────────────────────────────────────────
    amz_tag = cfg("amazon_affiliate_tag","brizzah-20")
    if amz_tag:
        try:
            t0 = _t.time()
            prods = amazon_buscar_produtos("camiseta", limit=3)
            dt = round(_t.time()-t0, 1)
            if prods:
                link = prods[0].get("affiliate_url","")
                ok("Amazon", f"✅ {len(prods)} produto(s) em {dt}s — tag={amz_tag} — link: {link[:50]}")
            else:
                warn("Amazon", f"⚠️ 0 produtos retornados em {dt}s — Amazon pode estar bloqueando scraping")
        except Exception as e:
            err("Amazon", f"❌ Erro: {str(e)[:80]}")
    else:
        warn("Amazon", "⚠️ Tag não configurada — acesse /config_plataformas")

    # ── 3. MERCADO LIVRE ──────────────────────────────────────
    ml_id = cfg("ml_affiliate_id","ad20260407202239")
    try:
        t0 = _t.time()
        prods = mercadolivre_buscar_produtos("camiseta", limit=3)
        dt = round(_t.time()-t0, 1)
        if prods:
            link = prods[0].get("affiliate_url","")
            ok("Mercado Livre", f"✅ {len(prods)} produto(s) em {dt}s — ID={ml_id} — link: {link[:50]}")
        else:
            warn("Mercado Livre", f"⚠️ 0 produtos retornados em {dt}s")
    except Exception as e:
        err("Mercado Livre", f"❌ Erro na API: {str(e)[:80]}")

    # ── 4. INSTAGRAM ──────────────────────────────────────────
    ig_token = cfg("instagram_access_token","")
    ig_uid   = cfg("instagram_user_id","")
    if ig_token and ig_uid:
        try:
            import requests as _rq
            r = _rq.get(f"https://graph.facebook.com/v19.0/{ig_uid}",
                        params={"fields":"username,followers_count","access_token":ig_token},
                        timeout=8)
            d = r.json()
            if "username" in d:
                ok("Instagram", f"✅ Token válido — @{d['username']} | {d.get('followers_count','?')} seguidores")
            elif "error" in d:
                err("Instagram", f"❌ Token inválido: {d['error'].get('message','')[:60]}")
            else:
                warn("Instagram", f"⚠️ Resposta inesperada: {str(d)[:60]}")
        except Exception as e:
            err("Instagram", f"❌ Erro ao verificar token: {str(e)[:60]}")
    else:
        err("Instagram", "❌ Token ou User ID não configurado — acesse Configurar Instagram API")

    # ── 5. WHATSAPP ───────────────────────────────────────────
    wa_inst  = cfg("whatsapp_instance_id","")
    wa_token = cfg("whatsapp_token","")
    wa_group = cfg("whatsapp_group_id","")
    wa_ativo = cfg("post_whatsapp","false") == "true"
    if not wa_ativo:
        warn("WhatsApp", "⚠️ WhatsApp desativado nas configurações (post_whatsapp=false)")
    elif wa_inst and wa_token:
        try:
            import requests as _rq
            r = _rq.get("https://evolution-api-lad2.onrender.com/instance/fetchInstances",
                        headers={"apikey": wa_token}, timeout=10)
            if r.status_code == 200:
                insts = r.json()
                nomes = [i.get("instance",{}).get("instanceName","") for i in insts] if isinstance(insts,list) else []
                if wa_inst in nomes:
                    ok("WhatsApp", f"✅ Evolution API online — instância '{wa_inst}' encontrada")
                    if not wa_group:
                        warn("WhatsApp", "⚠️ Group ID não configurado — posts WA desativados")
                    else:
                        ok("WhatsApp", f"✅ Grupo configurado: {wa_group[:30]}")
                else:
                    warn("WhatsApp", f"⚠️ Instância '{wa_inst}' não encontrada — instâncias: {nomes}")
            else:
                err("WhatsApp", f"❌ Evolution API retornou status {r.status_code}")
        except Exception as e:
            err("WhatsApp", f"❌ Evolution API inacessível: {str(e)[:60]}")
    else:
        err("WhatsApp", "❌ Instância ou token não configurado")

    # ── 6. MULTI-PLATAFORMA ───────────────────────────────────
    try:
        t0 = _t.time()
        multi = buscar_multiplas_plataformas("camiseta feminina", limit_cada=2)
        dt = round(_t.time()-t0, 1)
        fontes = {}
        for p in multi:
            f = p.get("source","shopee")
            fontes[f] = fontes.get(f,0)+1
        ok("Multi-plataforma", f"✅ {len(multi)} produto(s) em {dt}s — fontes: {fontes}")
    except Exception as e:
        err("Multi-plataforma", f"❌ buscar_multiplas_plataformas: {str(e)[:80]}")

    # ── HTML ──────────────────────────────────────────────────
    linhas = ""
    for tipo, plat, msg in resultados:
        cor = {"ok":"#e8f5e9","err":"#ffebee","warn":"#fff8e1"}[tipo]
        icone = {"ok":"✅","err":"❌","warn":"⚠️"}[tipo]
        linhas += f"""<div style='background:{cor};border-radius:10px;padding:12px 14px;margin-bottom:10px'>
  <b style='font-size:13px'>{icone} {plat}</b><br>
  <span style='font-size:12px;color:#444'>{msg}</span>
</div>"""

    return f"""<!DOCTYPE html><html><head>
<meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Diagnóstico Plataformas</title>"""+CSS+"""
</head><body>
<div class='header'><h1>🔍 Diagnóstico Plataformas</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>
<div style='background:#e3f2fd;border-radius:10px;padding:12px;margin-bottom:16px;font-size:13px'>
  ℹ️ Verificando conexão com Shopee, Amazon, Mercado Livre, Instagram e WhatsApp...
</div>"""+linhas+"""
<a href='/diagnostico_plataformas' style='display:block;background:#ee4d2d;color:#fff;
   text-align:center;padding:13px;border-radius:10px;font-weight:700;text-decoration:none;margin-top:8px'>
  🔄 Testar novamente
</a>
<a href='/config_plataformas' style='display:block;background:#fff;color:#ee4d2d;border:2px solid #ee4d2d;
   text-align:center;padding:12px;border-radius:10px;font-weight:700;text-decoration:none;margin-top:10px'>
  ⚙️ Configurar Plataformas
</a>
</div></body></html>"""


@app.route("/wa_diagnostico")
@login_required
def wa_diagnostico():
    import requests as _rq
    status = []
    def ok(msg):   status.append(("ok",   msg))
    def err(msg):  status.append(("err",  msg))
    def warn(msg): status.append(("warn", msg))
    wa_inst  = cfg("whatsapp_instance_id","")
    wa_token = cfg("whatsapp_token","")
    wa_group = cfg("whatsapp_group_id","")
    post_wa  = cfg("post_whatsapp","false") == "true"
    wa_auto  = cfg("wa_auto_ativo","false") == "true"
    intervalo= cfg("wa_auto_intervalo","15")
    qtd      = cfg("wa_auto_qtd","2")
    ok(f"Instance ID: {'OK: ' + wa_inst if wa_inst else 'VAZIO — configure em /config'}")
    ok(f"Token: {'configurado' if wa_token else 'VAZIO — configure em /config'}")
    ok(f"Group ID: {'OK: ' + wa_group if wa_group else 'VAZIO — configure em /config'}")
    (ok if post_wa else warn)(f"Ciclo principal: {'ATIVO' if post_wa else 'DESATIVADO — ative Post WhatsApp em /config'}")
    (ok if wa_auto else err)(f"WA-AUTO: {'ATIVO — a cada ' + intervalo + ' min, ' + qtd + ' produto(s)' if wa_auto else 'DESATIVADO — ative em /wa_config'}")
    if wa_inst and wa_token:
        try:
            r = _rq.get("https://evolution-api-lad2.onrender.com/instance/fetchInstances",
                        headers={"apikey": wa_token}, timeout=10)
            if r.status_code == 200:
                insts = r.json()
                nomes = [i.get("instance",{}).get("instanceName","") for i in (insts if isinstance(insts,list) else [])]
                (ok if wa_inst in nomes else err)(f"Evolution API: {'online, instancia encontrada' if wa_inst in nomes else 'instancia nao encontrada: ' + str(nomes)}")
            else:
                err(f"Evolution API: status {r.status_code}")
        except Exception as e:
            err(f"Evolution API inacessivel: {str(e)[:50]}")
        try:
            r2 = _rq.get(f"https://evolution-api-lad2.onrender.com/instance/connectionState/{wa_inst}",
                         headers={"apikey": wa_token}, timeout=10)
            state = (r2.json().get("instance",{}) or {}).get("state","") or r2.json().get("state","")
            (ok if state=="open" else err)(f"WhatsApp estado: {state if state else 'desconhecido'}")
        except Exception as e:
            warn(f"Estado WA: {str(e)[:50]}")
    else:
        err("Impossivel testar sem Instance ID e Token")
    linhas = "".join(
        f"<div style='background:{'#e8f5e9' if t=='ok' else '#ffebee' if t=='err' else '#fff8e1'};border-radius:10px;padding:12px;margin-bottom:8px;font-size:13px'>{'✅' if t=='ok' else '❌' if t=='err' else '⚠️'} {m}</div>"
        for t,m in status)
    acoes = []
    if not wa_inst or not wa_token or not wa_group: acoes.append("Configure Instance ID, Token e Group ID em <a href='/config'>/config</a>")
    if not post_wa: acoes.append("Ative <b>Post WhatsApp</b> em <a href='/config'>/config</a>")
    if not wa_auto: acoes.append("Ative <b>Auto WhatsApp</b> em <a href='/wa_config'>/wa_config</a>")
    acoes_html = "".join(f"<li style='margin-bottom:8px'>{a}</li>" for a in acoes) or "<li>Tudo configurado! Aguarde o proximo intervalo.</li>"
    return f"""<!DOCTYPE html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Diagnostico WA</title>"""+CSS+"""</head><body>
<div class='header'><h1>📱 Diagnóstico WhatsApp</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>"""+linhas+f"""
<div style='background:#fff3e0;border-radius:10px;padding:14px;margin-top:12px'>
<b>📋 O que fazer:</b><ul style='margin:10px 0 0 16px;font-size:13px'>{acoes_html}</ul></div>
<div style='margin-top:14px;display:flex;flex-direction:column;gap:8px'>
<a href='/wa_diagnostico' style='display:block;background:#25D366;color:#fff;text-align:center;padding:12px;border-radius:10px;font-weight:700;text-decoration:none'>🔄 Testar Novamente</a>
<a href='/wa_config' style='display:block;background:#fff;color:#25D366;border:2px solid #25D366;text-align:center;padding:11px;border-radius:10px;font-weight:700;text-decoration:none'>⚙️ Auto WhatsApp</a>
<a href='/config' style='display:block;background:#fff;color:#333;border:2px solid #ddd;text-align:center;padding:11px;border-radius:10px;font-weight:700;text-decoration:none'>🔧 Configurações</a>
</div></div></body></html>"""


@app.route("/wa_amazon", methods=["GET","POST"])
@login_required
def wa_amazon():
    CATS = [
        ("Moda Feminina","vestido feminino"),("Tenis","tenis esportivo"),
        ("Eletronicos","fone bluetooth"),("Smartwatch","smartwatch"),
        ("Casa e Cozinha","airfryer"),("Beleza","perfume feminino"),
        ("Fitness","kit academia"),("Informatica","mouse sem fio"),
        ("Esportes","bola futebol"),("Kit Presente","kit presente"),
    ]
    wa_inst  = cfg("whatsapp_instance_id","")
    wa_token = cfg("whatsapp_token","")
    wa_group = cfg("whatsapp_group_id","")
    resultado = ""
    if request.method == "POST":
        acao = request.form.get("acao","")
        if acao == "auto":
            idx = int(request.form.get("categoria","0")) % len(CATS)
            cat_nome, cat_kw = CATS[idx]
            if wa_inst and wa_token and wa_group:
                ok_r, res = postar_amazon_wa(cat_nome, cat_kw, wa_inst, wa_token, wa_group)
                resultado = ("OK: Amazon '" + cat_nome + "' postado!") if ok_r else ("ERRO: " + res[:80])
            else:
                resultado = "ERRO: Configure WhatsApp em /config"
        elif acao == "manual":
            import urllib.parse as _up
            url_orig = request.form.get("url","").strip()
            titulo   = request.form.get("titulo","Produto Amazon").strip() or "Produto Amazon"
            preco    = request.form.get("preco","").strip()
            if url_orig:
                tag = cfg("amazon_affiliate_tag","brizzah-20")
                # Normaliza URL: adiciona https:// se necessário
                if url_orig and not url_orig.startswith("http"):
                    url_orig = "https://" + url_orig
                # Para links curtos amzn.to, não adiciona tag (já está no redirector)
                # Para links completos, adiciona tag
                if "amzn.to" in url_orig or "amzn.eu" in url_orig:
                    link_aff = url_orig  # link curto já tem rastreamento
                else:
                    sep = "&" if "?" in url_orig else "?"
                    link_aff = url_orig if "tag=" in url_orig else url_orig + sep + "tag=" + tag
                img_url = gerar_imagem_amazon_banner(titulo[:20])
                preco_str = ("\n\nA partir de R$ " + preco) if preco else ""
                caption = ("*" + titulo + "*\n\nOferta incrivel na Amazon!" + preco_str +
                           "\n\nEntrega rapida  Garantia Amazon\n\n" + link_aff +
                           "\n\n_@brizzah.br — achadinhos todo dia!_")
                if wa_inst and wa_token and wa_group:
                    ok_r, res = whatsapp_post(img_url or "", caption, wa_inst, wa_token, wa_group)
                    resultado = "OK: Postado com sucesso no grupo!" if ok_r else "ERRO: " + res[:120]
                else:
                    faltando = []
                    if not wa_inst:  faltando.append("Instance ID")
                    if not wa_token: faltando.append("Token")
                    if not wa_group: faltando.append("Group ID")
                    resultado = "ERRO: WhatsApp nao configurado. Falta: " + ", ".join(faltando) + ". Acesse /config"
            else:
                resultado = "ERRO: Cole uma URL da Amazon no campo acima"
    opts = "".join("<option value='" + str(i) + "'>" + n + "</option>" for i,(n,_) in enumerate(CATS))
    cor_res = "#e8f5e9" if resultado.startswith("OK") else "#ffebee" if resultado.startswith("ERRO") else ""
    res_html = ("<div style='background:" + cor_res + ";border-radius:10px;padding:12px;margin-bottom:14px;font-weight:600'>" + resultado + "</div>") if resultado else ""
    return """<!DOCTYPE html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Amazon WA</title>"""+CSS+"""
<style>.c{width:100%;padding:11px;border:1px solid #ddd;border-radius:8px;font-size:14px;box-sizing:border-box;margin-bottom:10px}
.card{background:#fff;border-radius:12px;padding:16px;margin-bottom:14px;box-shadow:0 2px 8px rgba(0,0,0,.08)}
.btna{background:#FF9900;color:#fff;border:none;border-radius:8px;padding:13px;width:100%;font-size:15px;font-weight:700;cursor:pointer;margin-bottom:8px}
.btnb{background:#232f3e;color:#fff;border:none;border-radius:8px;padding:13px;width:100%;font-size:15px;font-weight:700;cursor:pointer}</style>
</head><body><div class='header'><h1>🛒 Amazon → WhatsApp</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>""" + res_html + """
<div class='card'><h3 style='margin-bottom:12px'>⚡ Postar Categoria</h3>
<p style='font-size:13px;color:#666;margin-bottom:12px'>Selecione e o bot monta a oferta automaticamente.</p>
<form method='POST'><input type='hidden' name='acao' value='auto'>
<select name='categoria' class='c'>""" + opts + """</select>
<button class='btna'>🚀 Postar no WhatsApp agora</button></form></div>
<div class='card'><h3 style='margin-bottom:12px'>🔗 Link Específico</h3>
<p style='font-size:13px;color:#666;margin-bottom:12px'>Cole qualquer link Amazon — o bot adiciona sua tag de afiliado.</p>
<form method='POST'><input type='hidden' name='acao' value='manual'>
<label style='font-size:13px;font-weight:600;display:block;margin-bottom:4px'>URL Amazon</label>
<input class='c' name='url' placeholder='https://amzn.to/... ou https://www.amazon.com.br/dp/...' type='text'>
<label style='font-size:13px;font-weight:600;display:block;margin-bottom:4px'>Titulo do produto</label>
<input class='c' name='titulo' placeholder='Ex: Tenis Nike Air Max'>
<label style='font-size:13px;font-weight:600;display:block;margin-bottom:4px'>Preco (opcional)</label>
<input class='c' name='preco' placeholder='Ex: 199,90'>
<button class='btnb'>📤 Enviar para o Grupo</button></form>
<p style='font-size:11px;color:#888;margin-top:8px'>✅ Aceita links curtos (amzn.to) e links completos</p>
</div>
<div style='background:#fff8e1;border-radius:10px;padding:12px;font-size:12px;color:#666'>
💡 Qualquer compra pelo link em 24h gera comissao (tag: brizzah-20)</div>
</div></body></html>"""


@app.route("/postar_link", methods=["GET","POST"])
@login_required
def postar_link():
    """Cola qualquer link de produto → bot extrai info → posta no WA."""
    resultado = ""
    produto_preview = None

    def extrair_produto_do_link(url):
        """Extrai nome, preço e imagem de qualquer link de produto."""
        import requests as _rq, re as _re
        hdrs = {"User-Agent":"Mozilla/5.0 (Linux; Android 12) AppleWebKit/537.36","Accept-Language":"pt-BR,pt;q=0.9"}

        # ── Shopee ──────────────────────────────────────────
        if "shopee.com.br" in url:
            m = _re.search(r"/product/(\d+)/(\d+)", url)
            if not m: m = _re.search(r"-i\.(\d+)\.(\d+)", url)
            if m:
                shop_id, item_id = m.group(1), m.group(2)
                try:
                    r = _rq.get(f"https://shopee.com.br/api/v4/item/get?shopid={shop_id}&itemid={item_id}",
                                headers=hdrs, timeout=10)
                    d = r.json().get("data",{})
                    nome  = d.get("name","")
                    preco = float(d.get("price",0) or d.get("price_min",0) or 0)/100000
                    img   = "https://cf.shopee.com.br/file/" + (d.get("image","") or "")
                    aff   = cfg("shopee_affiliate_id","18345690956")
                    link  = f"{url}?smtt={aff}" if url else url
                    return {"nome":nome,"preco":preco,"img":img,"link":link,"fonte":"Shopee","emoji":"🛍️"}
                except: pass
            # Fallback: scraping
            try:
                r = _rq.get(url, headers=hdrs, timeout=10)
                nome  = _re.search(r'"name":"([^"]{10,120})"', r.text)
                preco = _re.search(r'"price":(\d+)', r.text)
                img   = _re.search(r'"image":"(https://[^"]+shopee[^"]+)"', r.text)
                return {
                    "nome":  nome.group(1) if nome else "Produto Shopee",
                    "preco": float(preco.group(1))/100000 if preco else 0,
                    "img":   img.group(1) if img else "",
                    "link":  url, "fonte":"Shopee","emoji":"🛍️"
                }
            except: pass

        # ── Amazon ──────────────────────────────────────────
        if "amazon.com.br" in url or "amzn.to" in url or "amzn.eu" in url:
            tag = cfg("amazon_affiliate_tag","brizzah-20")
            # Resolve link curto
            if "amzn.to" in url or "amzn.eu" in url:
                try:
                    r = _rq.head(url, allow_redirects=True, timeout=8, headers=hdrs)
                    url_full = r.url
                except: url_full = url
            else:
                url_full = url
            # Adiciona tag
            sep  = "&" if "?" in url_full else "?"
            link = url_full if "tag=" in url_full else f"{url_full}{sep}tag={tag}"
            try:
                r    = _rq.get(url_full, headers=hdrs, timeout=10)
                nome = _re.search(r'id="productTitle"[^>]*>\s*([^<]{5,200})', r.text)
                preco= _re.search(r'class="a-price-whole">([0-9.]+)<', r.text)
                img  = _re.search(r'"hiRes":"(https://[^"]+)"', r.text)
                if not img: img = _re.search(r'data-old-hires="(https://[^"]+)"', r.text)
                return {
                    "nome":  nome.group(1).strip() if nome else "Produto Amazon",
                    "preco": float((preco.group(1) if preco else "0").replace(".","")) if preco else 0,
                    "img":   img.group(1) if img else "",
                    "link":  link, "fonte":"Amazon","emoji":"🛒"
                }
            except:
                return {"nome":"Produto Amazon","preco":0,"img":"","link":link,"fonte":"Amazon","emoji":"🛒"}

        # ── Mercado Livre ────────────────────────────────────
        if "mercadolivre.com.br" in url or "mercadolibre.com" in url:
            aff  = cfg("ml_affiliate_id","ad20260407202239")
            import urllib.parse as _up
            link = f"https://www.mercadolivre.com.br/afiliados?aff_id={aff}&url={_up.quote(url)}" if aff else url
            try:
                r    = _rq.get(url, headers=hdrs, timeout=10)
                nome = _re.search(r'"name"\s*:\s*"([^"]{10,150})"', r.text)
                preco= _re.search(r'"price"\s*:\s*(\d+\.?\d*)', r.text)
                img  = _re.search(r'"thumbnail"\s*:\s*"(https://[^"]+\.jpg)"', r.text)
                return {
                    "nome":  nome.group(1) if nome else "Produto Mercado Livre",
                    "preco": float(preco.group(1)) if preco else 0,
                    "img":   img.group(1).replace("I.jpg","O.jpg") if img else "",
                    "link":  link, "fonte":"Mercado Livre","emoji":"🟡"
                }
            except:
                return {"nome":"Produto Mercado Livre","preco":0,"img":"","link":link,"fonte":"Mercado Livre","emoji":"🟡"}

        # ── Netshoes ─────────────────────────────────────────
        if "netshoes.com.br" in url or "zattini.com.br" in url:
            aff  = cfg("netshoes_affiliate_id","4686648")
            import urllib.parse as _up
            link = f"https://click.linksynergy.com/deeplink?id={aff}&mid=42238&murl={_up.quote(url)}" if aff else url
            try:
                r    = _rq.get(url, headers=hdrs, timeout=10)
                nome = _re.search(r'"name"\s*:\s*"([^"]{10,150})"', r.text)
                preco= _re.search(r'"price"\s*:\s*"?(\d+\.?\d*)"?', r.text)
                img  = _re.search(r'"image"\s*:\s*"(https://[^"]+)"', r.text)
                return {
                    "nome":  nome.group(1) if nome else "Produto Netshoes",
                    "preco": float(preco.group(1)) if preco else 0,
                    "img":   img.group(1) if img else "",
                    "link":  link, "fonte":"Netshoes","emoji":"👟"
                }
            except:
                return {"nome":"Produto Netshoes","preco":0,"img":"","link":link,"fonte":"Netshoes","emoji":"👟"}

        return None

    wa_inst  = cfg("whatsapp_instance_id","brizzah-bot")
    wa_token = cfg("whatsapp_token","Brizzah@2025!")
    wa_group = cfg("whatsapp_group_id","120363407236556172@g.us")

    if request.method == "POST":
        url     = request.form.get("url","").strip()
        titulo  = request.form.get("titulo","").strip()
        preco_f = request.form.get("preco","").strip()
        obs     = request.form.get("obs","").strip()

        if not url:
            resultado = "ERRO: Cole um link de produto"
        else:
            # Normaliza URL
            if not url.startswith("http"):
                url = "https://" + url

            acao = request.form.get("acao","postar")

            if acao == "buscar":
                # Só busca info, não posta
                produto_preview = extrair_produto_do_link(url)
                if produto_preview:
                    resultado = "OK: Produto encontrado! Revise e clique em Postar."
                else:
                    resultado = "AVISO: Não consegui extrair dados. Preencha manualmente."
                    produto_preview = {"nome":"","preco":0,"img":"","link":url,"fonte":"","emoji":"🛍️"}

            elif acao == "postar":
                # Busca ou usa dados do form
                nome  = titulo
                preco = 0.0
                img   = request.form.get("img","").strip()
                link  = url

                if not nome:
                    p = extrair_produto_do_link(url)
                    if p:
                        nome  = p["nome"]
                        preco = p["preco"]
                        img   = img or p["img"]
                        link  = p["link"]
                        emoji = p["emoji"]
                        fonte = p["fonte"]
                    else:
                        nome, emoji, fonte = "Produto", "🛍️", ""
                else:
                    emoji, fonte = "🛍️", ""
                    if "amazon" in url.lower() or "amzn" in url.lower():
                        emoji, fonte = "🛒", "Amazon"
                    elif "mercadolivre" in url.lower():
                        emoji, fonte = "🟡", "Mercado Livre"
                    elif "netshoes" in url.lower():
                        emoji, fonte = "👟", "Netshoes"
                    elif "shopee" in url.lower():
                        emoji, fonte = "🛍️", "Shopee"

                try:
                    preco = float(preco_f.replace(",",".")) if preco_f else preco
                except: pass

                preco_str = f"R$ {preco:,.2f}".replace(",","X").replace(".",",").replace("X",".") if preco > 0 else ""
                obs_str   = f"\n\n_{obs}_" if obs else ""
                fonte_str = f" | {fonte}" if fonte else ""

                caption = (
                    f"*{emoji} ACHADO DO DIA{fonte_str}*\n\n"
                    f"*{nome[:80]}*\n\n"
                    + (f"💰 *{preco_str}*\n\n" if preco_str else "")
                    + f"✅ Entrega rápida\n"
                    f"✅ Melhor preço garantido\n\n"
                    + (obs_str + "\n\n" if obs else "")
                    + f"🔗 {link}\n\n"
                    f"_@brizzah.br — achadinhos todo dia!_ 🔥"
                )

                # Garante imagem: se não extraiu, gera banner
                if not img:
                    img = gerar_imagem_amazon_banner(nome[:20] or fonte or "Oferta")
                if wa_inst and wa_token and wa_group:
                    ok_r, res = whatsapp_post(img or "", caption, wa_inst, wa_token, wa_group)
                else:
                    ok_r, res = False, "WhatsApp não configurado"

                resultado = f"OK: Postado no grupo!" if ok_r else f"ERRO: {res[:100]}"

    return """<!DOCTYPE html><html><head>
<meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Postar Link</title>"""+CSS+"""
<style>
.c{width:100%;padding:11px;border:1px solid #ddd;border-radius:8px;font-size:14px;
   box-sizing:border-box;margin-bottom:12px;font-family:inherit}
.card{background:#fff;border-radius:12px;padding:16px;margin-bottom:14px;
      box-shadow:0 2px 8px rgba(0,0,0,.08)}
.btn-ok{background:#25D366;color:#fff;border:none;border-radius:8px;padding:13px;
        width:100%;font-size:15px;font-weight:700;cursor:pointer;margin-bottom:8px}
.btn-sec{background:#f0f0f0;color:#333;border:none;border-radius:8px;padding:11px;
         width:100%;font-size:14px;font-weight:600;cursor:pointer}
.plat{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
.plat span{background:#f5f5f5;border-radius:20px;padding:5px 12px;font-size:12px;font-weight:600}
</style></head><body>
<div class='header'><h1>🔗 Postar Link no WA</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>
""" + (f"<div style='background:{'#e8f5e9' if resultado.startswith('OK') else '#fff3e0' if resultado.startswith('AVISO') else '#ffebee'};border-radius:10px;padding:12px;margin-bottom:14px;font-weight:600'>{'✅' if resultado.startswith('OK') else '⚠️' if resultado.startswith('AVISO') else '❌'} {resultado}</div>" if resultado else "") + """
<div class='plat'>
  <span>🛍️ Shopee</span><span>🛒 Amazon</span>
  <span>🟡 Mercado Livre</span><span>👟 Netshoes</span>
</div>
<form method='POST'>
  <div class='card'>
    <label style='font-weight:700;font-size:14px;display:block;margin-bottom:8px'>
      📎 Cole o link do produto
    </label>
    <input class='c' name='url' type='text'
           placeholder='https://shopee.com.br/... ou https://amzn.to/... ou qualquer loja'
           value='""" + (request.form.get("url","") if request.method=="POST" else "") + """'>
    <div style='display:flex;gap:8px'>
      <button type='submit' name='acao' value='buscar' class='btn-sec'>
        🔍 Buscar informações
      </button>
    </div>
  </div>
""" + (f"""
  <div class='card' style='border:2px solid #25D366'>
    <b style='font-size:13px;color:#25D366'>✅ Produto encontrado — revise se quiser:</b>
    <div style='display:flex;gap:12px;margin:10px 0;align-items:center'>
      {"<img src='" + produto_preview['img'] + "' style='width:70px;height:70px;object-fit:cover;border-radius:8px'>" if produto_preview and produto_preview.get('img') else ""}
      <div style='flex:1;font-size:13px'>
        <b>{produto_preview['nome'][:60] if produto_preview else ''}</b><br>
        <span style='color:#ee4d2d'>R$ {produto_preview['preco']:,.2f}</span>
        <span style='color:#888'> • {produto_preview['fonte']}</span>
      </div>
    </div>
  </div>
""" if produto_preview else "") + """
  <div class='card'>
    <label style='font-weight:600;font-size:13px;display:block;margin-bottom:4px'>
      ✏️ Título (deixe vazio para buscar automático)
    </label>
    <input class='c' name='titulo' placeholder='Ex: Tênis Nike Air Max Branco'
           value='""" + (produto_preview["nome"] if produto_preview else "") + """'>
    <input type='hidden' name='img' value='""" + (produto_preview["img"] if produto_preview else "") + """'>

    <label style='font-weight:600;font-size:13px;display:block;margin-bottom:4px'>
      💰 Preço (deixe vazio para buscar automático)
    </label>
    <input class='c' name='preco' placeholder='Ex: 89,90'
           value='""" + (f"{produto_preview['preco']:.2f}".replace(".",",") if produto_preview and produto_preview.get("preco") else "") + """'>

    <label style='font-weight:600;font-size:13px;display:block;margin-bottom:4px'>
      💬 Observação extra (opcional)
    </label>
    <input class='c' name='obs' placeholder='Ex: Frete grátis! Só hoje!'>
  </div>

  <button type='submit' name='acao' value='postar' class='btn-ok'>
    📤 Postar no Grupo WhatsApp agora
  </button>
</form>

<div style='background:#e3f2fd;border-radius:10px;padding:12px;font-size:12px;color:#555;margin-top:4px'>
  💡 <b>Dica:</b> Cole o link → clique "Buscar informações" → confira → clique "Postar".<br>
  Funciona com Shopee, Amazon (amzn.to), Mercado Livre e Netshoes.
</div>
</div></body></html>"""


@app.route("/top_performers")
@login_required
def top_performers():
    """Dashboard de performance — produtos que mais converteram."""
    with get_db() as c:
        top = c.execute("""
            SELECT pf.*,
                   (SELECT COUNT(*) FROM clicks ck
                    WHERE ck.product_hash=pf.product_hash) as total_clicks
            FROM performance pf
            ORDER BY total_clicks DESC, score DESC, posts DESC LIMIT 30
        """).fetchall()
        stats = c.execute("""
            SELECT COUNT(*) total, AVG(score) avg_score,
                   SUM(posts) total_posts,
                   COUNT(CASE WHEN posts>=2 THEN 1 END) repostados,
                   (SELECT COUNT(*) FROM clicks) total_clicks
            FROM performance
        """).fetchone()

    linhas = ""
    for i,r in enumerate(top):
        src_emoji = {"amazon":"🛒","mercadolivre":"🟡","netshoes":"👟"}.get(r["source"],"🛍️")
        img = r["image_url"] or ""
        if img.startswith("//"): img = "https:"+img
        linhas += f"""
<div style='display:flex;gap:12px;padding:12px;border-bottom:1px solid #f0f0f0;align-items:center'>
  <div style='font-size:20px;font-weight:800;color:#ee4d2d;min-width:30px'>#{i+1}</div>
  {"<img src='"+img+"' style='width:56px;height:56px;object-fit:cover;border-radius:8px'>" if img else "<div style='width:56px;height:56px;background:#f5f5f5;border-radius:8px;display:flex;align-items:center;justify-content:center'>🛍️</div>"}
  <div style='flex:1;min-width:0'>
    <div style='font-size:12px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis'>{src_emoji} {(r["name"] or "")[:50]}</div>
    <div style='font-size:11px;color:#888;margin-top:2px'>R$ {r["price"] or 0:.2f} • {r["posts"] or 0} posts • {r["total_clicks"] if "total_clicks" in r.keys() else 0} cliques • Score: {r["score"] or 0:.0f}</div>
  </div>
  <div style='text-align:center;min-width:50px'>
    <div style='background:#ee4d2d;color:#fff;border-radius:20px;padding:3px 8px;font-size:11px;font-weight:700'>{r["score"] or 0:.0f}pts</div>
    {"<div style='font-size:10px;color:#27ae60;margin-top:3px'>🔁 "+str(r["posts"])+"x</div>" if (r["posts"] or 0)>=2 else ""}
  </div>
</div>"""

    total  = stats["total"] if stats else 0
    avg_sc = stats["avg_score"] if stats and stats["avg_score"] else 0
    t_post = stats["total_posts"] if stats else 0
    reposts= stats["repostados"] if stats else 0

    return """<!DOCTYPE html><html><head>
<meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Top Performers</title>"""+CSS+"""</head><body>
<div class='header'><h1>🏆 Top Performers</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>
<div style='display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px'>
  <div style='background:#fff;border-radius:12px;padding:14px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06)'>
    <div style='font-size:24px;font-weight:800;color:#ee4d2d'>"""+str(total)+"""</div>
    <div style='font-size:11px;color:#888'>produtos rastreados</div>
  </div>
  <div style='background:#fff;border-radius:12px;padding:14px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06)'>
    <div style='font-size:24px;font-weight:800;color:#27ae60'>"""+f"{avg_sc:.0f}"+"""</div>
    <div style='font-size:11px;color:#888'>score médio</div>
  </div>
  <div style='background:#fff;border-radius:12px;padding:14px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06)'>
    <div style='font-size:24px;font-weight:800;color:#1565c0'>"""+str(t_post)+"""</div>
    <div style='font-size:11px;color:#888'>posts realizados</div>
  </div>
  <div style='background:#fff;border-radius:12px;padding:14px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06)'>
    <div style='font-size:24px;font-weight:800;color:#f57c00'>"""+str(reposts)+"""</div>
    <div style='font-size:11px;color:#888'>candidatos repost</div>
  </div>
  <div style='background:#fff;border-radius:12px;padding:14px;text-align:center;box-shadow:0 2px 8px rgba(0,0,0,.06);grid-column:1/-1'>
    <div style='font-size:28px;font-weight:800;color:#9c27b0'>"""+str(stats["total_clicks"] if stats and stats["total_clicks"] else 0)+"""</div>
    <div style='font-size:11px;color:#888'>total de cliques rastreados 🖱️</div>
  </div>
</div>
<div style='background:#fff;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.06)'>"""+linhas+"""
</div>
<a href='/repostar_top' style='display:block;background:#ee4d2d;color:#fff;text-align:center;padding:13px;
   border-radius:10px;font-weight:700;text-decoration:none;margin-top:14px'>
   🔁 Repostar Top 3 Agora
</a>
</div></body></html>"""

@app.route("/repostar_top")
@login_required
def repostar_top():
    """Reposta os produtos com melhor performance."""
    prods = buscar_produtos_para_repost(limite=3)
    if not prods:
        return "<script>alert('Nenhum produto com performance suficiente ainda.');history.back();</script>"
    posted = 0
    ig_token = cfg("instagram_access_token","")
    ig_uid   = cfg("instagram_user_id","")
    wa_inst  = cfg("whatsapp_instance_id","brizzah-bot")
    wa_token = cfg("whatsapp_token","Brizzah@2025!")
    wa_group = cfg("whatsapp_group_id","120363407236556172@g.us")
    for p in prods:
        try:
            prod = {"name":p["name"],"price":p["price"],"source":p.get("source","shopee"),
                    "image_url":p["image_url"],"affiliate_url":p["affiliate_url"],
                    "sold":999,"rating":4.8}
            cap_ig = formatar_mensagem(prod)
            cap_wa = _gerar_caption_wa(prod)
            img = p["image_url"] or ""
            if img.startswith("//"): img="https:"+img
            if ig_token and ig_uid and img:
                instagram_post(img, cap_ig, ig_token, ig_uid)
            if wa_inst and wa_token and wa_group and img:
                whatsapp_post(img, cap_wa, wa_inst, wa_token, wa_group)
                posted += 1
            with get_db() as c:
                c.execute("UPDATE performance SET repost_count=repost_count+1, last_posted=CURRENT_TIMESTAMP WHERE product_hash=?",
                          (gerar_hash_produto(prod),))
        except Exception as e:
            log("WARN",f"[REPOST] {str(e)[:50]}")
    return f"<script>alert('{posted} produto(s) repostado(s) com sucesso!');location.href='/top_performers';</script>"


@app.route("/r/<int:product_id>")
def redirect_produto(product_id):
    """Rastreia clique e redireciona para link de afiliado."""
    from flask import redirect as _redir
    try:
        with get_db() as c:
            p = c.execute("SELECT affiliate_url, product_url, name FROM products WHERE id=?",
                          (product_id,)).fetchone()
        if p:
            link = p["affiliate_url"] or p["product_url"] or ""
            registrar_click(product_id, canal="link_rastreado")
            log("INFO", f"[CLICK] produto_id={product_id} | {(p['name'] or '')[:40]}")
            if link:
                return _redir(link)
    except Exception as e:
        log("WARN", f"[CLICK] erro: {str(e)[:50]}")
    return "Produto não encontrado", 404


@app.route("/relatorio")
@login_required
def relatorio():
    """Painel de relatório de vendas e desempenho"""
    with get_db() as c:
        total     = c.execute("SELECT COUNT(*) as n FROM products").fetchone()["n"]
        sucesso   = c.execute("SELECT COUNT(*) as n FROM products WHERE status='success'").fetchone()["n"]
        hoje      = c.execute("SELECT COUNT(*) as n FROM products WHERE date(posted_at)=date('now')").fetchone()["n"]
        semana    = c.execute("SELECT COUNT(*) as n FROM products WHERE posted_at >= datetime('now','-7 days')").fetchone()["n"]
        por_canal = c.execute("SELECT channels, COUNT(*) as n FROM products GROUP BY channels").fetchall()
        recentes  = c.execute("SELECT * FROM products ORDER BY posted_at DESC LIMIT 20").fetchall()
        # Produtos mais caros = maior comissão potencial
        top_preco = c.execute("SELECT * FROM products WHERE status='success' ORDER BY price DESC LIMIT 5").fetchall()

    html = f"""<!DOCTYPE html>
<html lang='pt-BR'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Relatório — Brizzah Bot</title>
{CSS}
</head>
<body>
<div class='header'>
  <h1>📊 Relatório</h1>
  <a href='/dashboard'>← Voltar</a>
</div>
<div class='content'>
  <div class='card'>
    <h3>📈 Resumo Geral</h3>
    <div style='display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:8px'>
      <div style='background:#fff3f0;padding:14px;border-radius:10px;text-align:center'>
        <div style='font-size:28px;font-weight:700;color:#ee4d2d'>{total}</div>
        <div style='font-size:12px;color:#888'>Total Postados</div>
      </div>
      <div style='background:#f0fff4;padding:14px;border-radius:10px;text-align:center'>
        <div style='font-size:28px;font-weight:700;color:#27ae60'>{sucesso}</div>
        <div style='font-size:12px;color:#888'>Com Sucesso</div>
      </div>
      <div style='background:#f0f7ff;padding:14px;border-radius:10px;text-align:center'>
        <div style='font-size:28px;font-weight:700;color:#2980b9'>{hoje}</div>
        <div style='font-size:12px;color:#888'>Hoje</div>
      </div>
      <div style='background:#fefbf0;padding:14px;border-radius:10px;text-align:center'>
        <div style='font-size:28px;font-weight:700;color:#f39c12'>{semana}</div>
        <div style='font-size:12px;color:#888'>Esta Semana</div>
      </div>
    </div>
  </div>

  <div class='card'>
    <h3>🏆 Top 5 Produtos (Maior Preço)</h3>
    {"".join([f'''
    <div style='border-bottom:1px solid #f0f0f0;padding:10px 0;display:flex;align-items:center;gap:10px'>
      <img src='{p["image_url"] or ""}' style='width:50px;height:50px;border-radius:8px;object-fit:cover' onerror="this.style.display='none'">
      <div style='flex:1;min-width:0'>
        <div style='font-size:12px;font-weight:600;overflow:hidden;white-space:nowrap;text-overflow:ellipsis'>{p["name"][:45]}</div>
        <div style='font-size:13px;color:#ee4d2d;font-weight:700'>R$ {p["price"]:.2f}</div>
        <div style='font-size:11px;color:#888'>{p["channels"]}</div>
      </div>
    </div>''' for p in top_preco])}
  </div>

  <div class='card'>
    <h3>📋 Últimos 20 Posts</h3>
    {"".join([f'''
    <div style='border-bottom:1px solid #f0f0f0;padding:8px 0'>
      <div style='font-size:12px;font-weight:500;color:#333'>{p["name"][:50]}</div>
      <div style='display:flex;justify-content:space-between;margin-top:4px'>
        <span style='font-size:11px;color:#ee4d2d'>R$ {p["price"]:.2f}</span>
        <span style='font-size:11px;color:{"#27ae60" if p["status"]=="success" else "#e74c3c"}'>{p["status"]}</span>
        <span style='font-size:11px;color:#888'>{p["posted_at"][:16] if p["posted_at"] else ""}</span>
      </div>
    </div>''' for p in recentes])}
  </div>

  <div class='card'>
    <h3>🔗 Links Rápidos</h3>
    <a href='/vitrine' target='_blank' style='display:block;background:#ee4d2d;color:#fff;padding:12px;border-radius:10px;text-align:center;text-decoration:none;margin-bottom:8px'>🛍️ Ver Vitrine Pública</a>
    <a href='/logs' style='display:block;background:#333;color:#fff;padding:12px;border-radius:10px;text-align:center;text-decoration:none'>📋 Ver Logs Completos</a>
  </div>
</div>
</body></html>"""
    return html


# ════════════════════════════════════════════════════════
#  DIAGNÓSTICO E AUTOCORREÇÃO
# ════════════════════════════════════════════════════════
@app.route("/diagnostico", methods=["GET","POST"])
@login_required
def diagnostico():
    """Verifica todos os componentes do bot e corrige o que for possível"""

    resultados = []
    correcoes  = []
    action     = request.form.get("action", "")

    def check(nome, status, detalhe, corrigivel=False, correcao_key=""):
        resultados.append({
            "nome": nome, "status": status,
            "detalhe": detalhe,
            "corrigivel": corrigivel,
            "correcao_key": correcao_key
        })

    # ── AUTO-CORREÇÕES ────────────────────────────────────
    if action == "corrigir_cache":
        try:
            with get_db() as c:
                c.execute("DELETE FROM config WHERE key LIKE '_img_%'")
            correcoes.append("✅ Cache de imagens limpo com sucesso!")
        except Exception as e:
            correcoes.append(f"❌ Erro ao limpar cache: {e}")

    if action == "corrigir_logs":
        try:
            with get_db() as c:
                c.execute("DELETE FROM logs WHERE id NOT IN (SELECT id FROM logs ORDER BY id DESC LIMIT 200)")
            correcoes.append("✅ Logs antigos removidos (mantidos os 200 mais recentes)!")
        except Exception as e:
            correcoes.append(f"❌ Erro ao limpar logs: {e}")

    if action == "corrigir_duplicatas":
        try:
            with get_db() as c:
                antes = c.execute("SELECT COUNT(*) n FROM products").fetchone()["n"]
                # Histórico preservado — nunca deletar produtos postados
                pass  # c.execute removido para preservar histórico
                depois = c.execute("SELECT COUNT(*) n FROM products").fetchone()["n"]
            correcoes.append(f"✅ {antes - depois} produtos duplicados removidos do histórico!")
        except Exception as e:
            correcoes.append(f"❌ Erro ao remover duplicatas: {e}")

    if action == "corrigir_db":
        try:
            with get_db() as c:
                c.execute("VACUUM")
                c.execute("REINDEX")
            correcoes.append("✅ Banco de dados otimizado com sucesso!")
        except Exception as e:
            correcoes.append(f"❌ Erro ao otimizar banco: {e}")

    if action == "reset_agendamento":
        try:
            cfg_set("last_auto_run", "")
            correcoes.append("✅ Agendamento resetado! O próximo horário vai disparar normalmente.")
        except Exception as e:
            correcoes.append(f"❌ Erro: {e}")

    # ══════════════════════════════════════════════════════
    #  VERIFICAÇÕES
    # ══════════════════════════════════════════════════════

    # 1. Banco de dados
    try:
        with get_db() as c:
            c.execute("SELECT COUNT(*) FROM config").fetchone()
            c.execute("SELECT COUNT(*) FROM logs").fetchone()
            c.execute("SELECT COUNT(*) FROM products").fetchone()
        check("🗄️ Banco de Dados", "ok", "SQLite funcionando corretamente")
    except Exception as e:
        check("🗄️ Banco de Dados", "erro", f"Erro: {str(e)[:80]}", True, "corrigir_db")

    # 2. Credenciais Shopee
    # Prioridade: 1) Env Var do Render  2) Banco  3) Default hardcoded
    app_id = (os.environ.get("SHOPEE_APP_ID","") or cfg("shopee_app_id") or "18345690956").strip()
    secret = (os.environ.get("SHOPEE_SECRET","") or cfg("shopee_secret") or "CSWN4EHO64ARF4LWQRKMSP22QFFMHQZH").strip()
    # Corrige banco se tiver valor antigo
    if secret == "CSWN4EHO64ARF4LWQRKMSP22QFFMHQZH":
        try:
            db_secret = ""
            with get_db() as _c:
                _r = _c.execute("SELECT value FROM config WHERE key='shopee_secret'").fetchone()
                db_secret = _r["value"] if _r else ""
            if db_secret and db_secret != "CSWN4EHO64ARF4LWQRKMSP22QFFMHQZH":
                cfg_set("shopee_secret", "CSWN4EHO64ARF4LWQRKMSP22QFFMHQZH")
                cfg_set("shopee_app_id", "18345690956")
        except: pass
    if app_id and secret:
        check("🔑 Shopee App ID + Secret", "ok", f"App ID: {app_id[:8]}...  Secret: {secret[:6]}...")
    else:
        check("🔑 Shopee Credenciais", "erro",
              "App ID ou Secret Key não configurados! Acesse Configurações → API Shopee.")

    # 3. Teste real da API Shopee
    if app_id and secret:
        try:
            import hashlib as _hl
            ts  = str(int(time.time()))
            q   = json.dumps({"query":'{productOfferV2(keyword:"kit",listType:1,sortType:2,page:1,limit:1){nodes{productName}}}'}, separators=(",",":"))
            sig = _hl.sha256(f"{app_id}{ts}{q}{secret}".encode()).hexdigest()
            hdrs = {"Authorization": f"SHA256 Credential={app_id}, Timestamp={ts}, Signature={sig}",
                    "Content-Type": "application/json"}
            resp = requests.post("https://open-api.affiliate.shopee.com.br/graphql",
                                 headers=hdrs, data=q, timeout=10)
            nodes = resp.json().get("data",{}).get("productOfferV2",{}).get("nodes",[])
            if nodes:
                check("🛍️ API Shopee (conexão real)", "ok",
                      f"Conectada! Produto teste: {nodes[0].get('productName','')[:40]}")
            else:
                check("🛍️ API Shopee (conexão real)", "warn",
                      f"Conectou mas retornou vazio. Status: {resp.status_code}")
        except Exception as e:
            check("🛍️ API Shopee (conexão real)", "erro", f"Falha: {str(e)[:80]}")
    else:
        check("🛍️ API Shopee (conexão real)", "erro", "Sem credenciais para testar")

    # 4. Instagram Token
    ig_token = cfg("instagram_access_token","")
    ig_uid   = cfg("instagram_user_id","")
    if not ig_token:
        check("📸 Instagram Token", "erro",
              "Não configurado! Acesse Configurar Instagram API.")
    elif not ig_token.startswith("EAA"):
        check("📸 Instagram Token", "warn",
              "Token parece inválido (deve começar com EAA...). Gere um novo.")
    else:
        try:
            r = requests.get(
                "https://graph.facebook.com/v18.0/me",
                params={"access_token": ig_token, "fields": "name,id"},
                timeout=8
            )
            d = r.json()
            if "error" in d:
                err_msg = d["error"].get("message","")
                if "expired" in err_msg.lower() or "session" in err_msg.lower():
                    check("📸 Instagram Token", "erro",
                          "TOKEN EXPIRADO! Gere um novo em developers.facebook.com/tools/explorer")
                else:
                    check("📸 Instagram Token", "erro", f"Token inválido: {err_msg[:80]}")
            else:
                check("📸 Instagram Token", "ok",
                      f"Válido! Conta: {d.get('name','?')} (ID: {d.get('id','?')})")
        except Exception as e:
            check("📸 Instagram Token", "warn", f"Não foi possível testar: {str(e)[:60]}")

    check("👤 Instagram User ID", "ok" if ig_uid else "erro",
          f"Configurado: {ig_uid}" if ig_uid else "Não configurado! Configure em Configurar Instagram API.")

    # 4b. Verificar cota diária do Instagram
    if ig_token and ig_uid:
        try:
            usado, limite, pode = _ig_checar_limite_diario(ig_token, ig_uid)
            restantes = limite - usado
            if not pode:
                check("📊 Cota Instagram (24h)", "erro",
                      f"LIMITE ATINGIDO: {usado}/{limite} posts usados. Aguarde até amanhã para postar.")
            elif restantes <= 5:
                check("📊 Cota Instagram (24h)", "warn",
                      f"Atenção: {usado}/{limite} posts usados — restam apenas {restantes}.")
            else:
                check("📊 Cota Instagram (24h)", "ok",
                      f"{usado}/{limite} posts usados hoje — restam {restantes} disponíveis.")
        except Exception:
            pass

    # 5. Telegram
    tg_token = cfg("telegram_bot_token","") or cfg("telegram_token","")
    tg_chat  = cfg("telegram_chat_id","")
    if tg_token and tg_chat:
        try:
            r = requests.get(f"https://api.telegram.org/bot{tg_token}/getMe", timeout=6)
            d = r.json()
            if d.get("ok"):
                check("🤖 Telegram Bot", "ok", f"Ativo: @{d['result'].get('username','?')}")
            else:
                check("🤖 Telegram Bot", "erro", f"Token inválido: {d.get('description','')[:60]}")
        except Exception as e:
            check("🤖 Telegram Bot", "warn", f"Não testado: {str(e)[:60]}")
    else:
        check("🤖 Telegram Bot", "warn", "Não configurado (opcional)")

    # 6. Agendamento
    auto_on  = cfg("auto_enabled","false") == "true"
    schedule = cfg("auto_schedule","")
    last_run = cfg("last_auto_run","")
    if auto_on and schedule:
        check("⏰ Agendamento", "ok",
              f"ATIVO — Horários: {schedule} | Último: {last_run or 'nunca'}")
    elif auto_on:
        check("⏰ Agendamento", "warn", "Ativo mas sem horários! Configure em Configurar Horários.")
    else:
        check("⏰ Agendamento", "warn", "Desativado. Ative em Configurar Horários.")

    # 7. Cache de imagens
    try:
        with get_db() as c:
            n_imgs = c.execute("SELECT COUNT(*) n FROM config WHERE key LIKE '_img_%'").fetchone()["n"]
        if n_imgs > 100:
            check("🖼️ Cache de Imagens", "warn",
                  f"{n_imgs} imagens em cache (pode deixar lento). Recomenda limpar.",
                  True, "corrigir_cache")
        else:
            check("🖼️ Cache de Imagens", "ok", f"{n_imgs} imagens em cache")
    except Exception as e:
        check("🖼️ Cache de Imagens", "warn", f"Não verificado: {e}")

    # 8. Histórico de produtos
    try:
        with get_db() as c:
            total   = c.execute("SELECT COUNT(*) n FROM products").fetchone()["n"]
            sucesso = c.execute("SELECT COUNT(*) n FROM products WHERE status='success'").fetchone()["n"]
            falha   = c.execute("SELECT COUNT(*) n FROM products WHERE status='error'").fetchone()["n"]
            duplas  = c.execute("SELECT COUNT(*) n FROM products WHERE name IN (SELECT name FROM products GROUP BY name HAVING COUNT(*)>1)").fetchone()["n"]
        if duplas > 10:
            check("📦 Histórico de Produtos", "warn",
                  f"{total} registros | {sucesso} ok | {falha} falhas | {duplas} duplicados",
                  True, "corrigir_duplicatas")
        else:
            check("📦 Histórico de Produtos", "ok",
                  f"{total} registros | {sucesso} sucessos | {falha} falhas")
    except Exception as e:
        check("📦 Histórico de Produtos", "warn", f"Não verificado: {e}")

    # 9. Logs
    try:
        with get_db() as c:
            n_logs  = c.execute("SELECT COUNT(*) n FROM logs").fetchone()["n"]
            n_erros = c.execute("SELECT COUNT(*) n FROM logs WHERE level='ERROR' AND id > (SELECT MAX(id)-50 FROM logs)").fetchone()["n"]
        if n_erros >= 5:
            check("📋 Logs", "warn",
                  f"{n_logs} registros | ⚠️ {n_erros} erros recentes!", True, "corrigir_logs")
        elif n_logs > 1000:
            check("📋 Logs", "warn",
                  f"{n_logs} registros (banco crescendo). Recomenda limpar.", True, "corrigir_logs")
        else:
            check("📋 Logs", "ok", f"{n_logs} registros | {n_erros} erros recentes")
    except Exception as e:
        check("📋 Logs", "warn", f"Não verificado: {e}")

    # 10. Pillow
    try:
        from PIL import Image
        check("🎨 Gerador de Imagens (Pillow)", "ok", "Instalado e funcionando")
    except ImportError:
        check("🎨 Gerador de Imagens (Pillow)", "erro",
              "Não instalado! Adicione 'Pillow' ao requirements.txt")

    # 11. Palavra-chave do nicho
    keyword = cfg("niche_keyword","")
    check("🏷️ Palavra-chave do Nicho",
          "ok" if keyword else "warn",
          f"Configurada: '{keyword}'" if keyword else "Não configurada. Defina em Configurações.")

    # 12. URL do bot
    bot_url = cfg("bot_url","") or os.environ.get("BOT_URL","")
    check("🌐 URL do Bot",
          "ok" if bot_url else "warn",
          f"Configurada: {bot_url}" if bot_url else "Não configurada. Imagens podem não aparecer nos posts.")

    # ── Monta HTML ────────────────────────────────────────
    total_ok   = sum(1 for r in resultados if r["status"]=="ok")
    total_warn = sum(1 for r in resultados if r["status"]=="warn")
    total_erro = sum(1 for r in resultados if r["status"]=="erro")
    saude      = int((total_ok / len(resultados)) * 100) if resultados else 0
    cor_saude  = "#27ae60" if saude>=80 else "#f39c12" if saude>=50 else "#e74c3c"
    emoji_saude= "🟢" if saude>=80 else "🟡" if saude>=50 else "🔴"

    rows_html = ""
    for r in resultados:
        icone = "✅" if r["status"]=="ok" else "⚠️" if r["status"]=="warn" else "❌"
        cor   = "#27ae60" if r["status"]=="ok" else "#f39c12" if r["status"]=="warn" else "#e74c3c"
        btn   = ""
        if r["corrigivel"] and r["correcao_key"]:
            btn = f"""<form method='post' style='display:inline;margin-left:10px'>
                        <input type='hidden' name='action' value='{r["correcao_key"]}'>
                        <button type='submit' style='background:#ee4d2d;color:#fff;border:none;
                          padding:5px 14px;border-radius:20px;font-size:12px;cursor:pointer'>
                          🔧 Corrigir
                        </button>
                      </form>"""
        rows_html += f"""
        <div style='display:flex;align-items:center;justify-content:space-between;
                    padding:14px 16px;border-bottom:1px solid #f0f0f0;flex-wrap:wrap;gap:8px'>
          <div style='flex:1;min-width:200px'>
            <div style='font-size:14px;font-weight:600;color:#333'>{icone} {r["nome"]}</div>
            <div style='font-size:12px;color:#666;margin-top:3px'>{r["detalhe"]}</div>
          </div>
          <div style='display:flex;align-items:center'>
            <span style='color:{cor};font-size:12px;font-weight:700;
                         background:{cor}22;padding:4px 12px;border-radius:20px'>
              {"OK" if r["status"]=="ok" else "ATENÇÃO" if r["status"]=="warn" else "ERRO"}
            </span>{btn}
          </div>
        </div>"""

    corr_html = ""
    for c2 in correcoes:
        cor2 = "#27ae60" if c2.startswith("✅") else "#e74c3c"
        corr_html += f"<div style='padding:10px 14px;background:{cor2}18;border-left:4px solid {cor2};margin-bottom:8px;border-radius:6px;font-size:13px;color:{cor2};font-weight:600'>{c2}</div>"

    guia_erros = ""
    if total_erro > 0:
        itens = []
        if any("Token" in r["nome"] and r["status"]=="erro" for r in resultados):
            itens.append("<b>📸 Token Expirado:</b> developers.facebook.com/tools/explorer → gere novo EAA... → salve em Configurar Instagram API")
        if any("Shopee" in r["nome"] and r["status"]=="erro" for r in resultados):
            itens.append("<b>🔑 Shopee API:</b> Acesse Configurações → preencha seu App ID e Secret Key do portal affiliate.shopee.com.br")
        if any("Pillow" in r["nome"] and r["status"]=="erro" for r in resultados):
            itens.append("<b>🎨 Pillow:</b> Adicione 'Pillow' no requirements.txt e faça commit")
        if any("User ID" in r["nome"] and r["status"]=="erro" for r in resultados):
            itens.append("<b>👤 User ID:</b> Acesse Configurar Instagram API e preencha o campo")
        if itens:
            lista = "".join(f"<li style='margin-bottom:10px'>{i}</li>" for i in itens)
            guia_erros = f"""
            <div class='card' style='background:#fff8f0;border-left:4px solid #e74c3c'>
              <h3>🚨 Como Corrigir os Erros</h3>
              <ol style='font-size:13px;line-height:1.8;padding-left:18px'>{lista}</ol>
            </div>"""

    html = f"""<!DOCTYPE html>
<html lang='pt-BR'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Diagnóstico — Brizzah Bot</title>
{CSS}
<style>
.meter-bar{{height:20px;border-radius:10px;background:#eee;overflow:hidden;margin:10px 0}}
.meter-fill{{height:100%;border-radius:10px;background:{cor_saude};width:{saude}%}}
</style>
</head>
<body>
<div class='header'><h1>🔧 Diagnóstico do Bot</h1><a href='/dashboard'>← Voltar</a></div>
<div class='content'>

  {"<div class='card' style='background:#f0fff4;border-left:4px solid #27ae60'>" + corr_html + "</div>" if correcoes else ""}

  <div class='card' style='text-align:center'>
    <h3>Saúde Geral do Bot</h3>
    <div style='font-size:60px;margin:8px 0'>{emoji_saude}</div>
    <div style='font-size:52px;font-weight:700;color:{cor_saude}'>{saude}%</div>
    <div class='meter-bar'><div class='meter-fill'></div></div>
    <div style='display:flex;justify-content:center;gap:24px;margin-top:10px;font-size:13px'>
      <span style='color:#27ae60'>✅ {total_ok} OK</span>
      <span style='color:#f39c12'>⚠️ {total_warn} Atenção</span>
      <span style='color:#e74c3c'>❌ {total_erro} Erro</span>
    </div>
  </div>

  <div class='card' style='padding:0'>
    <div style='padding:16px;border-bottom:1px solid #f0f0f0'><h3 style='margin:0'>📋 Resultado Detalhado</h3></div>
    {rows_html}
  </div>

  {guia_erros}

  <div class='card'>
    <h3>⚡ Ações de Manutenção</h3>
    <div style='display:flex;flex-direction:column;gap:10px'>
      <form method='post'><input type='hidden' name='action' value='corrigir_cache'>
        <button type='submit' style='width:100%;background:#3498db;color:#fff;border:none;padding:12px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer'>🖼️ Limpar Cache de Imagens</button></form>
      <form method='post'><input type='hidden' name='action' value='corrigir_logs'>
        <button type='submit' style='width:100%;background:#9b59b6;color:#fff;border:none;padding:12px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer'>📋 Limpar Logs Antigos</button></form>
      <form method='post'><input type='hidden' name='action' value='corrigir_duplicatas'>
        <button type='submit' style='width:100%;background:#e67e22;color:#fff;border:none;padding:12px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer'>📦 Remover Produtos Duplicados</button></form>
      <form method='post'><input type='hidden' name='action' value='corrigir_db'>
        <button type='submit' style='width:100%;background:#27ae60;color:#fff;border:none;padding:12px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer'>🗄️ Otimizar Banco de Dados</button></form>
      <form method='post'><input type='hidden' name='action' value='reset_agendamento'>
        <button type='submit' style='width:100%;background:#e74c3c;color:#fff;border:none;padding:12px;border-radius:10px;font-size:14px;font-weight:600;cursor:pointer'>⏰ Resetar Agendamento (se travado)</button></form>
    </div>
  </div>

  <a href='/diagnostico' style='display:block;background:#ee4d2d;color:#fff;padding:14px;border-radius:10px;text-align:center;text-decoration:none;font-size:15px;font-weight:700;margin-bottom:10px'>🔄 Rodar Diagnóstico Novamente</a>
  <a href='/dashboard' style='display:block;background:#333;color:#fff;padding:14px;border-radius:10px;text-align:center;text-decoration:none;font-size:15px;font-weight:700'>← Voltar ao Painel</a>
</div></body></html>"""
    return html


@app.route("/privacy")
def privacy():
    html = (
        "<!DOCTYPE html><html><head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>Politica de Privacidade - ShopeeBot</title>"
        "<style>body{font-family:sans-serif;max-width:700px;margin:0 auto;"
        "padding:24px;color:#333}h1{color:#ee4d2d}h2{color:#555;margin-top:24px}"
        "p{line-height:1.7;margin:8px 0}</style></head><body>"
        "<h1>Politica de Privacidade - ShopeeBot</h1>"
        "<p><b>Ultima atualizacao:</b> Marco de 2026</p>"
        "<p>Este aplicativo ShopeeBot e uma ferramenta de automacao para afiliados Shopee.</p>"
        "<h2>1. Dados Coletados</h2>"
        "<p>Coletamos apenas token de acesso do Instagram, ID do usuario e configuracoes do afiliado.</p>"
        "<h2>2. Uso dos Dados</h2>"
        "<p>Os dados sao usados exclusivamente para publicar posts no Instagram.</p>"
        "<h2>3. Permissoes</h2>"
        "<p>Usamos: instagram_basic, instagram_content_publish e pages_read_engagement.</p>"
        "<h2>4. Contato</h2>"
        "<p>Instagram: @brizzah.br</p>"
        "</body></html>"
    )
    return html

@app.route("/env_status")
@login_required
def env_status():
    """Página que mostra quais variáveis estão salvas no Render"""
    variaveis = [
        ("SHOPEE_APP_ID",       "🔑 Shopee App ID",           "shopee_app_id"),
        ("SHOPEE_SECRET",       "🔐 Shopee Secret Key",        "shopee_secret"),
        ("SHOPEE_AFFILIATE_ID", "🏷️ Shopee Affiliate ID",     "shopee_affiliate_id"),
        ("INSTAGRAM_TOKEN",     "📸 Instagram Token",          "instagram_access_token"),
        ("INSTAGRAM_USER_ID",   "👤 Instagram User ID",        "instagram_user_id"),
        ("BOT_PASSWORD",        "🔒 Senha do Bot",             "bot_password"),
        ("TELEGRAM_TOKEN",      "🤖 Telegram Token",           "telegram_token"),
        ("TELEGRAM_CHAT_ID",    "💬 Telegram Chat ID",         "telegram_chat_id"),
        ("BOT_URL",             "🌐 URL do Bot",               "bot_url"),
        ("NICHE_KEYWORD",       "🏷️ Palavra-chave do Nicho",   "niche_keyword"),
    ]

    rows = ""
    for env_key, label, db_key in variaveis:
        env_val  = os.environ.get(env_key, "")
        db_val   = ""
        with get_db() as c:
            r = c.execute("SELECT value FROM config WHERE key=?", (db_key,)).fetchone()
            db_val = r["value"] if r else ""

        if env_val:
            status   = "✅ Salvo no Render"
            cor      = "#27ae60"
            mostra   = ("*" * 6 + env_val[-4:]) if len(env_val) > 4 else "****"
            fonte    = "Variável de Ambiente"
        elif db_val:
            status   = "⚠️ Só no Banco (some após reinício)"
            cor      = "#f39c12"
            mostra   = ("*" * 6 + db_val[-4:]) if len(db_val) > 4 else "****"
            fonte    = "Banco SQLite"
        else:
            status   = "❌ Não configurado"
            cor      = "#e74c3c"
            mostra   = "—"
            fonte    = "—"

        rows += f"""
        <tr>
          <td style='padding:10px;font-size:13px;font-weight:600'>{label}</td>
          <td style='padding:10px;font-size:12px;color:#888;font-family:monospace'>{env_key}</td>
          <td style='padding:10px;font-size:12px;font-family:monospace;color:#555'>{mostra}</td>
          <td style='padding:10px;font-size:12px;color:#888'>{fonte}</td>
          <td style='padding:10px'><span style='color:{cor};font-size:12px;font-weight:600'>{status}</span></td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang='pt-BR'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Status das Variáveis — Brizzah Bot</title>
{CSS}
</head>
<body>
<div class='header'>
  <h1>🔐 Configurações Permanentes</h1>
  <a href='/dashboard'>← Voltar</a>
</div>
<div class='content'>

  <div class='card' style='background:#f0fff4;border-left:4px solid #27ae60'>
    <h3>✅ Como funciona</h3>
    <p style='font-size:13px;line-height:1.7'>
      <b>Variável de Ambiente</b> = salva no Render, <u>nunca some</u>, mesmo após reinício ou deploy.<br>
      <b>Banco SQLite</b> = salva localmente, <u>some quando o servidor reinicia</u>.<br><br>
      Tudo marcado como ✅ está 100% seguro e permanente.
    </p>
  </div>

  <div class='card'>
    <h3>📋 Status de Cada Configuração</h3>
    <div style='overflow-x:auto'>
    <table style='width:100%;border-collapse:collapse;font-size:13px'>
      <thead>
        <tr style='background:#f5f5f5;font-size:12px'>
          <th style='padding:10px;text-align:left'>Configuração</th>
          <th style='padding:10px;text-align:left'>Variável</th>
          <th style='padding:10px;text-align:left'>Valor</th>
          <th style='padding:10px;text-align:left'>Fonte</th>
          <th style='padding:10px;text-align:left'>Status</th>
        </tr>
      </thead>
      <tbody>{rows}</tbody>
    </table>
    </div>
  </div>

  <div class='card' style='background:#fff8f0;border-left:4px solid #ee4d2d'>
    <h3>⚠️ Itens marcados como "Só no Banco"?</h3>
    <p style='font-size:13px;line-height:1.7'>
      Siga o passo a passo abaixo para salvar permanentemente no Render:
    </p>
    <ol style='font-size:13px;line-height:2;padding-left:20px'>
      <li>Acesse <b>dashboard.render.com</b></li>
      <li>Clique no serviço <b>shopee-bot-jt11</b></li>
      <li>Clique em <b>Environment</b> no menu lateral</li>
      <li>Clique em <b>"Add Environment Variable"</b></li>
      <li>Adicione o <b>Key</b> e o <b>Value</b> de cada item ⚠️</li>
      <li>Clique em <b>"Save Changes"</b></li>
      <li>Aguarde o redeploy automático</li>
      <li>Volte aqui — tudo deve aparecer ✅</li>
    </ol>
  </div>

  <div class='card'>
    <a href='https://dashboard.render.com' target='_blank'
       style='display:block;background:#ee4d2d;color:#fff;padding:14px;border-radius:10px;
              text-align:center;text-decoration:none;font-size:15px;font-weight:700;margin-bottom:10px'>
      🚀 Abrir Render Dashboard
    </a>
    <a href='/dashboard'
       style='display:block;background:#333;color:#fff;padding:14px;border-radius:10px;
              text-align:center;text-decoration:none;font-size:15px;font-weight:700'>
      ← Voltar ao Painel
    </a>
  </div>

</div>
</body></html>"""
    return html




@app.route("/top100", methods=["GET", "POST"])
@login_required
def top100():
    import json as _j
    msg = ""; msg_tipo = "info"; produtos_preview = []
    acao = request.form.get("acao","")
    if request.method == "POST":
        nicho = request.form.get("nicho","geral")
        ordem = int(request.form.get("ordem","2") or "2")
        produtos_todos = shopee_api_top100(nicho=nicho, sort_type=ordem)
        if not produtos_todos:
            msg = "❌ Sem produtos. Verifique credenciais Shopee nos Logs."; msg_tipo="erro"
        elif acao == "preview":
            produtos_preview = produtos_todos[:20]
            msg = f"✅ {len(produtos_todos)} produtos carregados — selecione e clique em Postar."; msg_tipo="ok"
        elif acao == "postar_selecionados":
            idxs = [int(x) for x in request.form.getlist("sel_idx") if x.isdigit()]
            sel = [produtos_todos[i] for i in idxs if i < len(produtos_todos)]
            if not sel: msg = "⚠️ Nenhum selecionado."; msg_tipo="warn"
            else:
                tok=cfg("instagram_access_token",""); uid=cfg("instagram_user_id","")
                if not tok or not uid: msg="❌ Instagram não configurado."; msg_tipo="erro"
                else:
                    def _run(prods,t,u):
                        import time as _t
                        ct=0
                        for p in prods:
                            img=(p.get("image_url") or "").strip()
                            if img.startswith("//"): img="https:"+img
                            elif img and not img.startswith("http"): img="https://cf.shopee.com.br/file/"+img
                            if not img: continue
                            cap=formatar_mensagem(p)
                            img_p=preparar_imagem_produto(img,p) or img
                            ok2,res=instagram_post(img_p,cap,t,u)
                            if ok2: ct+=1; salvar_produto(p,"success","instagram",cap)
                            else: log("WARN",f"[t100] ❌ {str(res)[:50]}")
                            _t.sleep(35)
                        log("INFO",f"[t100] {ct}/{len(prods)} postados")
                    threading.Thread(target=_run,args=(sel,tok,uid),daemon=True).start()
                    msg=f"🚀 {len(sel)} produto(s) sendo postados em background!"; msg_tipo="ok"
                    produtos_preview = produtos_todos[:20]
        elif acao == "postar_todos":
            tok=cfg("instagram_access_token",""); uid=cfg("instagram_user_id","")
            if not tok or not uid: msg="❌ Instagram não configurado."; msg_tipo="erro"
            else:
                def _run_all(prods,t,u):
                    import time as _t; ct=0
                    for p in prods:
                        img=(p.get("image_url") or "").strip()
                        if img.startswith("//"): img="https:"+img
                        elif img and not img.startswith("http"): img="https://cf.shopee.com.br/file/"+img
                        if not img: continue
                        cap=formatar_mensagem(p)
                        img_p=preparar_imagem_produto(img,p) or img
                        ok2,res=instagram_post(img_p,cap,t,u)
                        if ok2: ct+=1; salvar_produto(p,"success","instagram",cap)
                        _t.sleep(35)
                    log("INFO",f"[t100-all] {ct}/{len(prods)} postados")
                threading.Thread(target=_run_all,args=(produtos_todos,tok,uid),daemon=True).start()
                msg=f"🚀 Postando {len(produtos_todos)} produtos!"; msg_tipo="ok"
        elif acao == "salvar_fila":
            ct=0
            with get_db() as c:
                for p in produtos_todos:
                    try:
                        c.execute("INSERT OR IGNORE INTO products (name,price,commission,rating,sold,image_url,product_url,affiliate_url,shop_id,item_id,shop_name,status,channels,posted_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,'queued','',datetime('now'))",
                            (p.get("name",""),p.get("price",0),p.get("commission",0),p.get("rating",0),p.get("sold",0),p.get("image_url",""),p.get("product_url",""),p.get("affiliate_url",""),p.get("shop_id",""),p.get("item_id",""),p.get("shop_name",""))); ct+=1
                    except: pass
            msg=f"✅ {ct} produtos na fila!"; msg_tipo="ok"

    nichos=[("geral","🌐 Geral"),("beleza","💄 Beleza"),("casa","🏠 Casa"),("moda","👗 Moda"),("eletro","📱 Eletrônicos"),("fitness","💪 Fitness"),("pet","🐾 Pet"),("infantil","👶 Infantil")]
    nopts="".join(f"<option value='{v}'>{l}</option>" for v,l in nichos)
    mc={"ok":"background:#e8f5e9;border-left:4px solid #27ae60;color:#1b5e20","erro":"background:#ffebee;border-left:4px solid #e53935;color:#b71c1c","warn":"background:#fff8e1;border-left:4px solid #f9a825;color:#5d4037"}.get(msg_tipo,"background:#e3f2fd;border-left:4px solid #1976d2;color:#0d47a1")
    mh=f"<div style='{mc};padding:12px;border-radius:10px;margin-bottom:16px;font-weight:600'>{msg}</div>" if msg else ""
    cs="""<style>
.pc{background:#fff;border-radius:12px;padding:10px;display:flex;align-items:flex-start;gap:8px;cursor:pointer;border:2px solid transparent;transition:all .15s;box-shadow:0 2px 8px rgba(0,0,0,.07);position:relative;user-select:none}
.pc:has(input:checked){border-color:#ee4d2d;background:#fff8f6;box-shadow:0 0 0 3px #ee4d2d33}
.pc input{display:none}
.pc img{width:80px;height:80px;object-fit:contain;border-radius:8px;border:1px solid #eee;flex-shrink:0}
.pk{position:absolute;top:8px;right:8px;width:22px;height:22px;border-radius:50%;border:2px solid #ddd;background:#fff;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:transparent;transition:all .15s}
.pc:has(input:checked) .pk{background:#ee4d2d;border-color:#ee4d2d;color:#fff}
.pc:has(input:checked) .pk::after{content:'✓'}
.pi{width:68px;height:68px;object-fit:cover;border-radius:8px;flex-shrink:0}
.pn{font-size:12px;font-weight:600;line-height:1.4;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
</style>"""
    cards=""; grid=""
    for i,p in enumerate(produtos_preview):
        nm=(p.get("name") or "")[:55].replace("'","&#39;").replace('"',"&quot;")
        img=(p.get("image_url") or "").strip()
        if img.startswith("//"): img="https:"+img
        pr=float(p.get("price",0) or 0); sl=int(p.get("sold",0) or 0); rt=float(p.get("rating",0) or 0)
        st="⭐"*min(int(round(rt)),5) if rt>=1 else ""
        cards+=f"""<label class='pc' for='i{i}'><input type='checkbox' name='sel_idx' value='{i}' id='i{i}' onchange='upd()'><div class='pk'></div><img src='{img}' class='pi' onerror="this.src='https://placehold.co/68x68/eee/999?text=?'" loading='lazy'><div style='flex:1;min-width:0'><div class='pn'>{nm}</div><div style='display:flex;gap:5px;margin-top:3px;flex-wrap:wrap'><span style='color:#ee4d2d;font-weight:700;font-size:13px'>R$ {pr:.2f}</span>{"<span style='font-size:11px;color:#888'>🛒"+str(sl)+"</span>" if sl else ""}{"<span style='font-size:11px'>"+st+"</span>" if st else ""}</div></div></label>"""
    if produtos_preview:
        grid=f"""<div class='card' style='margin-top:16px'>
<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:12px'>
  <h3 style='margin:0;font-size:15px'>👀 {len(produtos_preview)} produtos — clique para selecionar</h3>
  <div style='display:flex;gap:8px'>
    <button type='button' onclick='sel(true)' style='padding:6px 12px;background:#ee4d2d;color:#fff;border:none;border-radius:8px;font-size:12px;font-weight:700;cursor:pointer'>☑ Todos</button>
    <button type='button' onclick='sel(false)' style='padding:6px 12px;background:#666;color:#fff;border:none;border-radius:8px;font-size:12px;cursor:pointer'>✖ Limpar</button>
  </div>
</div>
<div style='display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:10px;padding-bottom:80px'>{cards}</div>
</div>
<div id='bar' style='display:none;position:fixed;bottom:0;left:0;right:0;background:#ee4d2d;color:#fff;padding:12px 16px;z-index:9999;box-shadow:0 -4px 16px rgba(0,0,0,.25);align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px'>
  <span style='font-weight:700;font-size:15px'><span id='cnt'>0</span> produto(s) selecionado(s)</span>
  <div style='display:flex;gap:8px'>
    <button type='button' onclick='sel(true)' style='padding:8px 14px;background:rgba(255,255,255,.25);color:#fff;border:1px solid rgba(255,255,255,.5);border-radius:8px;font-size:13px;cursor:pointer'>☑ Todos</button>
    <button type='button' onclick='sel(false)' style='padding:8px 14px;background:rgba(255,255,255,.15);color:#fff;border:1px solid rgba(255,255,255,.4);border-radius:8px;font-size:13px;cursor:pointer'>✖ Limpar</button>
    <button name='acao' value='postar_selecionados' onclick='return conf()' style='padding:8px 18px;background:#fff;color:#ee4d2d;border:none;border-radius:8px;font-size:14px;font-weight:700;cursor:pointer'>🚀 Postar Selecionados</button>
  </div>
</div>"""
    return f"""<!DOCTYPE html><html lang='pt-BR'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Top 100 — Brizzah</title>{CSS}{cs}</head><body>
<div class='header'><h1>🏆 Top 100 Mais Vendidos</h1><a href='/dashboard'>← Voltar</a></div>
<div class='content'>{mh}
<form method='POST' id='frm'>
<div class='card'><h3 style='margin-bottom:12px'>⚙️ Configurar Busca</h3>
<label style='font-size:13px;font-weight:600;display:block;margin-bottom:4px'>Nicho</label>
<select name='nicho' style='width:100%;padding:10px;border-radius:8px;border:1.5px solid #ddd;font-size:14px;margin-bottom:12px;background:#fff'>{nopts}</select>
<label style='font-size:13px;font-weight:600;display:block;margin-bottom:4px'>Ordenar por</label>
<select name='ordem' style='width:100%;padding:10px;border-radius:8px;border:1.5px solid #ddd;font-size:14px;margin-bottom:16px;background:#fff'>
<option value='2'>🔥 Mais Vendidos</option><option value='3'>⭐ Relevância</option></select>
<button name='acao' value='preview' style='width:100%;padding:12px;background:#1976d2;color:#fff;border:none;border-radius:10px;font-size:15px;font-weight:700;margin-bottom:8px;cursor:pointer'>🔍 Buscar e Selecionar Produtos</button>
<button name='acao' value='postar_todos' onclick="return confirm('Postar TODOS os 100 produtos agora?')" style='width:100%;padding:12px;background:#ee4d2d;color:#fff;border:none;border-radius:10px;font-size:15px;font-weight:700;margin-bottom:8px;cursor:pointer'>🚀 Buscar e Postar Todos (100)</button>
<button name='acao' value='salvar_fila' style='width:100%;padding:12px;background:#27ae60;color:#fff;border:none;border-radius:10px;font-size:15px;font-weight:700;cursor:pointer'>📋 Salvar na Fila (postar gradualmente)</button>
</div>
{grid}
</form>
</div>
<script>
function upd(){{var n=document.querySelectorAll('input[name=sel_idx]:checked').length;var b=document.getElementById('bar');var c=document.getElementById('cnt');if(b)b.style.display=n>0?'flex':'none';if(c)c.textContent=n;}}
function sel(v){{document.querySelectorAll('input[name=sel_idx]').forEach(function(x){{x.checked=v;}});upd();}}
function conf(){{var n=document.querySelectorAll('input[name=sel_idx]:checked').length;if(n===0){{alert('Selecione pelo menos 1 produto!');return false;}}return confirm('Confirmar postagem de '+n+' produto(s)?');}}
</script></body></html>"""








# ════════════════════════════════════════════════════════
#  TESTE RÁPIDO — diagnóstico completo de postagem
# ════════════════════════════════════════════════════════
@app.route("/teste_post", methods=["GET","POST"])
@login_required
def teste_post():
    """Testa cada etapa da postagem e mostra exatamente onde falha."""
    passos = []
    erro_fatal = False

    def ok(msg):  passos.append(("ok",  msg))
    def err(msg): passos.append(("err", msg))
    def warn(msg):passos.append(("warn",msg))

    # 1. Contexto temporal
    try:
        ctx = _detectar_contexto_temporal()
        ok(f"Contexto: {ctx['periodo']} | {ctx['tipo_dia']} | data={ctx.get('data_comemorativa') or 'nenhuma'}")
    except Exception as e:
        err(f"Contexto temporal: {e}"); erro_fatal = True

    # 2. Nicho
    try:
        kw_config = cfg("niche_keyword","").strip()
        fixar     = cfg("fixar_nicho_keyword","false") == "true"
        if kw_config and fixar:
            keyword, de = kw_config, None
            ok(f"Nicho FIXO: '{keyword}'")
        else:
            keyword, de = _nicho_inteligente_contextual(ctx)
            ok(f"Nicho inteligente: '{keyword}' | data_especial={de or 'nenhuma'}")
    except Exception as e:
        err(f"Seleção de nicho: {e}"); keyword = "kit presente"; erro_fatal = True

    # 3. Busca de produto
    produto = None
    try:
        prods = shopee_api_buscar_produtos(keyword, limit=3)
        if prods:
            produto = prods[0]
            ok(f"Produto encontrado: '{produto['name'][:50]}' | R$ {produto.get('price',0):.2f}")
        else:
            warn(f"API Shopee sem resultado para '{keyword}' — tentando fallback...")
            prods2 = shopee_search(keyword, limit=3)
            if prods2:
                produto = prods2[0]
                ok(f"Produto (fallback): '{produto['name'][:50]}'")
            else:
                err(f"Nenhum produto encontrado para '{keyword}'"); erro_fatal = True
    except Exception as e:
        err(f"Busca Shopee: {e}"); erro_fatal = True

    # 4. Imagem
    img_url = ""
    if produto:
        try:
            raw = produto.get("image_url","")
            if raw.startswith("//"): raw = "https:" + raw
            if raw and not raw.startswith("http"): raw = "https://cf.shopee.com.br/file/" + raw
            img_url = raw
            if img_url:
                r = requests.head(img_url, timeout=8)
                ok(f"Imagem acessível: {img_url[:70]} | status={r.status_code}")
            else:
                err("Produto sem URL de imagem")
        except Exception as e:
            warn(f"Verificação de imagem: {e} (URL: {img_url[:60]})")

    # 5. Caption
    caption = ""
    if produto:
        try:
            caption = formatar_mensagem(produto)
            ok(f"Caption gerado ({len(caption)} chars)")
        except Exception as e:
            err(f"Geração de caption: {e}")

    # 6. Instagram — credenciais
    ig_token = cfg("instagram_access_token","")
    ig_uid   = cfg("instagram_user_id","")
    if not ig_token:
        err("Instagram token NÃO configurado → vá em Configurar Instagram API")
        erro_fatal = True
    elif not ig_token.startswith("EAA"):
        warn(f"Token suspeito (não começa com EAA): {ig_token[:20]}...")
    else:
        ok(f"Instagram token presente: {ig_token[:20]}...")

    if not ig_uid:
        err("Instagram User ID NÃO configurado"); erro_fatal = True
    else:
        ok(f"Instagram User ID: {ig_uid}")

    # 7. Cota diária
    if ig_token and ig_uid:
        try:
            usado, limite, pode = _ig_checar_limite_diario(ig_token, ig_uid)
            if not pode:
                err(f"COTA ESGOTADA: {usado}/{limite} posts hoje. Aguarde amanhã.")
                erro_fatal = True
            else:
                ok(f"Cota OK: {usado}/{limite} usados | restam {limite-usado}")
        except Exception as e:
            warn(f"Verificação de cota: {e}")

    # 7b. WhatsApp — verifica configuração
    wa_inst  = cfg("whatsapp_instance_id","")
    wa_token = cfg("whatsapp_token","")
    wa_group = cfg("whatsapp_group_id","")
    post_wa  = cfg("post_whatsapp","false") == "true"
    if post_wa:
        if wa_inst and wa_token and wa_group:
            ok(f"WhatsApp configurado: instância={wa_inst[:12]}... grupo={wa_group[:12]}...")
        else:
            err(f"WhatsApp ATIVADO mas faltam campos: inst={'✅' if wa_inst else '❌'} token={'✅' if wa_token else '❌'} grupo={'✅' if wa_group else '❌'}")
    else:
        warn("WhatsApp desativado nas configurações (ative em /config para postar no grupo)")

    # 8. Teste real de postagem (somente se tudo OK)
    post_result = ""
    if request.method == "POST" and not erro_fatal and ig_token and ig_uid and img_url and caption:
        canais_ok = []; canais_err = []
        # Instagram
        try:
            ok_post, result = instagram_post(img_url, caption, ig_token, ig_uid)
            if ok_post:
                ok(f"✅ INSTAGRAM — Post publicado! ID={result}")
                canais_ok.append("instagram")
                if produto: salvar_produto(produto, "success", "instagram", caption)
            else:
                err(f"❌ INSTAGRAM recusou: {result}")
                canais_err.append("instagram")
        except Exception as e:
            err(f"❌ INSTAGRAM exceção: {e}")
            canais_err.append("instagram")
        # WhatsApp
        if post_wa and wa_inst and wa_token and wa_group:
            try:
                ok_wa, res_wa = whatsapp_post(img_url, caption, wa_inst, wa_token, wa_group)
                if ok_wa:
                    ok(f"✅ WHATSAPP — Postado no grupo com sucesso!")
                    canais_ok.append("whatsapp")
                else:
                    err(f"❌ WHATSAPP falhou: {str(res_wa)[:80]}")
                    canais_err.append("whatsapp")
            except Exception as e:
                err(f"❌ WHATSAPP exceção: {e}")
                canais_err.append("whatsapp")
        elif post_wa:
            warn("WhatsApp ativado mas credenciais incompletas — não testado")
        post_result = "sucesso" if canais_ok else "falha"
    elif request.method == "POST" and erro_fatal:
        err("Postagem não tentada — corrija os erros acima primeiro")

    # Renderiza resultado
    linhas_html = ""
    for tipo, msg in passos:
        cor = {"ok":"#e8f5e9","err":"#ffebee","warn":"#fff8e1"}[tipo]
        icon = {"ok":"✅","err":"❌","warn":"⚠️"}[tipo]
        linhas_html += f"<div style='padding:10px 14px;background:{cor};border-radius:8px;margin-bottom:6px;font-size:13px'>{icon} {msg}</div>"

    botao = ""
    if not erro_fatal and ig_token and ig_uid and img_url:
        botao = """
        <form method='POST' style='margin-top:16px'>
          <button type='submit' style='width:100%;padding:14px;background:#ee4d2d;color:#fff;
            border:none;border-radius:10px;font-size:15px;font-weight:700;cursor:pointer'>
            🚀 Executar postagem de teste agora
          </button>
        </form>"""

    resultado_banner = ""
    if post_result == "sucesso":
        resultado_banner = "<div style='background:#e8f5e9;border:2px solid #4caf50;border-radius:12px;padding:16px;text-align:center;font-size:16px;font-weight:700;color:#2e7d32;margin-bottom:16px'>🎉 POST PUBLICADO NO INSTAGRAM COM SUCESSO!</div>"
    elif post_result in ("falha","excecao"):
        resultado_banner = "<div style='background:#ffebee;border:2px solid #f44336;border-radius:12px;padding:16px;text-align:center;font-size:16px;font-weight:700;color:#c62828;margin-bottom:16px'>❌ FALHA NA POSTAGEM — veja detalhes abaixo</div>"

    preview_caption = f"<div style='background:#f5f5f5;border-radius:8px;padding:12px;margin-top:12px;font-size:12px;white-space:pre-wrap;max-height:200px;overflow-y:auto'>{caption[:500]}...</div>" if caption else ""
    preview_img = f"<img src='{img_url}' style='max-width:120px;border-radius:8px;margin-top:8px'>" if img_url else ""

    return f"""<!DOCTYPE html>
<html lang='pt-BR'>
<head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Teste de Postagem — Brizzah</title>{CSS}</head>
<body>
<div class='header'>
  <h1>🔬 Diagnóstico de Postagem</h1>
  <a href='/dashboard'>← Voltar</a>
</div>
<div class='content'>
  {resultado_banner}
  <div class='card'>
    <h3 style='margin-bottom:12px'>Resultado de cada etapa:</h3>
    {linhas_html}
    {preview_img}
    {preview_caption}
    {botao}
  </div>
  <div class='card' style='background:#fff8e1;border-left:4px solid #f39c12'>
    <b>💡 Como usar:</b><br>
    <small>Esta página executa todas as etapas sem postar. Clique no botão vermelho para testar uma postagem real e ver o resultado imediato.</small>
  </div>
</div>
</body></html>"""




# ════════════════════════════════════════════════════════════════════
# BRIZZAH V4 MÁXIMA CONVERSÃO — SCRAPER HÍBRIDO DE PREÇO REAL
# Usa requests/metatags primeiro e Playwright como fallback quando instalado.
# ════════════════════════════════════════════════════════════════════

def _brizzah_v4_extract_prices_from_text(texto):
    import re as _re
    vals = []
    if not texto:
        return 0, 0
    for m in _re.finditer(r'R\$\s*([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?|[0-9]{2,6})', texto, flags=_re.I):
        val = _safe_float(m.group(1), 0)
        if val and 5 <= val <= 50000:
            vals.append(val)
    if not vals:
        return 0, 0
    clean=[]
    for v in vals:
        if all(abs(v-x)>0.01 for x in clean):
            clean.append(v)
    vals=clean
    atual = min(vals)
    antigos = [v for v in vals if v > atual * 1.08]
    antigo = max(antigos) if antigos else 0
    return atual, antigo


def _brizzah_v4_requests_parse(url, source=''):
    import re as _re
    dados = {"name":"", "price":0, "old_price":0, "image_url":"", "product_url":url, "store_name":"", "coupon":"", "installments":""}
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Cache-Control": "no-cache",
        }
        r = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        html = r.text or ''
        dados['product_url'] = r.url or url
        def meta(prop):
            pats = [
                r'<meta[^>]+property=["\']' + _re.escape(prop) + r'["\'][^>]+content=["\']([^"\']+)',
                r'<meta[^>]+name=["\']' + _re.escape(prop) + r'["\'][^>]+content=["\']([^"\']+)',
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']' + _re.escape(prop) + r'["\']',
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']' + _re.escape(prop) + r'["\']',
            ]
            for pat in pats:
                m = _re.search(pat, html, flags=_re.I|_re.S)
                if m:
                    return _html_unescape(m.group(1))
            return ''
        try:
            for prod in _extract_jsonld_products(html):
                if prod.get('name') and not dados['name']:
                    dados['name'] = _limpar_nome_produto_ext(prod.get('name'), source)
                img = prod.get('image')
                if isinstance(img, list): img = img[0] if img else ''
                if img and not dados['image_url']:
                    dados['image_url'] = _fix_img(str(img))
                offers = prod.get('offers') or {}
                if isinstance(offers, list): offers = offers[0] if offers else {}
                if isinstance(offers, dict):
                    price = offers.get('price') or offers.get('lowPrice') or offers.get('highPrice')
                    if price and not dados['price']:
                        dados['price'] = _safe_float(price, 0)
                    seller = offers.get('seller') or {}
                    if isinstance(seller, dict) and seller.get('name'):
                        dados['store_name'] = seller.get('name')
        except Exception:
            pass
        title = meta('og:title') or meta('twitter:title')
        if not title:
            m = _re.search(r'<title[^>]*>(.*?)</title>', html, flags=_re.I|_re.S)
            if m: title = _re.sub(r'\s+', ' ', m.group(1)).strip()
        if title:
            dados['name'] = _limpar_nome_produto_ext(title, source)
        img = meta('og:image') or meta('twitter:image') or meta('image')
        if img: dados['image_url'] = _fix_img(img)
        price_meta = meta('product:price:amount') or meta('og:price:amount') or meta('twitter:data1')
        if not dados['price']:
            dados['price'] = _safe_float(price_meta, 0)
        if not dados['price']:
            p_atual, p_antigo = _brizzah_v4_extract_prices_from_text(html[:350000])
            dados['price'] = p_atual
            dados['old_price'] = p_antigo
        else:
            dados['old_price'] = _extrair_preco_antigo_texto(html[:350000], dados['price'])
        if not dados['old_price'] and dados['price']:
            p_atual, p_antigo = _brizzah_v4_extract_prices_from_text(html[:350000])
            if p_antigo and p_antigo > dados['price']:
                dados['old_price'] = p_antigo
        for pat in [r'Cupom\s*[:\-]?\s*</?[^>]*>?\s*([A-Z0-9]{4,24})', r'cupom\s+([A-Z0-9]{4,24})']:
            m = _re.search(pat, html, flags=_re.I|_re.S)
            if m:
                dados['coupon'] = m.group(1).upper(); break
        parc = _re.search(r'([0-9]{1,2})x\s*(?:de\s*)?R\$\s*([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2})', html, flags=_re.I)
        if parc:
            dados['installments'] = f"{parc.group(1)}x R$ {parc.group(2)}"
    except Exception as e:
        try: log('WARN', f'[V4][REQ] falhou: {str(e)[:80]}')
        except Exception: pass
    return dados


def _brizzah_v4_playwright_parse(url, source=''):
    dados = {"name":"", "price":0, "old_price":0, "image_url":"", "product_url":url, "store_name":"", "coupon":"", "installments":""}
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return dados
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=['--no-sandbox','--disable-dev-shm-usage'])
            page = browser.new_page(locale='pt-BR', user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36')
            page.set_default_timeout(18000)
            page.goto(url, wait_until='domcontentloaded', timeout=25000)
            try:
                page.wait_for_load_state('networkidle', timeout=8000)
            except Exception:
                pass
            try:
                page.evaluate('window.scrollTo(0, Math.min(800, document.body.scrollHeight))')
                page.wait_for_timeout(1200)
                page.evaluate('window.scrollTo(0,0)')
            except Exception:
                pass
            dados['product_url'] = page.url or url
            title = ''
            for sel in ['h1', '[data-testid="product-title"]', '.ui-pdp-title', '.product-name', '.product-title']:
                try:
                    loc = page.locator(sel).first
                    if loc.count():
                        txt = loc.inner_text(timeout=2500)
                        if txt and len(txt.strip()) > len(title): title = txt.strip()
                except Exception:
                    pass
            if not title:
                try: title = page.title() or ''
                except Exception: title = ''
            if title: dados['name'] = _limpar_nome_produto_ext(title, source)
            img = ''
            for sel in ['meta[property="og:image"]', 'meta[name="twitter:image"]']:
                try:
                    val = page.locator(sel).first.get_attribute('content', timeout=2000)
                    if val: img = val; break
                except Exception: pass
            if not img:
                for sel in ['img.ui-pdp-image', 'img[data-testid="image"]', '.product-image img', 'img']:
                    try:
                        loc = page.locator(sel).first
                        if loc.count():
                            val = loc.get_attribute('src', timeout=2000) or loc.get_attribute('data-src', timeout=2000)
                            if val and 'logo' not in val.lower(): img = val; break
                    except Exception: pass
            if img: dados['image_url'] = _fix_img(img)
            try: body = page.locator('body').inner_text(timeout=6000)
            except Exception: body = ''
            price_candidates=[]
            for sel in ['.andes-money-amount__fraction','[data-testid="price-part"]','[data-testid="product-price"]','.price','.product-price','.default-price','.sales-price']:
                try:
                    locs = page.locator(sel)
                    n = min(locs.count(), 8)
                    for i in range(n):
                        txt = locs.nth(i).inner_text(timeout=1200)
                        v = _safe_float(txt, 0)
                        if v and 5 <= v <= 50000: price_candidates.append(v)
                except Exception: pass
            if price_candidates:
                dados['price'] = min(price_candidates)
                oldc=[v for v in price_candidates if v > dados['price']*1.08]
                if oldc: dados['old_price'] = max(oldc)
            if not dados['price']:
                p_atual, p_antigo = _brizzah_v4_extract_prices_from_text(body)
                dados['price'] = p_atual
                dados['old_price'] = p_antigo
            elif not dados['old_price']:
                p_atual, p_antigo = _brizzah_v4_extract_prices_from_text(body)
                if p_antigo and p_antigo > dados['price']:
                    dados['old_price'] = p_antigo
            import re as _re
            m = _re.search(r'(?:Cupom|cupom)\s*[:\-]?\s*([A-Z0-9]{4,24})', body)
            if m: dados['coupon'] = m.group(1).upper()
            parc = _re.search(r'([0-9]{1,2})x\s*(?:de\s*)?R\$\s*([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2})', body, flags=_re.I)
            if parc: dados['installments'] = f"{parc.group(1)}x R$ {parc.group(2)}"
            browser.close()
    except Exception as e:
        try: log('WARN', f'[V4][BROWSER] falhou: {str(e)[:100]}')
        except Exception: pass
    return dados


def _brizzah_merge_dados(base, extra):
    out = dict(base or {})
    for k,v in (extra or {}).items():
        if k in ('price','old_price'):
            if _safe_float(v,0) and not _safe_float(out.get(k),0): out[k]=v
        else:
            if v and (not out.get(k) or _nome_generico_externo(str(out.get(k)))):
                out[k]=v
    return out


def extrair_dados_link_externo(url, source=''):
    source = (source or '').lower().strip()
    url = _limpar_url_compartilhamento(url)
    url_destino, nome_slug = _url_destino_e_nome_slug_externo(url)
    request_url = url_destino or url
    nome_default = nome_slug or ("Oferta selecionada" if not source else f"Oferta selecionada na {_fonte_emoji_externo(source)[1]}")
    dados = {"name": nome_default, "price":0, "old_price":0, "image_url":"", "product_url": request_url, "store_name":"", "coupon":"", "installments":""}
    if not request_url: return dados
    d1 = _brizzah_v4_requests_parse(request_url, source)
    dados = _brizzah_merge_dados(dados, d1)
    precisa_browser = (not _safe_float(dados.get('price'),0)) or _nome_generico_externo(dados.get('name')) or len(str(dados.get('name',''))) < 12
    if precisa_browser:
        d2 = _brizzah_v4_playwright_parse(request_url, source)
        dados = _brizzah_merge_dados(dados, d2)
    # Brizzah PRO: nunca inventar preço antigo. Se o anúncio não informar, mantém 0.
    if not _safe_float(dados.get('old_price'),0):
        dados['old_price'] = 0
    if _nome_generico_externo(dados.get('name')):
        dados['name'] = nome_slug or f"Oferta selecionada na {_fonte_emoji_externo(source)[1]}"
    try: log('INFO', f"[V4] import {source}: nome='{str(dados.get('name'))[:45]}' preço={dados.get('price')} img={'OK' if dados.get('image_url') else 'N/A'}")
    except Exception: pass
    return dados


def _caption_wa_externo(p):
    emoji, fonte = _fonte_emoji_externo(p.get('source'))
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p))[:115]
    preco = _safe_float(p.get('price'), 0)
    orig = _safe_float(p.get('old_price'), 0)
    orig = _brz_preco_antigo_confiavel(preco, orig, p.get('source'))
    desc = int((1 - preco / orig) * 100) if orig and preco and orig > preco else 0
    link = p.get('affiliate_url') or p.get('product_url') or ''
    cupom = (p.get('coupon') or '').strip()
    installments = (p.get('installments') or '').strip()
    linhas = [f"*{_headline_promo_externo(p)}*", "", nome, ""]
    if preco:
        if orig and orig > preco:
            linhas.append(f"de {_format_brl(orig)} por *{_format_brl(preco)}* 👊")
            if desc >= 15: linhas.append(f"{desc}% OFF 🔥")
        else:
            linhas.append(f"por *{_format_brl(preco)}* 👊")
        if installments: linhas.append(f"💳 {installments}")
    else:
        linhas.append("💰 Confira o preço atualizado no link 👇")
    if cupom: linhas.append(f"Cupom: *{cupom}* ⚠️")
    linhas += ["", f"Loja oficial {fonte}:", f"🔗 {link}", "", "_Brizzah | Achados inteligentes_ 🔥"]
    return "\n".join(linhas)




# ════════════════════════════════════════════════════════
#  BRIZZAH PRO — FILTRO PROFISSIONAL DE OFERTAS
#  - Sem preço fake
#  - Sem cupom não validado
#  - Todos com prioridade igual
#  - Fila rotativa entre Netshoes, Shopee e Mercado Livre
#  - Links ruins são arquivados automaticamente
# ════════════════════════════════════════════════════════

def _brz_norm_source(src):
    src = (src or "").lower().strip()
    if src in ("ml", "mercado livre", "mercadolivre", "meli"):
        return "mercadolivre"
    if src in ("netshoe", "netshoes"):
        return "netshoes"
    if src in ("shopee", "shoppe"):
        return "shopee"
    if src in ("amazon", "amazon brasil"):
        return "amazon"
    return src or "externo"


def _brz_preco_antigo_confiavel(preco, antigo, source=""):
    """Só aceita preço antigo quando faz sentido. Nunca inventa 'de/por'.

    Regra especial: Netshoes costuma bloquear/mascarar preço antigo em HTML.
    Para evitar o erro clássico "de R$ 500" falso, Netshoes só mostra preço atual.
    """
    source = _brz_norm_source(source)
    preco = _safe_float(preco, 0)
    antigo = _safe_float(antigo, 0)
    if source == "netshoes":
        return 0
    if not preco or not antigo:
        return 0
    if antigo <= preco:
        return 0
    desconto = int((1 - preco / antigo) * 100)
    # Bloqueia descontos exagerados que geralmente vêm de scraping errado.
    if desconto < 5 or desconto > 70:
        return 0
    # Bloqueia preço antigo muito distante do atual, ex.: R$500 x R$209 sem confirmação.
    if antigo > preco * 2.20:
        return 0
    return round(antigo, 2)


def validar_cupom_auto(p):
    """Cupom só aparece se tiver padrão confiável. Genéricos são bloqueados."""
    import re as _re
    cupom = (p.get("coupon") or p.get("cupom") or "").strip().upper()
    source = _brz_norm_source(p.get("source") or p.get("origem"))
    if not cupom:
        return False
    bloqueados = {
        "SPAN", "PROMO", "DESCONTO", "OFERTA", "SALE", "CUPOM",
        "NULL", "NONE", "N/A", "BRASIL", "APP", "SITE"
    }
    if cupom in bloqueados:
        return False
    # Precisa ter letras e números. Evita palavra genérica capturada do HTML.
    if not _re.fullmatch(r"(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{5,16}", cupom):
        return False
    # Mercado Livre costuma gerar muitos falsos positivos por HTML. Só aceita ML se marcado manualmente.
    if source == "mercadolivre" and int(p.get("cupom_valido") or 0) != 1:
        return False
    return True


def produto_externo_profissional_valido(p):
    """Filtro profissional: só deixa postar item com dados mínimos reais."""
    nome = (p.get("name") or "").strip()
    source = _brz_norm_source(p.get("source") or p.get("origem"))
    preco = _safe_float(p.get("price"), 0)
    link = (p.get("affiliate_url") or p.get("product_url") or p.get("link") or "").strip()
    nome_low = nome.lower()

    if not link.startswith("http"):
        return False, "sem link válido"
    if not nome or len(nome) < 12:
        return False, "nome curto/ausente"
    if _nome_generico_externo(nome) or "produto importado" in nome_low:
        return False, "nome genérico"
    if preco <= 0:
        return False, "sem preço"
    # Bloqueia códigos soltos: MLB123, 48Q1IgE, etc.
    import re as _re
    if _re.fullmatch(r"[A-Z0-9]{4,14}", nome.strip(), flags=_re.I):
        return False, "nome parece código"
    # Evita kits/lotes ruins que costumam converter mal e poluir o grupo.
    ruins = ["lote", "atacado", "revenda", "100 unidades", "50 unidades"]
    if any(x in nome_low for x in ruins):
        return False, "produto com perfil ruim"
    # Shopee pode ter produtos bons sem marca, mas precisa pelo menos nome e preço.
    if source not in {"netshoes", "shopee", "mercadolivre", "amazon", "externo"}:
        return False, "origem inválida"
    return True, "ok"


def _brz_arquivar_produto_ruim(pid, motivo=""):
    try:
        with get_db() as c:
            c.execute("""
                UPDATE external_products
                SET status='archived', is_active=0, notes=COALESCE(notes,'') || ?
                WHERE id=?
            """, (f"\n[auto] Arquivado: {motivo}", pid))
        try: log("WARN", f"[FILTRO PRO] Produto {pid} arquivado: {motivo}")
        except Exception: pass
    except Exception as e:
        try: log("WARN", f"[FILTRO PRO] Falha ao arquivar {pid}: {str(e)[:60]}")
        except Exception: pass


def _brz_proxima_ordem_fontes():
    """Alterna fontes sem depender de prioridade manual."""
    ordem = ["netshoes", "shopee", "mercadolivre", "amazon", "externo"]
    last = _brz_norm_source(cfg("_brz_last_source", ""))
    if last in ordem:
        i = ordem.index(last)
        return ordem[i+1:] + ordem[:i+1]
    return ordem


def buscar_external_products_aprovados(limit=30):
    """Versão PRO: prioridade igual, curadoria por fonte e fila rotativa."""
    with get_db() as c:
        rows = c.execute("""
            SELECT * FROM external_products
            WHERE status='approved' AND is_active=1
            ORDER BY id ASC
        """).fetchall()
    todos = [dict(r) for r in rows]

    validos = []
    for p in todos:
        ok, motivo = produto_externo_profissional_valido(p)
        if not ok:
            _brz_arquivar_produto_ruim(p.get("id"), motivo)
            continue
        # Sanitiza preço antigo falso no próprio objeto antes de postar.
        p["old_price"] = _brz_preco_antigo_confiavel(p.get("price"), p.get("old_price"), p.get("source"))
        p["source"] = _brz_norm_source(p.get("source"))
        validos.append(p)

    if not validos:
        return []

    hoje = _agora_brasil().date().isoformat() if '_agora_brasil' in globals() else datetime.now().date().isoformat()
    nao_postados_hoje = [p for p in validos if not str(p.get("last_posted") or "").startswith(hoje)]
    # Se todos já foram postados hoje, libera a rotação novamente pegando os menos recentes.
    base = nao_postados_hoje if nao_postados_hoje else validos

    ordem = _brz_proxima_ordem_fontes()
    resultado = []
    usados = set()

    # 1 produto por fonte por rodada, para alternar Netshoes/Shopee/ML.
    for fonte in ordem:
        candidatos = [p for p in base if p.get("id") not in usados and _brz_norm_source(p.get("source")) == fonte]
        candidatos.sort(key=lambda p: (str(p.get("last_posted") or ""), p.get("id") or 0))
        if candidatos:
            escolhido = candidatos[0]
            resultado.append(escolhido)
            usados.add(escolhido.get("id"))
        if len(resultado) >= limit:
            break

    # Completa com o restante menos recente.
    if len(resultado) < limit:
        resto = [p for p in base if p.get("id") not in usados]
        resto.sort(key=lambda p: (str(p.get("last_posted") or ""), p.get("id") or 0))
        resultado.extend(resto[:max(0, limit-len(resultado))])

    return resultado[:limit]


def ja_postado_recentemente_externo(prod_id, horas=6):
    """PRO: não bloqueia por prioridade; a rotação controla repetição/fila."""
    return False


def score_externo(p):
    """Score PRO sem prioridade manual: qualidade + marca + preço + cliques."""
    ok, _ = produto_externo_profissional_valido(p)
    if not ok:
        return -9999
    nome = (p.get("name") or "").lower()
    preco = _safe_float(p.get("price"), 0)
    clicks = _safe_int(p.get("clicks"), 0)
    source = _brz_norm_source(p.get("source"))
    s = 0
    if 20 <= preco <= 99: s += 8
    elif 100 <= preco <= 199: s += 7
    elif 200 <= preco <= 399: s += 5
    elif preco > 0: s += 2
    marcas = ["nike", "adidas", "puma", "fila", "mizuno", "new balance", "jbl", "xiaomi", "lattafa"]
    if any(m in nome for m in marcas): s += 8
    if source == "netshoes": s += 4
    elif source == "mercadolivre": s += 4
    elif source == "shopee": s += 3
    elif source == "amazon": s += 2
    s += min(clicks, 10)
    return s


def _caption_wa_externo(p):
    emoji, fonte = _fonte_emoji_externo(_brz_norm_source(p.get("source")))
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p))[:115]
    preco = _safe_float(p.get("price"), 0)
    orig = _brz_preco_antigo_confiavel(preco, p.get("old_price"), p.get("source"))
    desc = int((1 - preco / orig) * 100) if orig and preco and orig > preco else 0
    link = p.get("affiliate_url") or p.get("product_url") or p.get("link") or ""
    installments = (p.get("installments") or "").strip()

    linhas = [f"*{_headline_promo_externo(p)}*", "", nome, ""]
    if preco:
        if orig and desc >= 5:
            linhas.append(f"de {_format_brl(orig)} por *{_format_brl(preco)}* 👊")
            linhas.append(f"🔥 {desc}% OFF")
        else:
            linhas.append(f"por *{_format_brl(preco)}* 👊")
        if installments:
            linhas.append(f"💳 {installments}")
    else:
        linhas.append("💰 Confira o preço atualizado no link 👇")

    if validar_cupom_auto(p):
        linhas.append(f"🎟 Cupom confirmado: *{(p.get('coupon') or p.get('cupom')).strip().upper()}*")
    else:
        linhas.append("🔥 Oferta com desconto no anúncio")

    linhas += ["", f"{emoji} Origem: *{fonte}*", f"🔗 {link}", "", "_Brizzah | Achados inteligentes_ 🔥"]
    return "\n".join(linhas)


def _caption_ig_externo(p):
    emoji, fonte = _fonte_emoji_externo(_brz_norm_source(p.get("source")))
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p))[:110]
    preco = _safe_float(p.get("price"), 0)
    orig = _brz_preco_antigo_confiavel(preco, p.get("old_price"), p.get("source"))
    desc = int((1 - preco / orig) * 100) if orig and preco and orig > preco else 0
    link = p.get("affiliate_url") or p.get("product_url") or p.get("link") or ""
    linhas = [f"{emoji} {_headline_promo_externo(p)}", "", nome, ""]
    if preco:
        if orig and desc >= 5:
            linhas.append(f"De {_format_brl(orig)} por {_format_brl(preco)} 🔥 {desc}% OFF")
        else:
            linhas.append(f"Preço de hoje: {_format_brl(preco)}")
    else:
        linhas.append("Confira o preço atualizado no link 👇")
    if validar_cupom_auto(p):
        linhas.append(f"Cupom confirmado: {(p.get('coupon') or p.get('cupom')).strip().upper()}")
    linhas += ["", f"Origem: {fonte}", "Entre no grupo VIP para receber primeiro 👆", link[:120], "", "#achadinhos #ofertas #brizzah #promocao #desconto"]
    return "\n".join(linhas)


def _gerar_caption_wa(produto):
    """Caption segura também para Shopee/API: nunca inventa preço antigo."""
    nome  = _corrigir_portugues_produto((produto.get("name","") or "")[:75])
    preco = _safe_float(produto.get("price"), 0)
    orig  = _brz_preco_antigo_confiavel(preco, produto.get("original_price") or produto.get("old_price"), produto.get("source"))
    desc  = int((1-preco/orig)*100) if orig and preco and orig>preco else 0
    link  = produto.get("affiliate_url") or produto.get("product_url","")
    sold  = _safe_int(produto.get("sold"), 0)
    stars = _safe_float(produto.get("rating"), 0)
    try: _e, _n = _fonte_emoji(produto)
    except Exception: _e, _n = "🛍️", _fonte_emoji_externo(_brz_norm_source(produto.get("source")))[1]
    p = [f"{_e} *{nome}*", ""]
    if preco:
        if orig and desc >= 5:
            p.append(f"de {_format_brl(orig)} por *{_format_brl(preco)}* 🔥")
            p.append(f"{desc}% OFF confirmado")
        else:
            p.append(f"💰 *{_format_brl(preco)}*")
    else:
        p.append("💰 Confira o preço atualizado no link")
    if stars > 0: p.append(f"⭐ {stars:.1f}/5")
    if sold > 0: p.append(f"📦 {sold}+ vendidos")
    if validar_cupom_auto(produto):
        p.append(f"🎟 Cupom confirmado: *{(produto.get('coupon') or produto.get('cupom')).strip().upper()}*")
    p += ["", f"🔗 {link}", "", "_Brizzah | Achados inteligentes_ 🔥"]
    return "\n".join(p)


def _brz_marcar_fonte_postada(source):
    try:
        cfg_set("_brz_last_source", _brz_norm_source(source))
    except Exception:
        pass



# ════════════════════════════════════════════════════════
#  BRIZZAH PRO FINAL — INTELIGÊNCIA DE PRODUTO + RODÍZIO
#  Correções: camisa de time não vira tênis, calça não vira camisa,
#  Shopee entra junto na rotação e agenda fica 30/30min.
# ════════════════════════════════════════════════════════

def _brz_txt_norm(txt):
    try:
        import unicodedata as _unicodedata
        s = str(txt or '').lower()
        return ''.join(c for c in _unicodedata.normalize('NFD', s) if _unicodedata.category(c) != 'Mn')
    except Exception:
        return str(txt or '').lower()


def _brz_tipo_produto_inteligente(nome):
    n = _brz_txt_norm(nome)
    times = [
        'botafogo','flamengo','palmeiras','santos','corinthians','sao paulo','vasco',
        'fluminense','gremio','internacional','cruzeiro','atletico','bahia','brasil',
        'real madrid','barcelona','psg','manchester','liverpool','chelsea','arsenal',
        'juventus','milan','bayern'
    ]
    # Prioridade máxima: peças de roupa/time antes de tênis.
    if any(w in n for w in ['camisa','camiseta','jersey','manto','uniforme','regata','polo']):
        if any(t in n for t in times) or any(w in n for w in ['futebol','torcedor','oficial','clube']):
            return 'camisa_futebol'
        if any(w in n for w in ['academia','treino','fitness','dry fit','dri fit','corrida']):
            return 'camisa_treino'
        return 'camisa'
    if any(w in n for w in ['calca','jogger','bermuda','short','legging']):
        return 'calca_bermuda'
    if any(w in n for w in ['tenis','sneaker','sapato','chuteira','sandalia','chinelo','bota','calcado']):
        return 'calcado'
    if any(w in n for w in ['moletom','jaqueta','casaco','corta vento']):
        return 'roupa_frio'
    if any(w in n for w in ['whey','creatina','suplemento','pre treino','protein']):
        return 'suplemento'
    if any(w in n for w in ['perfume','colonia','fragrancia','lattafa']):
        return 'perfume'
    if any(w in n for w in ['fone','headphone','jbl','xiaomi','caixa de som','bluetooth']):
        return 'eletronico'
    if any(w in n for w in ['mochila','bolsa','mala']):
        return 'bolsa_mochila'
    return 'geral'


def _headline_promo_externo(p):
    import random as _random
    nome = (p.get('name') or p.get('nome') or '').strip()
    source = _brz_norm_source(p.get('source') or p.get('fonte') or '') if '_brz_norm_source' in globals() else (p.get('source') or '').lower()
    tipo = _brz_tipo_produto_inteligente(nome)
    mapa = {
        'camisa_futebol': [
            'MANTO COM PREÇO BOM PRA APROVEITAR',
            'CAMISA DE TIME COM PREÇO DE GARIMPO',
            'PRA TORCER NO ESTILO SEM PAGAR CARO',
            'ACHADO FORTE PRA QUEM CURTE FUTEBOL',
        ],
        'camisa_treino': [
            'CAMISA DE TREINO COM PREÇO FORTE',
            'PRA TREINAR NO ESTILO SEM PAGAR CARO',
            'PEÇA ESPORTIVA COM PREÇO BOM',
        ],
        'camisa': [
            'PREÇO BOM DEMAIS NESSA CAMISA',
            'CAMISA COM PREÇO DE GARIMPO',
            'PRA USAR TODO DIA SEM SOFRER',
        ],
        'calca_bermuda': [
            'PREÇO TOP NESSA PEÇA',
            'CALÇA/BERMUDA COM PREÇO DE GARIMPO',
            'PRA RENOVAR O VISUAL SEM PAGAR CARO',
        ],
        'calcado': [
            'CALÇADO COM PREÇO DE OPORTUNIDADE',
            'CONFORTO E ESTILO COM PREÇO BOM',
            'PRA SAIR NO ESTILO SEM PAGAR CARO',
        ],
        'roupa_frio': [
            'PEÇA DE FRIO COM PREÇO BOM',
            'ACHADO PRA SAIR NO ESTILO',
        ],
        'suplemento': [
            'SUPLEMENTO COM PREÇO DE GUERRA',
            'PRA REFORÇAR O PROJETO FITNESS',
        ],
        'perfume': [
            'PERFUME COM PREÇO PRA APROVEITAR',
            'CHEIRO DE QUEM GARIMPA OFERTA BOA',
        ],
        'eletronico': [
            'ACHADO TECH PRA APROVEITAR',
            'GADGET COM PREÇO BOM DEMAIS',
        ],
        'bolsa_mochila': [
            'MOCHILA/BOLSA COM PREÇO BOM',
            'ACHADO PRA USAR TODO DIA',
        ],
        'geral': [
            'OFERTA BOA PRA APROVEITAR HOJE',
            'ACHADO FORTE DO DIA',
            'PREÇO BOM DEMAIS PRA DEIXAR PASSAR',
        ],
    }
    opts = list(mapa.get(tipo, mapa['geral']))
    if source == 'netshoes': opts.append('NETSHOES COM PREÇO DE GARIMPO')
    elif source == 'mercadolivre': opts.append('ACHADO FORTE NO MERCADO LIVRE')
    elif source == 'shopee': opts.append('ACHADO FORTE NA SHOPEE')
    elif source == 'amazon': opts.append('ACHADO FORTE NA AMAZON')
    return _random.choice(opts)


def _brz_aplicar_agenda_30min_runtime():
    try:
        grade = "07:00,07:30,08:00,08:30,09:00,09:30,10:00,10:30,11:00,11:30,12:00,12:30,13:00,13:30,14:00,14:30,15:00,15:30,16:00,16:30,17:00,17:30,18:00,18:30,19:00,19:30,20:00,20:30,21:00,21:30,22:00,22:30,23:00,23:30"
        if cfg('force_30min_schedule','true') == 'true' and cfg('auto_schedule','') != grade:
            cfg_set('auto_schedule', grade)
            log('INFO', '[PRO] Agenda 30/30min aplicada no runtime')
        if cfg('products_per_cycle','1') != '1':
            cfg_set('products_per_cycle','1')
    except Exception as e:
        try: log('WARN', f'[PRO] agenda runtime falhou: {str(e)[:80]}')
        except Exception: pass

_brz_aplicar_agenda_30min_runtime()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)


# ════════════════════════════════════════════════════════
#  CURADORIA MANUAL — buscar, selecionar e postar
# ════════════════════════════════════════════════════════

# Nichos rápidos para o wa_manual
_WA_NICHOS_RAPIDOS = [
    ("","🔍 Busca livre"),("moda feminina","👗 Moda Feminina"),
    ("moda masculina","👔 Moda Masculina"),("tenis calcados","👟 Calçados"),
    ("eletronicos gadget","📱 Eletrônicos"),("casa decoracao","🏠 Casa"),
    ("beleza skincare","💄 Beleza"),("fitness academia","💪 Fitness"),
    ("brinquedo kids","🎁 Infantil"),("pet shop","🐾 Pet"),
    ("cozinha utilidades","🍳 Cozinha"),("bolsa acessorios","👜 Acessórios"),
    ("relogio joias","⌚ Relógios"),("suplemento saude","💊 Saúde"),
]

# ═══════════════════════════════════════════════════════════════════════
# AUTOMAÇÃO WHATSAPP — Rodízio de nichos automático
# ═══════════════════════════════════════════════════════════════════════
_WA_NICHOS_AUTO = [
    # ── MAIS VENDIDOS E RECOMENDADOS (prioridade máxima) ──
    ("mais vendido shopee","🏆"),
    ("tendencia shopee","🔥"),
    ("melhor avaliado shopee","⭐"),
    ("viral shopee","🔥"),
    ("produto recomendado shopee","👍"),
    ("oferta do dia shopee","💰"),
    # ── MODA FEMININA ──
    ("vestido feminino","👗"),
    ("conjunto feminino","👗"),
    ("blusa feminina","👗"),
    ("calca feminina","👖"),
    ("macacão feminino","👗"),
    # ── MODA MASCULINA ──
    ("camiseta masculina","👔"),
    ("calca masculina","👖"),
    ("moletom masculino","👕"),
    # ── CALÇADOS ──
    ("tenis feminino","👟"),
    ("tenis masculino","👟"),
    ("sandalia feminina","👡"),
    # ── TECH ──
    ("fone bluetooth","🎧"),
    ("smartwatch","⌚"),
    ("carregador rapido","⚡"),
    ("caixinha som bluetooth","🔊"),
    # ── CASA ──
    ("airfryer","🍳"),
    ("organizador casa","🏠"),
    ("kit cozinha","🍴"),
    ("luminaria led","💡"),
    # ── BELEZA ──
    ("skincare","💆"),
    ("perfume feminino","🌸"),
    ("kit maquiagem","💄"),
    ("creme hidratante","✨"),
    # ── FITNESS ──
    ("legging fitness","💪"),
    ("kit academia","💪"),
    ("garrafa termica","🥤"),
    # ── ACESSÓRIOS ──
    ("bolsa feminina","👜"),
    ("mochila","🎒"),
    ("oculos sol","😎"),
    # ── PET ──
    ("racao cachorro","🐕"),
    ("brinquedo pet","🐾"),
]
# Palavras proibidas — filtra replicas, sensores, itens irrelevantes
_WA_FILTRO_NEGATIVO = [
    # Réplicas / falsificados
    "replica","copia","falso","fake","imitacao",
    # Peças e componentes técnicos
    "sensor","termometro","termostato","resistencia","componente",
    "parafuso","ferramenta","solda","adaptador","modulo","arduino",
    "disjuntor","rele","lampada","tomada","fio eletrico",
    "peca reposicao","display celular","tela celular","bateria celular",
    "flex cabo","lcd","motherboard","carcaca celular",
    # Livros / papelaria
    "livro","caderno","caneta","agenda","papelaria",
    "biblico","biblia","romance","literatura",
    # Jogos infantis
    "jogo infantil","brinquedo bebe","boneca","carrinho brinquedo",
    "pokemon carta","lego","massinha","slime",
    # Lingerie sem apelo comercial
    "fio dental calcinha","calcinha fio","fantasia erotica",
    # Outros sem apelo
    "saco plastico","embalagem","etiqueta","lacre","pilha",
    # Festa
    "balao","enfeite festa","fantasia carnaval",
]
_wa_nicho_idx_auto = 0

def _ciclo_whatsapp_auto():
    """Ciclo automático do WhatsApp — roda em thread separada."""
    global _wa_nicho_idx_auto
    import time as _t, random as _r
    _t.sleep(30)  # aguarda app inicializar
    while True:
        try:
            if cfg("wa_auto_ativo","true") != "true":
                _t.sleep(60); continue
            intervalo = int(cfg("wa_auto_intervalo","15") or "15")
            _t.sleep(intervalo * 60)
            if cfg("wa_auto_ativo","true") != "true": continue
            wa_inst  = cfg("whatsapp_instance_id","brizzah-bot")
            wa_token = cfg("whatsapp_token","Brizzah@2025!")
            wa_group = cfg("whatsapp_group_id","120363407236556172@g.us")
            if not (wa_inst and wa_token and wa_group):
                log("WARN","[WA-AUTO] Credenciais WA não configuradas. Configure em /config"); continue
            # Verifica se Evolution API está online
            try:
                import requests as _req2
                _r_check = _req2.get(
                    f"https://evolution-api-lad2.onrender.com/instance/fetchInstances",
                    headers={"apikey": wa_token}, timeout=10)
                if _r_check.status_code not in (200, 201):
                    log("WARN", f"[WA-AUTO] Evolution API offline (status {_r_check.status_code}). Aguardando...")
                    _t.sleep(120); continue
            except Exception as _ec:
                log("WARN", f"[WA-AUTO] Evolution API inacessível: {str(_ec)[:50]}. Aguardando...")
                _t.sleep(120); continue
            qtd = int(cfg("wa_auto_qtd","2") or "2")
            for _ in range(qtd):
                nicho, emoji = _WA_NICHOS_AUTO[_wa_nicho_idx_auto % len(_WA_NICHOS_AUTO)]
                _wa_nicho_idx_auto += 1
                try:
                    # WA-AUTO usa apenas Shopee (Amazon/ML só têm links de busca, não produtos reais)
                    produtos = shopee_api_buscar_produtos(nicho, limit=25)
                    if not produtos:
                        termo_simples = nicho.split()[0]
                        produtos = shopee_api_buscar_produtos(termo_simples, limit=15)
                    if not produtos:
                        log("WARN", f"[WA-AUTO] Sem produtos Shopee para '{nicho}' — pulando")
                        continue
                    # Filtra: sem is_search_link, sem replicas, com vendas e preço válido
                    def _ok(x):
                        if x.get("is_search_link"): return False
                        nm=(x.get("name") or "").lower()
                        for t in _WA_FILTRO_NEGATIVO:
                            if t in nm: return False
                        if int(x.get("sold",0) or 0)<20: return False
                        pr=float(x.get("price",0) or 0)
                        return 15<=pr<=2000
                    val=[x for x in produtos if _ok(x)] or sorted(
                        [x for x in produtos if not x.get("is_search_link")],
                        key=lambda x:int(x.get("sold",0) or 0),reverse=True)[:5]
                    val.sort(key=lambda x:int(x.get("sold",0) or 0),reverse=True)
                    p = _r.choice(val[:5])
                    preco = float(p.get("price",0) or 0)
                    orig  = _brz_preco_antigo_confiavel(preco, p.get("original_price",0), p.get("source","shopee"))
                    desc  = int(round((1-preco/orig)*100)) if orig and orig > preco else 0
                    nome  = p.get("name","")
                    img   = p.get("image_url","")
                    link  = p.get("affiliate_url") or p.get("product_url","")
                    if img.startswith("//"): img="https:"+img
                    elif img and not img.startswith("http"): img="https://cf.shopee.com.br/file/"+img
                    if not img or not link: continue
                    pt = f"{preco:,.2f}".replace(",","X").replace(".",",").replace("X",".")
                    ot = f"{orig:,.2f}".replace(",","X").replace(".",",").replace("X",".") if orig else ""
                    cat = nicho.upper().split()[0]
                    _we, _wn = _fonte_emoji(p)
                    loja_str = f" | {_wn}" if p.get("source","") in ("amazon","mercadolivre","netshoes") else ""
                    linhas = [f"*{_we} {cat} EM OFERTA{loja_str}*","",nome[:70],""]
                    if orig and desc >= 5:
                        linhas.append(f"de R$ {ot} *por R$ {pt}* " + chr(128293))
                    else:
                        linhas.append(f"por *R$ {pt}* " + chr(128293))
                    if desc >= 20: linhas.append(f"*{desc}% de desconto!* " + chr(128176))
                    linhas += ["", chr(128279) + " Acesse: " + link]
                    caption = chr(10).join(linhas)
                    log("INFO", f"[WA-AUTO] Enviando: {nome[:35]} | img={img[:60]}")
                    ok_wa, res = whatsapp_post(img, caption, wa_inst, wa_token, wa_group)
                    log("INFO", f"[WA-AUTO] {'✅' if ok_wa else '❌ FALHOU: '+str(res)[:60]} | {nicho} | {nome[:35]}")
                    if qtd > 1: _t.sleep(10)
                except Exception as ex:
                    log("WARN", f"[WA-AUTO] nicho '{nicho}': {str(ex)[:60]}")
        except Exception as e:
            log("WARN", f"[WA-AUTO] ciclo: {str(e)[:80]}")
            _t.sleep(60)

import threading as _thr_wa2
_thr_wa2.Thread(target=_ciclo_whatsapp_auto, daemon=True).start()


@app.route("/wa_config", methods=["GET","POST"])
@login_required
def wa_config():
    """Configuração da automação automática do WhatsApp."""
    msg = ""
    if request.method == "POST":
        # wa_auto_ativo: checkbox só envia valor quando marcado
        ativo_val = "true" if request.form.get("wa_auto_ativo") else "false"
        cfg_set("wa_auto_ativo", ativo_val)
        for k in ["wa_auto_intervalo","wa_auto_qtd"]:
            v = request.form.get(k,"")
            if v: cfg_set(k, v)
        msg = "✅ Configurações salvas!"

    ativo     = cfg("wa_auto_ativo","false") == "true"
    intervalo = cfg("wa_auto_intervalo","15")
    qtd       = cfg("wa_auto_qtd","2")
    _nichos_display = [
        ("👗","Moda Feminina"),("👔","Moda Masculina"),("👟","Calçados"),
        ("📱","Eletrônicos"),("🏠","Casa/Decoração"),("💄","Beleza"),
        ("💪","Fitness"),("🎁","Infantil"),("🐾","Pet"),("🍳","Cozinha"),
    ]
    nichos_lista = chr(10).join([f"{e} {n}" for e,n in _nichos_display])

    return render_template_string("""<!DOCTYPE html><html><head>
<meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Automação WhatsApp</title>""" + CSS + """
<style>
.campo{width:100%;padding:10px;border:1px solid #ddd;border-radius:8px;font-size:14px;box-sizing:border-box;margin-bottom:12px}
.toggle{display:flex;align-items:center;gap:12px;margin-bottom:16px}
.toggle input[type=checkbox]{width:50px;height:26px;cursor:pointer;accent-color:#25D366}
.btn-save{background:#25D366;color:#fff;border:none;border-radius:10px;padding:13px;width:100%;font-size:15px;font-weight:700;cursor:pointer}
.info-box{background:#e8f5e9;border-left:4px solid #25D366;padding:14px;border-radius:8px;margin-bottom:16px;font-size:13px;line-height:1.7}
</style></head><body>
<div class='header'><h1>⚙️ Automação WhatsApp</h1><a href='/wa_manual'>← Voltar</a></div>
<div class='content'>
{% if msg %}<div style='background:#e8f5e9;border-left:4px solid #27ae60;padding:12px;border-radius:8px;margin-bottom:16px;font-weight:600'>{{msg}}</div>{% endif %}

<div class='info-box'>
  <b>Como funciona:</b><br>
  O bot busca produtos automaticamente nos melhores nichos da Shopee e envia
  para o seu grupo do WhatsApp no intervalo configurado. Os nichos se alternam
  automaticamente a cada envio.
</div>

<form method='POST'>
  <div class='toggle'>
    <input type='checkbox' name='wa_auto_ativo' value='true' {% if ativo %}checked{% endif %}>
    <label style='font-weight:700;font-size:16px'>{% if ativo %}✅ Automação ATIVADA{% else %}❌ Automação DESATIVADA{% endif %}</label>
  </div>

  <label style='font-weight:600;font-size:13px'>⏱️ Intervalo entre posts (minutos)</label>
  <select name='wa_auto_intervalo' class='campo'>
    {% for v,l in [("5","5 min — muito frequente"),("10","10 min"),("15","15 min (recomendado)"),("20","20 min"),("30","30 min"),("60","1 hora")] %}
    <option value='{{v}}' {% if intervalo==v %}selected{% endif %}>{{l}}</option>
    {% endfor %}
  </select>

  <label style='font-weight:600;font-size:13px'>📦 Produtos por envio</label>
  <select name='wa_auto_qtd' class='campo'>
    {% for v,l in [("1","1 produto"),("2","2 produtos (recomendado)"),("3","3 produtos"),("4","4 produtos")] %}
    <option value='{{v}}' {% if qtd==v %}selected{% endif %}>{{l}}</option>
    {% endfor %}
  </select>

  <div style='background:#f5f5f5;border-radius:10px;padding:14px;margin-bottom:16px'>
    <b style='font-size:13px'>🔄 Nichos em rodízio automático:</b>
    <pre style='font-size:12px;line-height:1.8;margin-top:8px;white-space:pre-wrap'>{{nichos_lista}}</pre>
  </div>

  <button type='submit' class='btn-save'>💾 Salvar Configurações</button>
</form>

<div style='margin-top:16px;background:#fff8e1;border-left:4px solid #f9a825;padding:12px;border-radius:8px;font-size:12px'>
  ⚠️ <b>Dica:</b> Com 15 min de intervalo e 2 produtos por envio = <b>8 produtos/hora</b>.
  Recomendamos de 4 a 8 produtos/hora para não sobrecarregar o grupo.
</div>
</div></body></html>""",
        msg=msg, ativo=ativo, intervalo=intervalo, qtd=qtd, nichos_lista=nichos_lista)


_WA_NICHOS_RAPIDOS = [
    ("","Livre"),("moda feminina","Feminina"),("moda masculina","Masculina"),
    ("tenis calcados","Calcados"),("eletronicos gadget","Tech"),
    ("casa decoracao","Casa"),("beleza skincare","Beleza"),
    ("fitness academia","Fitness"),("brinquedo kids","Infantil"),
    ("pet shop","Pet"),("cozinha utilidades","Cozinha"),
    ("suplemento saude","Saude"),
]



def _normalizar_img_url(img):
    img = (img or "").strip()
    if img.startswith("//"):
        img = "https:" + img
    elif img and not img.startswith("http"):
        img = "https://cf.shopee.com.br/file/" + img
    return img

def _caption_manual_produto(produto):
    try:
        return formatar_mensagem(produto)
    except Exception:
        nome = produto.get("name","Produto")
        preco = float(produto.get("price",0) or 0)
        link = produto.get("affiliate_url") or produto.get("product_url","")
        return f"{nome}\n\nR$ {preco:.2f}\n{link}"


@app.route("/manual_post", methods=["GET","POST"])
@login_required
def manual_post():
    termo = request.form.get("termo","").strip()
    plataforma = request.form.get("plataforma","todas").strip().lower()
    destino = request.form.get("destino","").strip().lower()
    msg = ""
    produtos = []

    def _buscar():
        nonlocal produtos, msg
        if not termo:
            msg = "Digite um termo para buscar."
            return
        raw = buscar_produtos_plataforma(termo, plataforma, limit=20)
        # filtra e ordena
        validos = []
        seen = set()
        for p in raw:
            nome = (p.get("name","") or "").strip()
            img = _normalizar_img_url(p.get("image_url",""))
            key = (nome[:35].lower(), img.split("/")[-1][:24])
            if not nome or not img or key in seen:
                continue
            seen.add(key)
            p["image_url"] = img
            validos.append(p)
        validos = sorted(validos, key=lambda x: calcular_score_produto(x), reverse=True)[:12]
        produtos = validos
        msg = f"{len(produtos)} produto(s) encontrado(s)." if produtos else "Nenhum resultado."

    if request.method == "POST":
        acao = request.form.get("acao","")
        if acao == "buscar":
            _buscar()
        elif acao == "postar":
            idx = int(request.form.get("idx","0") or "0")
            termo = request.form.get("termo","").strip()
            plataforma = request.form.get("plataforma","todas").strip().lower()
            _buscar()
            if 0 <= idx < len(produtos):
                p = produtos[idx]
                img = _normalizar_img_url(p.get("image_url",""))
                caption = _caption_manual_produto(p)
                canais_ok = []
                canais_err = []
                if destino in ("instagram","ambos"):
                    ig_token = cfg("instagram_access_token","")
                    ig_uid = cfg("instagram_user_id","")
                    if ig_token and ig_uid:
                        img_p = preparar_imagem_produto(img, p) or img
                        ok_ig, res_ig = instagram_post(img_p, caption, ig_token, ig_uid)
                        if ok_ig:
                            canais_ok.append("Instagram")
                            try: salvar_produto(p, "success", "instagram_manual", caption)
                            except: pass
                        else:
                            canais_err.append("Instagram: " + str(res_ig)[:80])
                    else:
                        canais_err.append("Instagram não configurado")
                if destino in ("whatsapp","ambos"):
                    wa_inst  = cfg("whatsapp_instance_id","")
                    wa_token = cfg("whatsapp_token","")
                    wa_group = cfg("whatsapp_group_id","")
                    if wa_inst and wa_token and wa_group:
                        ok_wa, res_wa = whatsapp_post(img, _gerar_caption_wa(p), wa_inst, wa_token, wa_group)
                        if ok_wa:
                            canais_ok.append("WhatsApp")
                        else:
                            canais_err.append("WhatsApp: " + str(res_wa)[:80])
                    else:
                        canais_err.append("WhatsApp não configurado")
                if canais_ok:
                    msg = "Postado com sucesso em: " + ", ".join(canais_ok)
                if canais_err:
                    msg += (" | " if msg else "") + "Erros: " + " ; ".join(canais_err)
            else:
                msg = "Produto não encontrado para postar."

    opts = """
      <option value='todas' {t}>Todas</option>
      <option value='shopee' {s}>Shopee</option>
      <option value='netshoes' {n}>Netshoes</option>
      <option value='amazon' {a}>Amazon</option>
      <option value='mercadolivre' {m}>Mercado Livre</option>
    """.format(
        t="selected" if plataforma=="todas" else "",
        s="selected" if plataforma=="shopee" else "",
        n="selected" if plataforma=="netshoes" else "",
        a="selected" if plataforma=="amazon" else "",
        m="selected" if plataforma=="mercadolivre" else "",
    )

    cards = ""
    for i,p in enumerate(produtos):
        nome = (p.get("name","Produto") or "Produto").replace("'", "&#39;")
        img = _normalizar_img_url(p.get("image_url",""))
        preco = float(p.get("price",0) or 0)
        fonte = (p.get("source","") or plataforma or "plataforma").title()
        sold = int(p.get("sold",0) or 0)
        rt = float(p.get("rating",0) or 0)
        cards += f"""
        <div class='card' style='padding:12px'>
          <div style='display:flex;gap:12px;align-items:flex-start'>
            <img src='{img}' style='width:90px;height:90px;object-fit:contain;border-radius:10px;border:1px solid #eee;background:#fff'>
            <div style='flex:1'>
              <div style='font-weight:700;font-size:14px;line-height:1.35'>{nome}</div>
              <div style='margin-top:6px;color:#ee4d2d;font-weight:700'>R$ {preco:.2f}</div>
              <div style='font-size:12px;color:#666;margin-top:4px'>Fonte: {fonte} {"| 🛒 "+str(sold) if sold else ""} {"| ⭐ "+str(round(rt,1)) if rt else ""}</div>
              <div style='display:flex;gap:8px;flex-wrap:wrap;margin-top:10px'>
                <form method='POST' style='display:inline'>
                  <input type='hidden' name='acao' value='postar'>
                  <input type='hidden' name='termo' value='{termo}'>
                  <input type='hidden' name='plataforma' value='{plataforma}'>
                  <input type='hidden' name='idx' value='{i}'>
                  <input type='hidden' name='destino' value='instagram'>
                  <button style='padding:8px 12px;border:none;border-radius:8px;background:#c13584;color:#fff;font-weight:700;cursor:pointer'>Instagram</button>
                </form>
                <form method='POST' style='display:inline'>
                  <input type='hidden' name='acao' value='postar'>
                  <input type='hidden' name='termo' value='{termo}'>
                  <input type='hidden' name='plataforma' value='{plataforma}'>
                  <input type='hidden' name='idx' value='{i}'>
                  <input type='hidden' name='destino' value='whatsapp'>
                  <button style='padding:8px 12px;border:none;border-radius:8px;background:#25D366;color:#fff;font-weight:700;cursor:pointer'>WhatsApp</button>
                </form>
                <form method='POST' style='display:inline'>
                  <input type='hidden' name='acao' value='postar'>
                  <input type='hidden' name='termo' value='{termo}'>
                  <input type='hidden' name='plataforma' value='{plataforma}'>
                  <input type='hidden' name='idx' value='{i}'>
                  <input type='hidden' name='destino' value='ambos'>
                  <button style='padding:8px 12px;border:none;border-radius:8px;background:#ee4d2d;color:#fff;font-weight:700;cursor:pointer'>Postar nos dois</button>
                </form>
              </div>
            </div>
          </div>
        </div>
        """

    msg_html = f"<div class='card' style='background:#f7fbff;border-left:4px solid #1976d2;font-weight:700'>{msg}</div>" if msg else ""
    return f"""<!DOCTYPE html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Busca Manual</title>{CSS}</head><body>
    <div class='header'><h1>🔎 Busca Manual Multi-Plataforma</h1><a href='/dashboard'>← Voltar</a></div>
    <div class='content'>
      {msg_html}
      <form method='POST' class='card'>
        <label style='font-weight:700;font-size:13px'>Produto ou palavra-chave</label>
        <input type='text' name='termo' value='{termo}' placeholder='Ex: tenis nike masculino' style='width:100%;padding:12px;border-radius:10px;border:1px solid #ddd;margin:6px 0 12px'>
        <label style='font-weight:700;font-size:13px'>Plataforma</label>
        <select name='plataforma' style='width:100%;padding:12px;border-radius:10px;border:1px solid #ddd;margin:6px 0 12px;background:#fff'>{opts}</select>
        <input type='hidden' name='acao' value='buscar'>
        <button style='width:100%;padding:12px;border:none;border-radius:10px;background:#1976d2;color:#fff;font-weight:700;cursor:pointer'>Buscar produtos</button>
      </form>
      <div style='display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px;margin-top:14px'>{cards}</div>
    </div></body></html>"""
@app.route("/wa_manual", methods=["GET","POST"])
@login_required
def wa_manual():
    wa_inst  = cfg("whatsapp_instance_id","")
    wa_token = cfg("whatsapp_token","")
    wa_group = cfg("whatsapp_group_id","")
    msg      = ""
    produtos = []
    sel      = {}

    NICHOS = [
        ("moda feminina","Moda Fem"),("moda masculina","Moda Masc"),
        ("tenis calcados","Calcados"),("eletronicos","Tech"),
        ("casa decoracao","Casa"),("beleza skincare","Beleza"),
        ("fitness academia","Fitness"),("brinquedo kids","Infantil"),
        ("pet shop","Pet"),("cozinha","Cozinha"),("suplemento","Saude"),
    ]

    acao     = request.form.get("acao","")
    termo    = request.form.get("termo","").strip()
    nicho    = request.form.get("nicho","").strip()
    query    = nicho if nicho else termo

    # ── BUSCA ──────────────────────────────────────────────────
    if acao in ("buscar","nicho"):
        if query:
            try:
                raw = shopee_api_buscar_produtos(query, limit=60) or []
                produtos = selecionar_produtos_top(raw, nicho_alvo=query, limite=20, horas_repeticao=24)
                msg = "OK: " + str(len(produtos)) + " produtos encontrados" if produtos else "Nenhum resultado para: " + query
            except Exception as e:
                msg = "Erro na busca: " + str(e)[:60]

    # ── SELECIONAR ─────────────────────────────────────────────
    elif acao == "sel":
        termo = request.form.get("termo","")
        nicho = request.form.get("nicho","")
        query = nicho if nicho else termo
        sel = {
            "nome":       request.form.get("s_nome",""),
            "preco_pix":  request.form.get("s_pp",""),
            "preco_orig": request.form.get("s_po",""),
            "loja":       request.form.get("s_loja","Shopee"),
            "link":       request.form.get("s_link",""),
            "img_url":    request.form.get("s_img",""),
        }
        if query:
            try:
                raw = shopee_api_buscar_produtos(query, limit=60) or []
                produtos = selecionar_produtos_top(raw, nicho_alvo=query, limite=20, horas_repeticao=24)
            except: pass
        msg = "Produto selecionado. Edite e envie."

    # ── ENVIAR ─────────────────────────────────────────────────
    elif acao == "enviar":
        titulo    = request.form.get("titulo","").strip()
        nome      = request.form.get("nome","").strip()
        p_orig    = request.form.get("preco_orig","").strip()
        p_pix     = request.form.get("preco_pix","").strip()
        cupom     = request.form.get("cupom","").strip()
        loja      = request.form.get("loja","Shopee").strip()
        link      = request.form.get("link","").strip()
        img       = request.form.get("img_url","").strip()
        if img.startswith("//"): img = "https:" + img
        elif img and not img.startswith("http"): img = "https://cf.shopee.com.br/file/" + img
        sel = {"nome":nome,"preco_pix":p_pix,"preco_orig":p_orig,
               "loja":loja,"link":link,"img_url":img,"titulo":titulo,"cupom":cupom}
        if not (wa_inst and wa_token and wa_group):
            msg = "ERRO: WhatsApp nao configurado"
        elif not nome or not link:
            msg = "ERRO: Preencha Nome e Link"
        else:
            NL = chr(10); cap = ""
            if titulo: cap += "*" + titulo.upper() + "*" + NL + NL
            cap += nome + NL + NL
            if p_orig and p_pix:
                cap += "de R$ " + p_orig + " *por R$ " + p_pix + " no Pix* " + chr(128293) + NL
            elif p_pix:
                cap += "*por R$ " + p_pix + " no Pix* " + chr(128293) + NL
            if cupom: cap += "Cupom: *" + cupom + "* " + chr(9888) + NL
            cap += NL
            if loja: cap += "Loja: " + loja + NL
            cap += link
            try:
                ok, res = whatsapp_post(img, cap, wa_inst, wa_token, wa_group)
                msg = "ENVIADO!" if ok else "FALHOU: " + str(res)[:80]
                if ok: log("INFO","[WA-MANUAL] " + nome[:40])
            except Exception as e:
                msg = "ERRO: " + str(e)[:80]

    # ── GERA HTML ──────────────────────────────────────────────
    wa_ok  = "Configurado" if (wa_inst and wa_token and wa_group) else "NAO configurado"
    wa_bg  = "#e8f5e9" if (wa_inst and wa_token and wa_group) else "#ffebee"

    # Cor da mensagem
    if msg.startswith("OK") or msg == "ENVIADO!" or "selecionado" in msg:
        mbg = "#e8f5e9"; mbc = "#27ae60"
    elif msg.startswith("ERRO") or msg.startswith("FALHOU"):
        mbg = "#ffebee"; mbc = "#e53935"
    else:
        mbg = "#fff8e1"; mbc = "#f9a825"

    # HTML da mensagem
    mhtml = ""
    if msg:
        mhtml = ("<div style='background:" + mbg + ";border-left:4px solid " + mbc +
                 ";padding:12px;border-radius:8px;margin-bottom:12px;font-weight:600'>" +
                 msg + "</div>")

    # Botoes de nicho
    nhtml = ""
    for v, l in NICHOS:
        bg = "#ee4d2d" if v == nicho else "#eee"
        fc = "#fff"   if v == nicho else "#333"
        nhtml += ("<form method='POST' style='display:inline-block;margin:2px'>"
                  "<input type='hidden' name='acao' value='nicho'>"
                  "<input type='hidden' name='nicho' value='" + v + "'>"
                  "<input type='hidden' name='termo' value='" + termo + "'>"
                  "<button type='submit' style='background:" + bg + ";color:" + fc +
                  ";border:none;border-radius:20px;padding:6px 12px;font-size:12px;"
                  "cursor:pointer'>" + l + "</button></form>")

    # Cards de produtos
    chtml = ""
    for p in produtos:
        pr   = float(p.get("price",0) or 0)
        img2 = p.get("image_url","")
        if img2.startswith("//"): img2 = "https:" + img2
        elif img2 and not img2.startswith("http"): img2 = "https://cf.shopee.com.br/file/" + img2
        og   = _brz_preco_antigo_confiavel(pr, p.get("original_price",0), p.get("source","shopee"))
        nm   = (p.get("name") or "")[:30]
        lj   = (p.get("shop_name") or "Shopee")
        lk   = (p.get("affiliate_url") or p.get("product_url",""))
        pf   = str(round(pr,2)).replace(".",",")
        of   = str(round(og,2)).replace(".",",")
        is_s = (sel.get("img_url","")[-15:] == img2[-15:]) if sel.get("img_url") else False
        bdr  = "border:2px solid #25D366" if is_s else "border:1px solid #eee"
        chtml += (
            "<form method='POST' style='display:inline-block;vertical-align:top;margin:3px'>"
            "<input type='hidden' name='acao' value='sel'>"
            "<input type='hidden' name='termo' value='" + termo + "'>"
            "<input type='hidden' name='nicho' value='" + nicho + "'>"
            "<input type='hidden' name='s_nome' value='" + nm.replace("'","") + "'>"
            "<input type='hidden' name='s_pp'   value='" + pf + "'>"
            "<input type='hidden' name='s_po'   value='" + of + "'>"
            "<input type='hidden' name='s_loja' value='" + lj.replace("'","") + "'>"
            "<input type='hidden' name='s_link' value='" + lk + "'>"
            "<input type='hidden' name='s_img'  value='" + img2 + "'>"
            "<button type='submit' style='background:#fff;" + bdr + ";"
            "border-radius:12px;padding:8px;cursor:pointer;width:120px;"
            "text-align:center;box-shadow:0 1px 4px rgba(0,0,0,.1)'>"
            "<img src='" + img2 + "' style='width:100px;height:100px;"
            "object-fit:contain;border-radius:6px;display:block;margin:0 auto'>"
            "<small style='font-size:10px;color:#555;white-space:normal;"
            "display:block;margin-top:3px'>" + nm[:25] + "</small>"
            "<b style='color:#ee4d2d;font-size:12px'>R$ " + pf + "</b>"
            "</button></form>"
        )

    # Preview
    phtml = ""; img_show = sel.get("img_url","")
    if img_show and not img_show.startswith("http"):
        img_show = "https://cf.shopee.com.br/file/" + img_show
    if sel.get("nome"):
        lp = []
        if sel.get("titulo"): lp += [sel["titulo"].upper(), ""]
        lp += [sel["nome"], ""]
        if sel.get("preco_orig") and sel.get("preco_pix"):
            lp.append("de R$ " + sel["preco_orig"] + "  por R$ " + sel["preco_pix"] + " no Pix")
        elif sel.get("preco_pix"):
            lp.append("por R$ " + sel["preco_pix"] + " no Pix")
        if sel.get("cupom"): lp.append("Cupom: " + sel.get("cupom",""))
        if sel.get("loja"):  lp.append("Loja: " + sel["loja"])
        if sel.get("link"):  lp.append(sel["link"][:55])
        phtml = "<br>".join(lp)

    # Monta pagina
    html = (
        "<!DOCTYPE html><html><head>"
        "<meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>Post WA</title>" + CSS +
        "<style>"
        "input,select{width:100%;padding:10px;border:1px solid #ddd;border-radius:8px;"
        "font-size:14px;box-sizing:border-box;margin-bottom:10px}"
        "input:focus{outline:none;border-color:#25D366}"
        "label{font-size:13px;font-weight:600;color:#555;display:block;margin-bottom:4px}"
        "</style></head><body>"
        "<div class='header'><h1>Post WhatsApp</h1>"
        "<div style='display:flex;gap:10px'>"
        "<a href='/wa_config' style='color:#25D366;font-size:13px'>Auto</a> "
        "<a href='/wa_teste'  style='color:#888;font-size:13px'>Teste</a> "
        "<a href='/dashboard'>Voltar</a>"
        "</div></div>"
        "<div class='content'>"
        "<div style='background:" + wa_bg + ";padding:10px;border-radius:8px;margin-bottom:12px'>"
        "WA: <b>" + wa_ok + "</b>"
        "</div>" +
        mhtml +
        # Busca
        "<form method='POST'>"
        "<input type='hidden' name='acao' value='buscar'>"
        "<input type='hidden' name='nicho' value='" + nicho + "'>"
        "<div style='display:flex;gap:8px;margin-bottom:8px'>"
        "<input name='termo' value='" + termo + "' placeholder='Buscar produto...' "
        "style='margin:0;flex:1'>"
        "<button type='submit' style='background:#ee4d2d;color:#fff;border:none;"
        "border-radius:8px;padding:10px 16px;font-weight:700;cursor:pointer;white-space:nowrap'>"
        "Buscar</button>"
        "</div>"
        "</form>"
        # Nichos
        "<div style='margin-bottom:12px;line-height:2.5'>" + nhtml + "</div>" +
        # Cards
        ("<div style='margin-bottom:12px'>"
         "<p style='font-size:12px;color:#666;margin:0 0 6px'>Clique para selecionar:</p>"
         "<div style='overflow-x:auto;white-space:nowrap'>" + chtml + "</div>"
         "</div>" if chtml else "") +
        # Formulario de envio
        "<form method='POST' style='background:#fff;border-radius:14px;padding:16px;"
        "box-shadow:0 2px 12px rgba(0,0,0,.1)'>"
        "<input type='hidden' name='acao' value='enviar'>"
        "<input type='hidden' name='img_url' value='" + sel.get("img_url","") + "'>"
        "<p style='font-weight:700;font-size:15px;margin:0 0 12px;border-bottom:1px solid #eee;padding-bottom:8px'>Edite e envie</p>"
        "<label>Titulo (ex: PRA GALERA DO TENIS)</label>"
        "<input name='titulo' value='" + sel.get("titulo","") + "' placeholder='PRA GALERA DA MODA'>"
        "<label>Nome do produto *</label>"
        "<input name='nome'   value='" + sel.get("nome","") + "' placeholder='Nome do produto'>"
        "<div style='display:grid;grid-template-columns:1fr 1fr;gap:8px'>"
        "<div><label>Preco original</label>"
        "<input name='preco_orig' value='" + sel.get("preco_orig","") + "' placeholder='189,90'></div>"
        "<div><label>Preco Pix</label>"
        "<input name='preco_pix'  value='" + sel.get("preco_pix","") + "' placeholder='89,90'></div>"
        "</div>"
        "<label>Cupom (opcional)</label>"
        "<input name='cupom' value='" + sel.get("cupom","") + "' placeholder='PROMO10'>"
        "<label>Loja</label>"
        "<input name='loja'  value='" + sel.get("loja","Shopee") + "'>"
        "<label>Link *</label>"
        "<input name='link'  value='" + sel.get("link","") + "' placeholder='https://...'>" +
        (
            "<div style='display:flex;gap:10px;align-items:flex-start;background:#dcf8c6;"
            "border-radius:10px;padding:12px;margin-bottom:10px'>" +
            ("<img src='" + img_show + "' style='width:80px;height:80px;object-fit:contain;"
             "border-radius:6px;flex-shrink:0'>" if img_show else "") +
            "<div style='font-size:14px;line-height:1.9'>" + phtml + "</div>"
            "</div>"
            if phtml else ""
        ) +
        "<button type='submit' style='background:#25D366;color:#fff;border:none;"
        "border-radius:10px;padding:14px;width:100%;font-size:16px;font-weight:700;"
        "cursor:pointer'>Enviar para o Grupo WhatsApp</button>"
        "</form>"
        "</div></body></html>"
    )
    return html


@app.route("/renovar_token")
@login_required
def renovar_token():
    """Renova o token do Instagram automaticamente (long-lived token)."""
    token  = cfg("instagram_access_token","")
    app_id = os.environ.get("FACEBOOK_APP_ID","")
    secret = os.environ.get("FACEBOOK_APP_SECRET","")
    if not token:
        return "<h2>Token não configurado em /ig_setup</h2>"
    # Verifica status atual
    try:
        r = requests.get("https://graph.facebook.com/v19.0/me",
                         params={"access_token": token}, timeout=10)
        d = r.json()
        if "error" in d:
            status = f"❌ Token inválido: {d['error'].get('message','')}"
        else:
            status = f"✅ Token válido — ID: {d.get('id','?')}"
    except Exception as e:
        status = f"Erro ao verificar: {e}"
    # Tenta renovar se tiver app_id e secret
    renovado = ""
    if app_id and secret:
        try:
            r2 = requests.get(
                "https://graph.facebook.com/v19.0/oauth/access_token",
                params={"grant_type":"fb_exchange_token","client_id":app_id,
                        "client_secret":secret,"fb_exchange_token":token}, timeout=15)
            d2 = r2.json()
            if "access_token" in d2:
                novo = d2["access_token"]
                cfg_set("instagram_access_token", novo)
                renovado = f"✅ Token renovado com sucesso!"
                log("INFO","[TOKEN] Token Instagram renovado automaticamente")
            else:
                renovado = f"❌ Falha: {d2}"
        except Exception as e:
            renovado = f"❌ Erro: {e}"
    else:
        renovado = "⚠️ Configure FACEBOOK_APP_ID e FACEBOOK_APP_SECRET nas variáveis de ambiente do Render para renovação automática."
    return f"""<html><head><meta charset='utf-8'></head><body style='font-family:sans-serif;padding:20px;max-width:600px;margin:0 auto'>
    <h2>🔑 Token Instagram</h2>
    <p><b>Status:</b> {status}</p>
    <p><b>Renovação:</b> {renovado}</p>
    <p style='background:#fff3e0;padding:12px;border-radius:8px;font-size:14px'>
    <b>Token expira em 60 dias.</b> Se expirou, vá em
    <a href='https://developers.facebook.com/tools/explorer' target='_blank'>Graph API Explorer</a>,
    gere um novo token e salve em <a href='/ig_setup'>/ig_setup</a>.
    </p>
    <a href='/dashboard' style='color:#1976d2'>← Voltar</a>
    </body></html>"""


@app.route("/config_plataformas", methods=["GET","POST"])
@login_required
def config_plataformas():
    """Configuração das plataformas de afiliados."""
    msg = ""
    if request.method == "POST":
        for k in ["amazon_affiliate_tag","ml_affiliate_id","ml_ativo",
                  "netshoes_affiliate_id","netshoes_ativo"]:
            v = request.form.get(k,"")
            cfg_set(k, v)
        msg = "✅ Configurações salvas!"
    vals = {
        "amazon_affiliate_tag": cfg("amazon_affiliate_tag","brizzah-20"),
        "ml_affiliate_id":      cfg("ml_affiliate_id","ad20260407202239"),
        "ml_ativo":             cfg("ml_ativo","true"),
        "netshoes_affiliate_id":cfg("netshoes_affiliate_id","4686648"),
        "netshoes_ativo":       cfg("netshoes_ativo","true"),
    }
    return render_template_string("""<!DOCTYPE html><html><head>
<meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>Plataformas Afiliados</title>"""+CSS+"""
<style>
.c{width:100%;padding:10px;border:1px solid #ddd;border-radius:8px;font-size:14px;box-sizing:border-box;margin-bottom:10px}
.lb{font-size:13px;font-weight:600;color:#555;margin-bottom:4px;display:block}
.card{background:#fff;border-radius:12px;padding:16px;margin-bottom:16px;box-shadow:0 2px 8px rgba(0,0,0,.08)}
.btns{background:#ee4d2d;color:#fff;border:none;border-radius:8px;padding:13px;width:100%;font-size:15px;font-weight:700;cursor:pointer}
</style></head><body>
<div class='header'><h1>🏪 Plataformas Afiliados</h1><a href='/dashboard'>Voltar</a></div>
<div class='content'>
{% if msg %}<div style='background:#e8f5e9;border-left:4px solid #27ae60;padding:12px;border-radius:8px;margin-bottom:16px;font-weight:600'>{{msg}}</div>{% endif %}
<form method='POST'>
  <!-- Amazon -->
  <div class='card'>
    <div style='display:flex;align-items:center;gap:10px;margin-bottom:12px'>
      <span style='font-size:24px'>🛒</span>
      <div><b>Amazon Brasil</b><br><small style='color:#888'>amazon.com.br/associados</small></div>
    </div>
    <label class='lb'>Tag de Afiliado (ex: brizzah-20)</label>
    <input class='c' name='amazon_affiliate_tag' value='{{vals.amazon_affiliate_tag}}' placeholder='seunome-20'>
    <small style='color:#888'>Cadastre-se em <a href='https://associados.amazon.com.br' target='_blank'>associados.amazon.com.br</a></small>
  </div>
  <!-- Mercado Livre -->
  <div class='card'>
    <div style='display:flex;align-items:center;gap:10px;margin-bottom:12px'>
      <span style='font-size:24px'>🟡</span>
      <div><b>Mercado Livre</b><br><small style='color:#888'>mercadolivre.com.br/afiliados</small></div>
    </div>
    <label class='lb'>ID de Afiliado ML</label>
    <input class='c' name='ml_affiliate_id' value='{{vals.ml_affiliate_id}}' placeholder='ID do programa de afiliados'>
    <label class='lb' style='display:flex;align-items:center;gap:8px'>
      <input type='checkbox' name='ml_ativo' value='true' {{'checked' if vals.ml_ativo in ('true','1','') or not vals.ml_ativo else ''}}>
      Ativar busca no Mercado Livre (mesmo sem afiliado)
    </label>
    <small style='color:#888'>Cadastre-se em <a href='https://www.mercadolivre.com.br/afiliados' target='_blank'>mercadolivre.com.br/afiliados</a></small>
  </div>
  <!-- Netshoes -->
  <div class='card'>
    <div style='display:flex;align-items:center;gap:10px;margin-bottom:12px'>
      <span style='font-size:24px'>👟</span>
      <div><b>Netshoes</b><br><small style='color:#888'>Via Lomadee ou Awin</small></div>
    </div>
    <label class='lb'>ID de Afiliado (Lomadee sourceId)</label>
    <input class='c' name='netshoes_affiliate_id' value='{{vals.netshoes_affiliate_id}}' placeholder='ID Lomadee ou Awin'>
    <label class='lb' style='display:flex;align-items:center;gap:8px'>
      <input type='checkbox' name='netshoes_ativo' value='true' {{'checked' if vals.netshoes_ativo=='true' else ''}}>
      Ativar busca na Netshoes (mesmo sem afiliado)
    </label>
    <small style='color:#888'>Cadastre-se em <a href='https://www.lomadee.com' target='_blank'>lomadee.com</a></small>
  </div>
  <button type='submit' class='btns'>💾 Salvar Configurações</button>
</form>
</div></body></html>""", msg=msg, vals=type('V',(),vals)())


# ============================================================
# BRIZZAH V7 ROTACAO + SHOPEE + CTA + PORTUGUES
# ============================================================

LINK_GRUPO_BRIZZAH = "https://chat.whatsapp.com/Fb6kF0NXlwi8CzIVoPHMDr?mode=gi_t"

def _brz_norm_final(txt):
    try:
        import unicodedata, re
        s = str(txt or "").lower()
        s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        return re.sub(r"\s+", " ", s).strip()
    except Exception:
        return str(txt or "").lower().strip()

def _corrigir_portugues_produto(texto):
    try:
        import re
        t = str(texto or "").strip().replace("_", " ").replace("-", " ")
        regras = [
            (r"\btnis\b", "tênis"), (r"\btenis\b", "tênis"),
            (r"\balgodo\b", "algodão"), (r"\balgodao\b", "algodão"),
            (r"\b100\s*algod[oa]\b", "100% algodão"),
            (r"\bpreco\b", "preço"), (r"\bpromocao\b", "promoção"),
            (r"\bversao\b", "versão"), (r"\bconfortavel\b", "confortável"),
            (r"\bcalcado\b", "calçado"), (r"\bcalcados\b", "calçados"),
            (r"\balca\b", "alça"), (r"\bpeca\b", "peça"), (r"\bpecas\b", "peças"),
            (r"\brelogio\b", "relógio"), (r"\bcaes\b", "cães"), (r"\bces\b", "cães"), (r"\bcao\b", "cão"),
            (r"\bolympikus\b", "Olympikus"), (r"\badidas\b", "Adidas"), (r"\bpuma\b", "Puma"),
            (r"\bnike\b", "Nike"), (r"\bfila\b", "Fila"), (r"\bjbl\b", "JBL"), (r"\bxiaomi\b", "Xiaomi"),
            (r"\b3\s*stripes\b", "3-Stripes"), (r"\bessentials\b", "Essentials"),
        ]
        for pat, rep in regras:
            t = re.sub(pat, rep, t, flags=re.I)
        t = re.sub(r"\b30\s+40\s+50\s*cm\b", "30, 40 ou 50 cm", t, flags=re.I)
        t = re.sub(r"\s+", " ", t).strip()
        return (t[:1].upper() + t[1:]) if t else ""
    except Exception:
        return str(texto or "").strip()

def _nome_profissional_produto(nome):
    n = _brz_norm_final(nome)
    corrigido = _corrigir_portugues_produto(nome)

    if "tenis" in n:
        if "olympikus" in n and "versa" in n and "infantil" in n:
            return "Tênis Olympikus Versa infantil confortável"
        if "olympikus" in n:
            return "Tênis Olympikus confortável para o dia a dia"
        if "adidas" in n:
            return "Tênis Adidas confortável com preço de oportunidade"
        if "nike" in n:
            return "Tênis Nike confortável com preço de oportunidade"
        return corrigido

    if "toalha" in n:
        return "Jogo de toalhas de banho 100% algodão felpudas e macias"
    if "camiseta" in n and "adidas" in n and "infantil" in n:
        return "Camiseta Adidas Essentials infantil"
    if "camiseta" in n and "adidas" in n:
        return "Camiseta Adidas Essentials 3-Stripes em algodão"
    if "meia" in n and "puma" in n:
        return "Kit com 3 meias Puma infantil cano longo em algodão"
    if "chinelo" in n and "adidas" in n:
        return "Chinelo Adidas confortável e leve para uso diário"
    if "espelho" in n and "adnet" in n:
        return "Espelho Adnet redondo 30, 40 ou 50 cm com alça em couro decorativo"
    if ("caminha" in n or "cama pet" in n) and ("pet" in n or "gato" in n or "cao" in n or "caes" in n):
        return "Caminha pet redonda sherpa peludinha para cães e gatos"
    return corrigido

def _brz_categoria_produto(p):
    nome = _brz_norm_final((p.get("name") or "") + " " + (p.get("category") or "") + " " + (p.get("source") or ""))
    if (p.get("source") or "").lower() == "shopee": return "shopee"
    if any(x in nome for x in ["infantil","crianca","menino","menina","bebe","kids"]): return "infantil"
    if any(x in nome for x in ["feminina","feminino","mulher","vestido","saia","legging"]): return "feminino"
    if any(x in nome for x in ["masculina","masculino","homem","polo","bermuda masculina"]): return "masculino"
    if any(x in nome for x in ["tenis","sneaker","calcado","sapato","sandalia","chinelo","bota"]): return "tenis"
    if any(x in nome for x in ["pet","cachorro","cao","caes","gato","gatos","caminha","coleira","racao"]): return "pet"
    if any(x in nome for x in ["casa","cozinha","toalha","espelho","decoracao","organizador","banheiro","sala"]): return "casa"
    if any(x in nome for x in ["fone","jbl","xiaomi","bluetooth","caixa de som","smartwatch","carregador"]): return "tech"
    return "geral"

def _headline_promo_externo(p):
    nome = _nome_profissional_produto(p.get("name") or "")
    n = _brz_norm_final(nome)
    if "tenis" in n: return "TÊNIS CONFORTÁVEL COM PREÇO DE OPORTUNIDADE"
    if "toalha" in n: return "TOALHAS MACIAS COM PREÇO QUE VALE A PENA"
    if "camiseta" in n or "camisa" in n: return "CAMISETA BOA PRA USAR MUITO PAGANDO POUCO"
    if "chinelo" in n: return "CONFORTO NO DIA A DIA SEM PAGAR CARO"
    if "pet" in n or "caminha" in n or "gato" in n or "cachorro" in n or "cao" in n: return "CONFORTO PRO SEU PET COM PREÇO BAIXO"
    if "espelho" in n or "adnet" in n: return "UM TOQUE BONITO PRA CASA GASTANDO POUCO"
    if "meia" in n: return "BÁSICO QUE TODO MUNDO USA COM PREÇO BOM"
    if "fone" in n or "bluetooth" in n or "jbl" in n: return "ACHADO TECH COM PREÇO BOM"
    return "OFERTA BOA PRA APROVEITAR HOJE"

def _brz_cta_grupo():
    import random
    opcoes = [
        f"🚨 Quer receber ofertas assim todos os dias?\n👉 Entre no grupo VIP:\n{LINK_GRUPO_BRIZZAH}\n\n🔥 Manda pra aquele amigo que ama economizar!",
        f"💰 Só entra quem gosta de pagar barato!\n👉 Entre no grupo agora:\n{LINK_GRUPO_BRIZZAH}",
        f"🤝 Tem um amigo que ama promoção?\n👉 Manda esse grupo pra ele:\n{LINK_GRUPO_BRIZZAH}\n\n🔥 Aqui é só oferta de verdade!",
        f"⚡ Ofertas assim acabam rápido!\n👉 Entre no grupo VIP:\n{LINK_GRUPO_BRIZZAH}",
    ]
    return random.choice(opcoes)

def _caption_wa_externo(p):
    emoji, fonte = _fonte_emoji_externo(p.get("source"))
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p))[:115]
    preco = _safe_float(p.get("price"), 0)
    orig = _safe_float(p.get("old_price"), 0)
    orig = _brz_preco_antigo_confiavel(preco, orig, p.get("source"))
    desc = int((1 - preco / orig) * 100) if orig and preco and orig > preco else 0
    link = p.get("affiliate_url") or p.get("product_url") or ""
    cupom = (p.get("coupon") or "").strip()
    installments = (p.get("installments") or "").strip()

    linhas = [f"*{_headline_promo_externo(p)}*", "", nome, ""]
    if preco:
        if orig and orig > preco:
            linhas.append(f"de {_format_brl(orig)} por *{_format_brl(preco)}* 👊")
            if desc >= 10: linhas.append(f"🔥 {desc}% OFF")
        else:
            linhas.append(f"por *{_format_brl(preco)}* 👊")
        if installments: linhas.append(f"💳 {installments}")
    else:
        linhas.append("💰 Confira o preço atualizado no link 👇")
    linhas.append("🔥 Oferta com desconto no anúncio")
    linhas.append("⚠️ Preço pode mudar a qualquer momento")
    if cupom: linhas.append(f"Cupom: *{cupom}* ⚠️")
    linhas += ["", f"{emoji} Origem: *{fonte}*", f"🔗 {link}", "", _brz_cta_grupo(), "", "_Brizzah | Achados inteligentes_ 🔥"]
    return "\n".join(linhas)

def _caption_ig_externo(p):
    emoji, fonte = _fonte_emoji_externo(p.get("source"))
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p))[:110]
    preco = _safe_float(p.get("price"), 0)
    orig = _safe_float(p.get("old_price"), 0)
    orig = _brz_preco_antigo_confiavel(preco, orig, p.get("source"))
    desc = int((1 - preco / orig) * 100) if orig and preco and orig > preco else 0
    link = p.get("affiliate_url") or p.get("product_url") or ""
    linhas = [f"{emoji} {_headline_promo_externo(p)}", "", nome, ""]
    if preco:
        linhas.append(f"De {_format_brl(orig)} por {_format_brl(preco)} 🔥 {desc}% OFF" if desc >= 10 else f"Preço de hoje: {_format_brl(preco)}")
    else:
        linhas.append("Confira o preço atualizado no link 👇")
    linhas += ["", f"Origem: {fonte}", "Link na bio ou direto no grupo VIP 👆", link[:120], "", "#achadinhos #ofertas #brizzah #promocao"]
    return "\n".join(linhas)

# Guarda a função original para buscar links cadastrados
_brz_buscar_external_original_v7 = buscar_external_products_aprovados

def _brz_buscar_produtos_shopee_para_rotacao(limite=24):
    try:
        nicho = cfg("niche_keyword", "geral") or "geral"
        if "buscar_pool_produtos_top" not in globals(): return []
        dados = buscar_pool_produtos_top(nicho_base=nicho)
        if not dados: return []
        if "remover_repetidos_recentes" in globals():
            dados = remover_repetidos_recentes(dados, horas=12)
        if "filtrar_produtos_top" in globals():
            dados = filtrar_produtos_top(dados, nicho_alvo=nicho, limite=limite)
        produtos = []
        for item in dados[:limite]:
            p = dict(item)
            p["source"] = "shopee"
            p["status"] = "approved"
            p["is_active"] = 1
            p.setdefault("old_price", 0)
            p.setdefault("coupon", "")
            if not p.get("affiliate_url"): p["affiliate_url"] = p.get("product_url") or p.get("link") or ""
            if not p.get("product_url"): p["product_url"] = p.get("affiliate_url") or p.get("link") or ""
            produtos.append(p)
        return produtos
    except Exception as e:
        try: log("WARN", f"[V7] Shopee rotação falhou: {str(e)[:100]}")
        except Exception: pass
        return []

def buscar_external_products_aprovados(limit=60):
    import random
    externos = _brz_buscar_external_original_v7(limit=limit) or []
    shopee = _brz_buscar_produtos_shopee_para_rotacao(limite=24) or []

    grupos = {}
    for p in externos:
        grupos.setdefault(_brz_categoria_produto(p), []).append(p)
    for cat in grupos:
        random.shuffle(grupos[cat])

    ordem = ["masculino","feminino","infantil","tenis","casa","pet","tech","geral"]
    random.shuffle(ordem)

    externos_variados = []
    while any(grupos.values()) and len(externos_variados) < limit:
        for cat in ordem:
            if grupos.get(cat):
                externos_variados.append(grupos[cat].pop(0))
                if len(externos_variados) >= limit: break

    random.shuffle(shopee)

    mix = []
    i_ext = i_shop = 0
    while len(mix) < limit and (i_ext < len(externos_variados) or i_shop < len(shopee)):
        if i_ext < len(externos_variados):
            mix.append(externos_variados[i_ext]); i_ext += 1
        if i_shop < len(shopee):
            mix.append(shopee[i_shop]); i_shop += 1

    return mix[:limit]

# ============================================================
# FIM BRIZZAH V7
# ============================================================



# BRIZZAH V9 FINAL — ANTI-DUPLICIDADE + ROTACAO BANCO + SHOPEE + CTA
# Objetivo:
# - Impedir 4 posts seguidos quando a agenda está em 30/30 minutos
# - Usar links do banco de dados: Mercado Livre, Netshoes, Amazon etc.
# - Manter Shopee na rotação, mas sem dominar todos os posts
# - Corrigir erro "no such column: notes"
# - Manter CTA do grupo WhatsApp no final das legendas

LINK_GRUPO_BRIZZAH = "https://chat.whatsapp.com/Fb6kF0NXlwi8CzIVoPHMDr?mode=gi_t"

def _brz_v9_log(msg):
    try:
        log("INFO", msg)
    except Exception:
        try:
            print(msg)
        except Exception:
            pass

def _brz_v9_ensure_external_columns():
    """Garante colunas usadas pelo filtro/rotação sem quebrar bancos antigos."""
    try:
        with get_db() as c:
            cols = [r[1] for r in c.execute("PRAGMA table_info(external_products)").fetchall()]
            add = {
                "notes": "ALTER TABLE external_products ADD COLUMN notes TEXT DEFAULT ''",
                "last_posted": "ALTER TABLE external_products ADD COLUMN last_posted TEXT DEFAULT ''",
                "is_active": "ALTER TABLE external_products ADD COLUMN is_active INTEGER DEFAULT 1",
                "priority": "ALTER TABLE external_products ADD COLUMN priority INTEGER DEFAULT 0",
                "clicks": "ALTER TABLE external_products ADD COLUMN clicks INTEGER DEFAULT 0",
                "status": "ALTER TABLE external_products ADD COLUMN status TEXT DEFAULT 'approved'",
                "source": "ALTER TABLE external_products ADD COLUMN source TEXT DEFAULT ''",
                "category": "ALTER TABLE external_products ADD COLUMN category TEXT DEFAULT ''",
            }
            for col, sql in add.items():
                if col not in cols:
                    try:
                        c.execute(sql)
                    except Exception:
                        pass
    except Exception as e:
        try:
            log("WARN", f"[V9] Falha ao garantir colunas: {str(e)[:80]}")
        except Exception:
            pass

_brz_v9_ensure_external_columns()

def _brz_v9_norm(txt):
    try:
        import unicodedata, re
        s = str(txt or "").lower()
        s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s
    except Exception:
        return str(txt or "").lower().strip()

def _brz_v9_source(src):
    s = _brz_v9_norm(src)
    if "mercado" in s or "meli" in s or "livre" in s:
        return "mercadolivre"
    if "netshoes" in s:
        return "netshoes"
    if "amazon" in s:
        return "amazon"
    if "shopee" in s:
        return "shopee"
    return s or "externo"

def _brz_v9_cta_grupo_whatsapp():
    import random
    opcoes = [
        f"🚨 Quer receber ofertas assim todos os dias?\n👉 Entre no grupo VIP:\n{LINK_GRUPO_BRIZZAH}\n\n🔥 Manda pra aquele amigo que ama economizar!",
        f"💰 Só entra quem gosta de pagar barato!\n👉 Entre no grupo agora:\n{LINK_GRUPO_BRIZZAH}",
        f"🤝 Tem um amigo que ama promoção?\n👉 Manda esse grupo pra ele:\n{LINK_GRUPO_BRIZZAH}\n\n🔥 Aqui é só oferta de verdade!",
        f"⚡ Ofertas assim acabam rápido!\n👉 Entre no grupo VIP:\n{LINK_GRUPO_BRIZZAH}",
    ]
    return random.choice(opcoes)

def _corrigir_portugues_produto(texto):
    """Correção forte para nomes vindos de slug/API."""
    try:
        import re
        t = str(texto or "").strip()
        if not t:
            return ""

        t = t.replace("_", " ").replace("-", " ")
        t = re.sub(r"\b_JM\b", "", t, flags=re.I)
        t = re.sub(r"\bP\d[A-Z]\b", "", t, flags=re.I)
        t = re.sub(r"\bSKU\b", "", t, flags=re.I)
        t = re.sub(r"\s+", " ", t).strip()

        regras = [
            (r"\btnis\b", "tênis"),
            (r"\btenis\b", "tênis"),
            (r"\bolympikus\b", "Olympikus"),
            (r"\bversa\b", "Versa"),
            (r"\badidas\b", "Adidas"),
            (r"\bpuma\b", "Puma"),
            (r"\bnike\b", "Nike"),
            (r"\bfila\b", "Fila"),
            (r"\bmizuno\b", "Mizuno"),
            (r"\bjbl\b", "JBL"),
            (r"\bxiaomi\b", "Xiaomi"),
            (r"\balgodo\b", "algodão"),
            (r"\balgodao\b", "algodão"),
            (r"\b100\s*algod[oa]\b", "100% algodão"),
            (r"\bcalcado\b", "calçado"),
            (r"\bcalcados\b", "calçados"),
            (r"\balca\b", "alça"),
            (r"\bpeca\b", "peça"),
            (r"\bpecas\b", "peças"),
            (r"\brelogio\b", "relógio"),
            (r"\boculos\b", "óculos"),
            (r"\bpromocao\b", "promoção"),
            (r"\bpreco\b", "preço"),
            (r"\bversao\b", "versão"),
            (r"\bconfortavel\b", "confortável"),
            (r"\beletronico\b", "eletrônico"),
            (r"\beletronicos\b", "eletrônicos"),
            (r"\bdecoracao\b", "decoração"),
            (r"\borganizacao\b", "organização"),
            (r"\butilidades domesticas\b", "utilidades domésticas"),
            (r"\bcaes\b", "cães"),
            (r"\bces\b", "cães"),
            (r"\bcao\b", "cão"),
            (r"\baco\b", "aço"),
            (r"\badnet\b", "Adnet"),
            (r"\bessentials\b", "Essentials"),
            (r"\b3\s*stripes\b", "3-Stripes"),
            (r"\boriginal\s+nf\b", "original"),
        ]
        for pat, repl in regras:
            t = re.sub(pat, repl, t, flags=re.I)

        t = re.sub(r"\b30\s+40\s+50\s*cm\b", "30, 40 ou 50 cm", t, flags=re.I)
        t = re.sub(r"\bcães e gatos cama luxo\b", "para cães e gatos", t, flags=re.I)
        t = re.sub(r"\bcães e gatos\b", "para cães e gatos", t, flags=re.I)
        t = re.sub(r"\s+", " ", t).strip()
        return (t[0].upper() + t[1:]) if t else t
    except Exception:
        return str(texto or "").strip()

def _nome_profissional_produto(nome):
    n = _brz_v9_norm(nome)
    corrigido = _corrigir_portugues_produto(nome)

    if "tapete" in n:
        return corrigido
    if "tnis" in n or "tenis" in n:
        if "olympikus" in n and "versa" in n and "infantil" in n:
            return "Tênis Olympikus Versa infantil confortável"
        if "olympikus" in n:
            return "Tênis Olympikus confortável para o dia a dia"
        if "adidas" in n:
            return "Tênis Adidas confortável com preço de oportunidade"
        if "nike" in n:
            return "Tênis Nike confortável com preço de oportunidade"
        return corrigido
    if "toalha" in n:
        return "Jogo de toalhas de banho 100% algodão felpudas e macias"
    if "camiseta" in n and "adidas" in n and "infantil" in n:
        return "Camiseta Adidas Essentials infantil"
    if "camiseta" in n and "adidas" in n:
        return "Camiseta Adidas Essentials 3-Stripes em algodão"
    if "meia" in n and "puma" in n:
        return "Kit com 3 meias Puma infantil cano longo em algodão"
    if "chinelo" in n and "adidas" in n:
        return "Chinelo Adidas Flexmove preto confortável original"
    if "espelho" in n and "adnet" in n:
        return "Espelho Adnet redondo 30, 40 ou 50 cm com alça em couro decorativo"
    if ("caminha" in n or "cama pet" in n) and ("pet" in n or "gato" in n or "cao" in n or "caes" in n):
        return "Caminha pet redonda sherpa peludinha para cães e gatos"
    return corrigido

def _headline_promo_externo(p):
    nome = _nome_profissional_produto(p.get("name") or "")
    n = _brz_v9_norm(nome)

    if "tapete" in n or "toalha" in n or "espelho" in n or "casa" in n:
        return "ACHADO PRA CASA COM PREÇO QUE VALE A PENA"
    if "tenis" in n:
        return "TÊNIS CONFORTÁVEL COM PREÇO DE OPORTUNIDADE"
    if "camiseta" in n or "camisa" in n:
        return "CAMISETA BOA PRA USAR MUITO PAGANDO POUCO"
    if "chinelo" in n:
        return "CONFORTO NO DIA A DIA SEM PAGAR CARO"
    if "pet" in n or "caminha" in n or "gato" in n or "cachorro" in n or "cao" in n:
        return "CONFORTO PRO SEU PET COM PREÇO BAIXO"
    if "meia" in n:
        return "BÁSICO QUE TODO MUNDO USA COM PREÇO BOM"
    if "fone" in n or "bluetooth" in n or "jbl" in n:
        return "ACHADO TECH COM PREÇO BOM"
    return "OFERTA BOA PRA APROVEITAR HOJE"

def _brz_v9_cooldown_reservar(segundos=1740):
    """
    Reserva global por banco. Evita múltiplas threads/workers dispararem 2, 3 ou 4 posts juntos.
    1740s = 29 min. Mantém margem para agenda de 30/30.
    """
    try:
        now = int(time.time())
        with get_db() as c:
            c.execute("BEGIN IMMEDIATE")
            r = c.execute("SELECT value FROM config WHERE key='_brz_v9_last_cycle_ts'").fetchone()
            last = int((r["value"] if r else "0") or 0)
            if last and (now - last) < segundos:
                restante = segundos - (now - last)
                c.execute("COMMIT")
                try:
                    log("WARN", f"[V9] Ciclo bloqueado por cooldown: aguarde {restante}s")
                except Exception:
                    pass
                return False
            c.execute("INSERT OR REPLACE INTO config(key,value) VALUES('_brz_v9_last_cycle_ts',?)", (str(now),))
            c.execute("COMMIT")
            return True
    except Exception as e:
        try:
            log("WARN", f"[V9] Falha no cooldown, permitindo ciclo: {str(e)[:80]}")
        except Exception:
            pass
        return True

def _brz_v9_cooldown_liberar_se_falhou(resultado):
    if resultado and resultado > 0:
        return
    try:
        with get_db() as c:
            c.execute("INSERT OR REPLACE INTO config(key,value) VALUES('_brz_v9_last_cycle_ts','0')")
    except Exception:
        pass

def _brz_v9_rotacao_atual():
    """
    Rotação balanceada:
    1) Mercado Livre do banco
    2) Netshoes do banco
    3) Shopee automática
    4) Amazon/externos do banco
    5) Shopee automática
    Repete.
    """
    seq = ["mercadolivre", "netshoes", "shopee", "amazon", "externo", "shopee"]
    try:
        idx = int(cfg("_brz_v9_rotacao_idx", "0") or 0)
    except Exception:
        idx = 0
    return seq[idx % len(seq)]

def _brz_v9_rotacao_avancar():
    try:
        idx = int(cfg("_brz_v9_rotacao_idx", "0") or 0)
        cfg_set("_brz_v9_rotacao_idx", str(idx + 1))
    except Exception:
        pass

def _brz_proxima_ordem_fontes():
    atual = _brz_v9_rotacao_atual()
    if atual == "shopee":
        return ["shopee", "mercadolivre", "netshoes", "amazon", "externo"]
    return [atual, "mercadolivre", "netshoes", "amazon", "externo", "shopee"]

def buscar_external_products_aprovados(limit=30):
    """
    Prioriza links já cadastrados no banco, por fonte da vez.
    Se a fonte da vez for Shopee, retorna [] para o ciclo cair na busca Shopee.
    """
    import random
    _brz_v9_ensure_external_columns()
    fonte = _brz_v9_rotacao_atual()

    if fonte == "shopee":
        try:
            log("INFO", "[V9] Rodízio: vez da Shopee automática.")
        except Exception:
            pass
        return []

    try:
        hoje = _agora_brasil().date().isoformat() if "_agora_brasil" in globals() else datetime.now().date().isoformat()
    except Exception:
        hoje = datetime.now().date().isoformat()

    try:
        with get_db() as c:
            rows = c.execute("""
                SELECT * FROM external_products
                WHERE status='approved'
                  AND is_active=1
                  AND (last_posted IS NULL OR last_posted='' OR last_posted NOT LIKE ?)
                ORDER BY RANDOM()
                LIMIT 300
            """, (hoje + "%",)).fetchall()
        produtos = [dict(r) for r in rows]
    except Exception as e:
        try:
            log("WARN", f"[V9] Falha ao buscar externos: {str(e)[:80]}")
        except Exception:
            pass
        return []

    if not produtos:
        try:
            with get_db() as c:
                rows = c.execute("""
                    SELECT * FROM external_products
                    WHERE status='approved' AND is_active=1
                    ORDER BY RANDOM()
                    LIMIT 300
                """).fetchall()
            produtos = [dict(r) for r in rows]
        except Exception:
            produtos = []

    validos = []
    for p in produtos:
        try:
            p["source"] = _brz_v9_source(p.get("source"))
            if "produto_externo_profissional_valido" in globals():
                ok, motivo = produto_externo_profissional_valido(p)
                if not ok:
                    continue
            p["old_price"] = _brz_preco_antigo_confiavel(p.get("price"), p.get("old_price"), p.get("source"))
            validos.append(p)
        except Exception:
            validos.append(p)

    # 1) tenta fonte da vez
    candidatos = [p for p in validos if _brz_v9_source(p.get("source")) == fonte]

    # 2) se não tiver, usa qualquer link de banco que não seja Shopee
    if not candidatos:
        candidatos = [p for p in validos if _brz_v9_source(p.get("source")) != "shopee"]

    random.shuffle(candidatos)
    if candidatos:
        try:
            log("INFO", f"[V9] Rodízio banco: fonte={fonte} | candidatos={len(candidatos)}")
        except Exception:
            pass
    return candidatos[:max(1, min(limit, 3))]

def _caption_wa_externo(p):
    emoji, fonte = _fonte_emoji_externo(p.get("source"))
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p))[:115]
    preco = _safe_float(p.get("price"), 0)
    orig = _safe_float(p.get("old_price"), 0)
    orig = _brz_preco_antigo_confiavel(preco, orig, p.get("source"))
    desc = int((1 - preco / orig) * 100) if orig and preco and orig > preco else 0
    link = p.get("affiliate_url") or p.get("product_url") or ""
    cupom = (p.get("coupon") or "").strip()
    installments = (p.get("installments") or "").strip()

    linhas = [f"*{_headline_promo_externo(p)}*", "", nome, ""]
    if preco:
        if orig and orig > preco:
            linhas.append(f"de {_format_brl(orig)} por *{_format_brl(preco)}* 👊")
            if desc >= 10:
                linhas.append(f"🔥 {desc}% OFF")
        else:
            linhas.append(f"por *{_format_brl(preco)}* 👊")
        if installments:
            linhas.append(f"💳 {installments}")
    else:
        linhas.append("💰 Confira o preço atualizado no link 👇")

    linhas.append("🔥 Oferta com desconto no anúncio")
    linhas.append("⚠️ Preço pode mudar a qualquer momento")

    if cupom:
        linhas.append(f"Cupom: *{cupom}* ⚠️")

    linhas += [
        "",
        f"{emoji} Origem: *{fonte}*",
        f"🔗 {link}",
        "",
        _brz_v9_cta_grupo_whatsapp(),
        "",
        "_Brizzah | Achados inteligentes_ 🔥",
    ]
    return "\n".join(linhas)

def _gerar_caption_wa(produto):
    """Legenda WhatsApp para Shopee/automáticos, mantendo CTA do grupo."""
    nome = _nome_profissional_produto(produto.get("name",""))[:115]
    preco = _safe_float(produto.get("price", 0), 0)
    orig = _brz_preco_antigo_confiavel(preco, produto.get("original_price",0) or produto.get("old_price",0), produto.get("source"))
    desc = int((1 - preco / orig) * 100) if orig and preco and orig > preco else 0
    link = produto.get("affiliate_url") or produto.get("product_url") or produto.get("link") or ""
    sold = int(produto.get("sold",0) or 0)
    stars = float(produto.get("rating",0) or 0)

    try:
        _e, _n = _fonte_emoji(produto)
    except Exception:
        _e, _n = "🟠", "Shopee"

    p = [f"{_e} *{_headline_promo_externo({'name': nome, 'source': produto.get('source','shopee')})}*", "", nome, ""]
    if desc >= 10 and orig:
        p.append(f"de {_format_brl(orig)} por *{_format_brl(preco)}* 🔥 *{desc}% OFF*")
    elif preco:
        p.append(f"💰 *{_format_brl(preco)}*")
    else:
        p.append("💰 Confira o preço atualizado no link 👇")

    if stars > 0:
        p.append(f"⭐ {stars:.1f}/5")
    if sold > 0:
        p.append(f"🛒 {sold:,}+ vendidos".replace(",", "."))

    try:
        p.append(f"💬 _{adicionar_opiniao(produto)}_")
    except Exception:
        pass

    p.append("🔥 Oferta com desconto no anúncio")
    p.append("⚠️ Preço pode mudar a qualquer momento")
    p.append("")
    p.append(f"🔗 {link}")
    p.append("")
    p.append(_brz_v9_cta_grupo_whatsapp())
    p.append("")
    p.append("_Brizzah | Achados inteligentes_ 🔥")
    return "\n".join(p)

# Wrapper final do ciclo: força 1 produto por ciclo, aplica cooldown global e avança rotação.
try:
    _brz_v9_executar_ciclo_original = executar_ciclo
except Exception:
    _brz_v9_executar_ciclo_original = None

def executar_ciclo():
    if cfg("products_per_cycle", "1") != "1":
        try:
            cfg_set("products_per_cycle", "1")
        except Exception:
            pass

    if not _brz_v9_cooldown_reservar(segundos=1740):
        return 0

    resultado = 0
    try:
        if _brz_v9_executar_ciclo_original:
            resultado = _brz_v9_executar_ciclo_original()
        else:
            resultado = 0
        if resultado and resultado > 0:
            _brz_v9_rotacao_avancar()
            try:
                log("INFO", f"[V9] Ciclo concluído: {resultado} post | próxima rotação avançada")
            except Exception:
                pass
        else:
            _brz_v9_cooldown_liberar_se_falhou(resultado)
        return resultado
    except Exception as e:
        _brz_v9_cooldown_liberar_se_falhou(0)
        try:
            log("ERROR", f"[V9] Erro no ciclo protegido: {str(e)[:100]}")
        except Exception:
            pass
        raise


# ============================================================
# BRIZZAH V10 PRO — ROTACAO HUMANA + ANTI-DUPLICIDADE FORTE
# Aplicado por atualização ChatGPT em 2026-05-06
# Mantém painel/WhatsApp/Instagram atuais e sobrepõe apenas regras finais.
# ============================================================

import hashlib as _brz_hashlib
import random as _brz_random

BRIZZAH_V10_COOLDOWN_SEG = int(os.environ.get("BRIZZAH_COOLDOWN_SEG", "1740") or "1740")  # 29 min
BRIZZAH_V10_REPOST_HORAS = int(os.environ.get("BRIZZAH_REPOST_HORAS", "72") or "72")
BRIZZAH_V10_EXTERNAL_LIMIT = int(os.environ.get("BRIZZAH_EXTERNAL_LIMIT", "3") or "3")


def _brz_v10_norm(txt):
    try:
        s = str(txt or "").lower()
        s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
        s = re.sub(r"[^a-z0-9\s]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s
    except Exception:
        return str(txt or "").lower().strip()


def _brz_v10_hash_produto(p):
    base = "|".join([
        _brz_v10_norm(p.get("affiliate_url") or p.get("product_url") or p.get("link") or ""),
        _brz_v10_norm(p.get("name") or "")[:80],
        str(_safe_float(p.get("price"), 0)),
    ])
    return _brz_hashlib.sha1(base.encode("utf-8", errors="ignore")).hexdigest()


def _brz_v10_source(src):
    s = _brz_v10_norm(src)
    if "mercado" in s or s in ("ml", "meli", "mercadolivre"):
        return "mercadolivre"
    if "netshoes" in s:
        return "netshoes"
    if "amazon" in s:
        return "amazon"
    if "shopee" in s:
        return "shopee"
    if "nike" in s:
        return "nike"
    return s or "externo"


def _brz_v10_categoria(p):
    n = _brz_v10_norm(" ".join([str(p.get("name") or ""), str(p.get("category") or ""), str(p.get("brand") or "")]))
    # Ordem importa: perfume antes de pet, tech antes de casa etc.
    if any(x in n for x in ["perfume", "colonia", "deo parfum", "eau de parfum", "lattafa", "egeo", "granado", "boticario", "fragrancia"]): return "perfume"
    if any(x in n for x in ["creatina", "whey", "suplemento", "protein", "coqueteleira", "pre treino", "termica feminina", "dryfit", "academia", "treino", "legging", "shorts academia"]): return "fitness"
    if any(x in n for x in ["tenis", "sneaker", "calcado", "sapato", "sandalia", "chinelo", "bota"]): return "calcados"
    if any(x in n for x in ["camiseta", "camisa", "short", "bermuda", "cueca", "meia", "mochila", "bolsa", "calca", "jaqueta", "polo"]): return "moda"
    if any(x in n for x in ["fone", "bluetooth", "jbl", "xiaomi", "g shock", "casio", "relogio", "smartwatch", "boombox", "caixa de som", "balanca digital"]): return "tech"
    if any(x in n for x in ["casa", "cozinha", "airfryer", "panela", "toalha", "espelho", "organizador", "decoracao", "tapete"]): return "casa"
    if any(x in n for x in ["pet", "cachorro", "cao", "caes", "gato", "racao", "coleira", "caminha"]): return "pet"
    if any(x in n for x in ["infantil", "crianca", "menino", "menina", "bebe", "kids"]): return "infantil"
    return "geral"


def _brz_v10_produto_ruim(p):
    n = _brz_v10_norm(p.get("name") or "")
    link = (p.get("affiliate_url") or p.get("product_url") or p.get("link") or "").strip()
    img = (p.get("image_url") or "").strip()
    preco = _safe_float(p.get("price"), 0)
    bloqueios = [
        "pelicula", "kit pelicula", "10 pecas", "20 pecas", "50 pecas", "100 pecas",
        "atacado", "lote", "relogio inteligente infantil", "sem marca", "produto importado",
        "sortido", "aleatorio", "brinde", "usado", "recondicionado"
    ]
    if not link: return True
    if not img and _brz_v10_source(p.get("source")) not in ("mercadolivre", "amazon", "netshoes", "nike"): return True
    if len(n) < 8: return True
    if any(b in n for b in bloqueios): return True
    if preco and preco < 9 and _brz_v10_categoria(p) not in ("moda", "casa"): return True
    if preco and preco > 5000: return True
    return False


def _brz_v10_score(p):
    n = _brz_v10_norm(p.get("name") or "")
    fonte = _brz_v10_source(p.get("source"))
    cat = _brz_v10_categoria(p)
    preco = _safe_float(p.get("price"), 0)
    old = _safe_float(p.get("old_price"), 0)
    clicks = _safe_int(p.get("clicks"), 0)
    prioridade = _safe_int(p.get("priority"), 0)
    s = 10 + prioridade + min(clicks, 8)
    if fonte in ("mercadolivre", "netshoes", "amazon", "nike"): s += 12
    if fonte == "shopee": s += 4
    if cat in ("moda", "calcados", "perfume", "fitness", "tech"): s += 12
    if cat == "casa": s += 6
    marcas = ["nike", "adidas", "puma", "reebok", "fila", "olympikus", "mizuno", "new balance", "casio", "g shock", "jbl", "philips", "lattafa", "granado", "boticario", "universal nutrition"]
    if any(m in n for m in marcas): s += 14
    if preco:
        if 20 <= preco <= 199: s += 10
        elif 200 <= preco <= 499: s += 6
        elif preco <= 19: s += 3
    if old and preco and old > preco:
        off = int((1 - preco / old) * 100)
        if off >= 50: s += 10
        elif off >= 30: s += 7
        elif off >= 15: s += 4
    return s


def _brz_v10_preco_linha(preco, orig=0, cupom="", installments=""):
    preco = _safe_float(preco, 0)
    orig = _safe_float(orig, 0)
    linhas = []
    if preco:
        if orig and orig > preco:
            linhas.append(f"de {_format_brl(orig)} por *{_format_brl(preco)}* 👊")
        else:
            linhas.append(f"por *{_format_brl(preco)}* 👊")
    else:
        linhas.append("Confira o preço atualizado no link 👇")
    if installments:
        linhas.append(f"💳 {installments}")
    if cupom:
        linhas.append(f"Cupom: *{cupom.strip().upper()}* ⚠️")
    return linhas


def _headline_promo_externo(p):
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p) if isinstance(p, dict) else str(p))
    n = _brz_v10_norm(nome)
    cat = _brz_v10_categoria(p if isinstance(p, dict) else {"name": nome})

    if "nike" in n and "mochila" in n: return "A NIKE SABE FAZER MOCHILA"
    if "nike" in n and ("camiseta" in n or "camisa" in n): return "NIKE MINIMALISTA"
    if "nike" in n and "tenis" in n: return "MENOR VALOR NESSE NIKE"
    if "g shock" in n or ("casio" in n and "relogio" in n): return "G-SHOCK DIGITAL É LINDO DEMAIS"
    if "boombox" in n or "philips" in n: return "CORREEEI PREÇÃO NESSA BOOMBOX DA PHILIPS"
    if "cueca" in n: return "CUECA NUNCA É DEMAIS"
    if "short" in n and ("femin" in n or "bolso" in n): return "SHORTINHO COM BOLSO LATERAL PRA ELAS 👸"
    if "short" in n or "bermuda" in n: return "SHORTS VERSÁTIL PRO DIA A DIA"
    if "camisa termica" in n or "dryfit" in n: return "FRESQUINHA PRO TREINO DELAS 🧘"
    if "creatina" in n: return "AGORA O SHAPE VEM"
    if cat == "perfume":
        if "arabe" in n or "lattafa" in n: return "PERFUMAÇO ÁRABE COM PREÇO BOM"
        return "É PERFUME PRO ANO INTEIRO"
    if cat == "calcados": return "TÊNIS PRA SURRAR NO DIA A DIA"
    if cat == "fitness": return "ACHADO FITNESS PRA APROVEITAR"
    if cat == "tech": return "TECH BOA COM PREÇO DE GARIMPO"
    if cat == "moda": return "ACHADO DE MODA PRA USAR MUITO"
    if cat == "casa": return "ACHADO PRA CASA COM PREÇO BOM"
    return "ACHADO BRIZZAH COM PREÇO BOM"


def _brz_v10_cta_grupo_whatsapp():
    link = globals().get("LINK_GRUPO_BRIZZAH", "") or cfg("link_grupo_brizzah", "") or cfg("whatsapp_invite_link", "")
    if link:
        return f"🔥 Quer receber ofertas assim todos os dias?\n👉 Entre no grupo VIP:\n{link}"
    return "🔥 Entre no grupo VIP Brizzah e receba os achados antes de acabar."


def _caption_wa_externo(p):
    emoji, fonte = _fonte_emoji_externo(p.get("source"))
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p))[:118]
    preco = _safe_float(p.get("price"), 0)
    orig = _brz_preco_antigo_confiavel(preco, p.get("old_price"), p.get("source"))
    link = p.get("affiliate_url") or p.get("product_url") or p.get("link") or ""
    cupom = (p.get("coupon") or "").strip()
    installments = (p.get("installments") or "").strip()

    linhas = [f"*{_headline_promo_externo(p)}*", "", nome, ""]
    linhas.extend(_brz_v10_preco_linha(preco, orig, cupom, installments))
    linhas += ["", f"Loja oficial {fonte}:", link, "", _brz_v10_cta_grupo_whatsapp()]
    return "\n".join([x for x in linhas if x is not None])


def _caption_ig_externo(p):
    emoji, fonte = _fonte_emoji_externo(p.get("source"))
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p))[:115]
    preco = _safe_float(p.get("price"), 0)
    orig = _brz_preco_antigo_confiavel(preco, p.get("old_price"), p.get("source"))
    link = p.get("affiliate_url") or p.get("product_url") or p.get("link") or ""
    linhas = [f"{emoji} {_headline_promo_externo(p)}", "", nome, ""]
    if preco:
        if orig and orig > preco:
            linhas.append(f"De {_format_brl(orig)} por {_format_brl(preco)} 👊")
        else:
            linhas.append(f"Preço de hoje: {_format_brl(preco)}")
    else:
        linhas.append("Confira o preço atualizado no link")
    linhas += ["", f"Origem: {fonte}", "Link direto no grupo VIP 👆", link[:120], "", "#achadinhos #ofertas #brizzah #promocao #economize"]
    return "\n".join(linhas)


def _gerar_caption_wa(produto):
    """Legenda WhatsApp final também para Shopee/automáticos."""
    p = dict(produto or {})
    p.setdefault("source", p.get("source") or "shopee")
    p.setdefault("old_price", p.get("original_price", 0) or p.get("old_price", 0))
    return _caption_wa_externo(p)


def _brz_v10_ensure_tables():
    try:
        with get_db() as c:
            c.execute("CREATE TABLE IF NOT EXISTS posted_fingerprints (fingerprint TEXT PRIMARY KEY, source TEXT DEFAULT '', category TEXT DEFAULT '', product_name TEXT DEFAULT '', created_at TEXT DEFAULT (datetime('now','localtime')))")
            try:
                c.execute("DELETE FROM posted_fingerprints WHERE created_at < datetime('now','-30 days')")
            except Exception:
                pass
    except Exception as e:
        try: log("WARN", f"[V10] Falha ensure tables: {str(e)[:80]}")
        except Exception: pass


def _brz_v10_ja_postado_hash(p, horas=None):
    horas = int(horas or BRIZZAH_V10_REPOST_HORAS)
    fp = _brz_v10_hash_produto(p)
    try:
        with get_db() as c:
            r = c.execute("SELECT created_at FROM posted_fingerprints WHERE fingerprint=? AND created_at >= datetime('now', ?)", (fp, f"-{horas} hours")).fetchone()
            return bool(r)
    except Exception:
        return False


def _brz_v10_marcar_postado_hash(p):
    fp = _brz_v10_hash_produto(p)
    try:
        with get_db() as c:
            c.execute("INSERT OR REPLACE INTO posted_fingerprints(fingerprint,source,category,product_name,created_at) VALUES(?,?,?,?,datetime('now','localtime'))", (fp, _brz_v10_source(p.get("source")), _brz_v10_categoria(p), (p.get("name") or "")[:140]))
            c.execute("INSERT OR REPLACE INTO config(key,value) VALUES('_brz_v10_last_source',?)", (_brz_v10_source(p.get("source")),))
            c.execute("INSERT OR REPLACE INTO config(key,value) VALUES('_brz_v10_last_category',?)", (_brz_v10_categoria(p),))
    except Exception:
        pass


def _brz_v10_cooldown_reservar(segundos=None):
    segundos = int(segundos or BRIZZAH_V10_COOLDOWN_SEG)
    try:
        now = int(time.time())
        with get_db() as c:
            c.execute("BEGIN IMMEDIATE")
            r = c.execute("SELECT value FROM config WHERE key='_brz_v10_last_cycle_ts'").fetchone()
            last = int((r["value"] if r else "0") or 0)
            if last and (now - last) < segundos:
                restante = segundos - (now - last)
                c.execute("COMMIT")
                try: log("WARN", f"[V10] Ciclo bloqueado por cooldown real: aguarde {restante}s")
                except Exception: pass
                return False
            c.execute("INSERT OR REPLACE INTO config(key,value) VALUES('_brz_v10_last_cycle_ts',?)", (str(now),))
            c.execute("COMMIT")
            return True
    except Exception as e:
        try: log("WARN", f"[V10] Falha no cooldown DB: {str(e)[:80]}")
        except Exception: pass
        return False


def _brz_v10_cooldown_liberar():
    try:
        with get_db() as c:
            c.execute("INSERT OR REPLACE INTO config(key,value) VALUES('_brz_v10_last_cycle_ts','0')")
    except Exception:
        pass


def _brz_v10_rotacao_seq():
    # Banco externo fica com prioridade. Shopee entra, mas não domina.
    return ["mercadolivre", "netshoes", "amazon", "mercadolivre", "shopee", "netshoes", "mercadolivre", "nike", "externo", "shopee"]


def _brz_v10_fonte_da_vez():
    seq = _brz_v10_rotacao_seq()
    try: idx = int(cfg("_brz_v10_rotacao_idx", "0") or 0)
    except Exception: idx = 0
    return seq[idx % len(seq)]


def _brz_v10_avancar_rotacao():
    try:
        idx = int(cfg("_brz_v10_rotacao_idx", "0") or 0)
        cfg_set("_brz_v10_rotacao_idx", str(idx + 1))
    except Exception:
        pass


def _brz_proxima_ordem_fontes():
    fonte = _brz_v10_fonte_da_vez()
    if fonte == "shopee":
        return ["shopee", "mercadolivre", "netshoes", "amazon", "nike", "externo"]
    return [fonte, "mercadolivre", "netshoes", "amazon", "nike", "externo", "shopee"]


def buscar_external_products_aprovados(limit=30):
    """Busca externa V10: prioriza banco manual, balanceia fonte/categoria e evita repetidos."""
    _brz_v10_ensure_tables()
    try:
        if "_brz_v9_ensure_external_columns" in globals():
            _brz_v9_ensure_external_columns()
    except Exception:
        pass

    fonte_vez = _brz_v10_fonte_da_vez()
    if fonte_vez == "shopee":
        try: log("INFO", "[V10] Rodízio: vez da Shopee automática")
        except Exception: pass
        return []

    try:
        with get_db() as c:
            rows = c.execute("""
                SELECT * FROM external_products
                WHERE status='approved' AND is_active=1
                ORDER BY COALESCE(priority,0) DESC, RANDOM()
                LIMIT 500
            """).fetchall()
        produtos = [dict(r) for r in rows]
    except Exception as e:
        try: log("WARN", f"[V10] Falha ao buscar externos: {str(e)[:80]}")
        except Exception: pass
        return []

    last_source = cfg("_brz_v10_last_source", "")
    last_cat = cfg("_brz_v10_last_category", "")
    validos = []
    for p in produtos:
        try:
            p["source"] = _brz_v10_source(p.get("source"))
            p["old_price"] = _brz_preco_antigo_confiavel(p.get("price"), p.get("old_price"), p.get("source"))
            if _brz_v10_produto_ruim(p):
                continue
            if p.get("id") and ja_postado_recentemente_externo(p.get("id"), horas=BRIZZAH_V10_REPOST_HORAS):
                continue
            if _brz_v10_ja_postado_hash(p, horas=BRIZZAH_V10_REPOST_HORAS):
                continue
            validos.append(p)
        except Exception:
            continue

    def pick(pool):
        pool = list(pool or [])
        if not pool: return []
        # evita repetir mesma categoria/fonte se houver alternativa
        alt = [p for p in pool if _brz_v10_categoria(p) != last_cat or _brz_v10_source(p.get("source")) != last_source]
        pool = alt or pool
        pool.sort(key=_brz_v10_score, reverse=True)
        return pool[:max(1, min(limit, BRIZZAH_V10_EXTERNAL_LIMIT))]

    candidatos = pick([p for p in validos if _brz_v10_source(p.get("source")) == fonte_vez])
    if not candidatos:
        candidatos = pick([p for p in validos if _brz_v10_source(p.get("source")) != "shopee"])

    try: log("INFO", f"[V10] Banco manual | fonte_da_vez={fonte_vez} | validos={len(validos)} | escolhidos={len(candidatos)}")
    except Exception: pass
    return candidatos


try:
    _brz_v10_executar_ciclo_original = executar_ciclo
except Exception:
    _brz_v10_executar_ciclo_original = None


def executar_ciclo():
    """Wrapper final: 1 post por ciclo, cooldown DB e rotação equilibrada."""
    _brz_v10_ensure_tables()
    try:
        if cfg("products_per_cycle", "1") != "1":
            cfg_set("products_per_cycle", "1")
    except Exception:
        pass

    if not _brz_v10_cooldown_reservar():
        return 0

    resultado = 0
    try:
        if _brz_v10_executar_ciclo_original:
            resultado = _brz_v10_executar_ciclo_original()
        resultado = int(resultado or 0)
        if resultado > 0:
            _brz_v10_avancar_rotacao()
            try: log("INFO", f"[V10] Ciclo OK: {resultado} post | próxima fonte={_brz_v10_fonte_da_vez()}")
            except Exception: pass
        else:
            _brz_v10_cooldown_liberar()
        return resultado
    except Exception as e:
        _brz_v10_cooldown_liberar()
        try: log("ERROR", f"[V10] Erro no ciclo protegido: {str(e)[:100]}")
        except Exception: pass
        raise

# ============================================================
# FIM BRIZZAH V10 PRO
# ============================================================

# ============================================================
# BRIZZAH V11 - PRIORIDADE NETSHOES/MERCADO LIVRE + SEM PET ERRADO
# Aplicado em 2026-05-06
# Objetivo: banco externo premium SEMPRE antes da Shopee automática.
# ============================================================

BRIZZAH_V11_EXTERNAL_LIMIT = int(os.environ.get("BRIZZAH_EXTERNAL_LIMIT", "20") or "20")


def _brz_v10_categoria(p):
    """Categoria V11: nunca usa PET como fallback; PET só com termo realmente pet."""
    n = _brz_v10_norm(" ".join([
        str((p or {}).get("name") or ""),
        str((p or {}).get("category") or ""),
        str((p or {}).get("brand") or "")
    ]))

    # Termos ambíguos: colar = acessório/moda, coleira = pet.
    if any(x in n for x in ["perfume", "colonia", "deo parfum", "eau de parfum", "lattafa", "egeo", "granado", "boticario", "fragrancia", "parfum"]):
        return "perfume"
    if any(x in n for x in ["creatina", "whey", "suplemento", "protein", "coqueteleira", "pre treino", "pos treino", "dryfit", "academia", "treino", "legging", "shorts academia", "camisa termica"]):
        return "fitness"
    if any(x in n for x in ["tenis", "sneaker", "calcado", "sapato", "sandalia", "chinelo", "bota"]):
        return "calcados"
    if any(x in n for x in ["camiseta", "camisa", "short", "bermuda", "cueca", "boxer", "meia", "mochila", "bolsa", "calca", "jaqueta", "polo", "colar", "corrente", "pulseira", "brinco", "anel"]):
        return "moda"
    if any(x in n for x in ["fone", "bluetooth", "jbl", "xiaomi", "g shock", "casio", "relogio", "smartwatch", "boombox", "caixa de som", "balanca digital", "power bank", "carregador"]):
        return "tech"
    if any(x in n for x in ["casa", "cozinha", "airfryer", "panela", "toalha", "espelho", "organizador", "decoracao", "tapete", "cama", "mesa"]):
        return "casa"

    # PET só quando for claro. NÃO inclui "colar".
    pet_claro = [" pet ", "cachorro", "cao", "caes", "gato", "racao", "coleira", "caminha pet", "arranhador", "areia sanitaria"]
    n_pad = f" {n} "
    if any(x in n_pad for x in pet_claro):
        return "pet"
    if any(x in n for x in ["infantil", "crianca", "menino", "menina", "bebe", "kids"]):
        return "infantil"
    return "geral"


def _headline_natural_produto(nome):
    """Headline neutra para produto genérico. PET nunca é fallback."""
    n = _brz_v10_norm(nome)
    cat = _brz_v10_categoria({"name": nome})
    if "toalha" in n:
        return "TOALHAS COM PREÇO QUE VALE A PENA"
    if "camiseta" in n or "camisa" in n:
        return "CAMISETA BOA PRA USAR MUITO"
    if "chinelo" in n:
        return "CONFORTO NO DIA A DIA SEM PAGAR CARO"
    if "tenis" in n:
        return "TÊNIS CONFORTÁVEL COM PREÇO DE OPORTUNIDADE"
    if cat == "pet":
        return "ACHADO PRO SEU PET COM PREÇO BOM"
    if "espelho" in n or "adnet" in n:
        return "UM TOQUE BONITO PRA CASA GASTANDO POUCO"
    if "meia" in n:
        return "BÁSICO QUE TODO MUNDO USA COM PREÇO BOM"
    if "fone" in n or "bluetooth" in n or "jbl" in n:
        return "ACHADO TECH COM PREÇO BOM"
    return "ACHADO BRIZZAH COM PREÇO BOM"


def _headline_promo_externo(p):
    """Headline V11: remove PET errado e usa chamadas humanas/neutras."""
    nome = _nome_profissional_produto(_nome_apresentavel_externo(p) if isinstance(p, dict) else str(p))
    n = _brz_v10_norm(nome)
    cat = _brz_v10_categoria(p if isinstance(p, dict) else {"name": nome})

    if "nike" in n and "mochila" in n: return "A NIKE SABE FAZER MOCHILA"
    if "nike" in n and ("camiseta" in n or "camisa" in n): return "NIKE MINIMALISTA"
    if "nike" in n and "tenis" in n: return "MENOR VALOR NESSE NIKE"
    if "adidas" in n and "tenis" in n: return "ADIDAS PRA USAR MUITO"
    if "g shock" in n or ("casio" in n and "relogio" in n): return "G-SHOCK DIGITAL É LINDO DEMAIS"
    if "boombox" in n or "philips" in n: return "CORREEEI PREÇÃO NESSA BOOMBOX DA PHILIPS"
    if "cueca" in n or "boxer" in n: return "CUECA NUNCA É DEMAIS"
    if "short" in n and ("femin" in n or "bolso" in n): return "SHORTINHO COM BOLSO LATERAL PRA ELAS 👸"
    if "short" in n or "bermuda" in n: return "SHORTS VERSÁTIL PRO DIA A DIA"
    if "camisa termica" in n or "dryfit" in n: return "FRESQUINHA PRO TREINO"
    if "creatina" in n: return "AGORA O SHAPE VEM"
    if cat == "perfume":
        if "arabe" in n or "lattafa" in n: return "PERFUMAÇO ÁRABE COM PREÇO BOM"
        return "É PERFUME PRO ANO INTEIRO"
    if cat == "calcados": return "TÊNIS PRA SURRAR NO DIA A DIA"
    if cat == "fitness": return "ACHADO FITNESS PRA APROVEITAR"
    if cat == "tech": return "TECH BOA COM PREÇO DE GARIMPO"
    if cat == "moda": return "ACHADO DE MODA PRA USAR MUITO"
    if cat == "casa": return "ACHADO PRA CASA COM PREÇO BOM"
    if cat == "pet": return "ACHADO PRO SEU PET COM PREÇO BOM"

    neutras = [
        "ACHADO BRIZZAH COM PREÇO BOM",
        "PREÇO BOM DEMAIS PRA DEIXAR PASSAR",
        "OLHA O PREÇO DISSO AQUI",
        "ACHADO PRA APROVEITAR HOJE",
        "BARATINHO QUE SURPREENDE",
    ]
    try:
        return _brz_random.choice(neutras)
    except Exception:
        return "ACHADO BRIZZAH COM PREÇO BOM"


def _brz_v10_produto_ruim(p):
    """Filtro V11: Shopee automática só entra se for produto minimamente vendável."""
    p = p or {}
    n = _brz_v10_norm(p.get("name") or "")
    fonte = _brz_v10_source(p.get("source"))
    link = (p.get("affiliate_url") or p.get("product_url") or p.get("link") or "").strip()
    img = (p.get("image_url") or "").strip()
    preco = _safe_float(p.get("price"), 0)

    bloqueios_gerais = [
        "pelicula", "kit pelicula", "atacado", "lote", "sem marca", "produto importado",
        "sortido", "aleatorio", "brinde", "usado", "recondicionado", "sacola kraft",
        "sacola papel", "embalagem", "adesivo", "etiqueta", "tag para presente", "porta cracha",
        "pecas", "100 unidades", "50 unidades", "20 unidades", "10 unidades", "kit 10 sacola",
    ]
    if not link: return True
    if len(n) < 8: return True
    if any(b in n for b in bloqueios_gerais): return True
    if preco and preco < 12: return True
    if preco and preco > 5000: return True

    # Para Shopee, filtro mais rígido para não virar grupo de bugiganga.
    if fonte == "shopee":
        boas = [
            "nike", "adidas", "puma", "reebok", "fila", "olympikus", "mizuno", "new balance",
            "jbl", "xiaomi", "philips", "casio", "g shock", "lattafa", "granado", "boticario",
            "tenis", "camiseta", "camisa", "short", "bermuda", "cueca", "mochila", "fone",
            "creatina", "whey", "perfume", "smartwatch", "airfryer", "toalha"
        ]
        if not any(x in n for x in boas):
            return True
        if not img:
            return True
    return False


def _brz_v10_score(p):
    """Score V11: Netshoes e ML dominam; Shopee fica por último."""
    p = p or {}
    n = _brz_v10_norm(p.get("name") or "")
    fonte = _brz_v10_source(p.get("source"))
    cat = _brz_v10_categoria(p)
    preco = _safe_float(p.get("price"), 0)
    old = _safe_float(p.get("old_price"), 0)
    clicks = _safe_int(p.get("clicks"), 0)
    prioridade = _safe_int(p.get("priority"), 0)

    s = 10 + prioridade + min(clicks, 10)
    if fonte == "netshoes": s += 90
    elif fonte in ("mercadolivre", "nike"): s += 80
    elif fonte == "amazon": s += 55
    elif fonte == "shopee": s -= 35
    else: s += 35

    if cat in ("calcados", "moda", "perfume", "fitness", "tech"): s += 25
    elif cat == "casa": s += 10
    elif cat == "pet": s += 5
    else: s += 3

    marcas = ["nike", "adidas", "puma", "reebok", "fila", "olympikus", "mizuno", "new balance", "casio", "g shock", "jbl", "philips", "lattafa", "granado", "boticario", "universal nutrition", "oakley", "asics"]
    if any(m in n for m in marcas): s += 30

    if preco:
        if 20 <= preco <= 199: s += 18
        elif 200 <= preco <= 499: s += 12
        elif 500 <= preco <= 999: s += 6
    if old and preco and old > preco:
        off = int((1 - preco / old) * 100)
        if off >= 50: s += 20
        elif off >= 30: s += 14
        elif off >= 15: s += 8
    if _brz_v10_produto_ruim(p):
        s -= 999
    return s


def _brz_v10_rotacao_seq():
    """Shopee fica por último e só como complemento."""
    return ["netshoes", "mercadolivre", "mercadolivre", "netshoes", "amazon", "nike", "externo", "mercadolivre", "netshoes", "shopee"]


def _brz_proxima_ordem_fontes():
    """Ordem definitiva: banco premium primeiro; Shopee só se não tiver externo bom."""
    return ["netshoes", "mercadolivre", "amazon", "nike", "externo", "shopee"]


def buscar_external_products_aprovados(limit=30):
    """V11: lê a fila premium do banco ANTES de qualquer Shopee automática."""
    try:
        _brz_v10_ensure_tables()
    except Exception:
        pass
    try:
        if "_brz_v9_ensure_external_columns" in globals():
            _brz_v9_ensure_external_columns()
    except Exception:
        pass

    try:
        with get_db() as c:
            rows = c.execute("""
                SELECT * FROM external_products
                WHERE (status='approved' OR status='active' OR status IS NULL OR status='')
                  AND (is_active=1 OR is_active IS NULL)
                ORDER BY COALESCE(priority,0) DESC, id DESC
                LIMIT 800
            """).fetchall()
        produtos = [dict(r) for r in rows]
    except Exception as e:
        try: log("WARN", f"[V11] Falha ao buscar fila premium: {str(e)[:90]}")
        except Exception: pass
        return []

    validos = []
    for p in produtos:
        try:
            p["source"] = _brz_v10_source(p.get("source"))
            p["old_price"] = _brz_preco_antigo_confiavel(p.get("price"), p.get("old_price"), p.get("source"))
            if _brz_v10_produto_ruim(p):
                continue
            if p.get("id") and ja_postado_recentemente_externo(p.get("id"), horas=BRIZZAH_V10_REPOST_HORAS):
                continue
            if _brz_v10_ja_postado_hash(p, horas=BRIZZAH_V10_REPOST_HORAS):
                continue
            validos.append(p)
        except Exception:
            continue

    # Se existir ML/Netshoes/Amazon/Nike bom, ignora Shopee da fila também.
    premium = [p for p in validos if _brz_v10_source(p.get("source")) in ("netshoes", "mercadolivre", "amazon", "nike", "externo")]
    pool = premium or validos
    pool.sort(key=_brz_v10_score, reverse=True)
    escolhidos = pool[:max(1, min(int(limit or 1), BRIZZAH_V11_EXTERNAL_LIMIT))]

    try:
        resumo = {}
        for p in escolhidos:
            resumo[_brz_v10_source(p.get("source"))] = resumo.get(_brz_v10_source(p.get("source")), 0) + 1
        log("INFO", f"[V11] Fila premium consultada | total={len(produtos)} | validos={len(validos)} | escolhidos={len(escolhidos)} | fontes={resumo}")
    except Exception:
        pass
    return escolhidos

try:
    cfg_set("_brz_v10_rotacao_idx", "0")
    log("INFO", "[V11] Atualização ativa: Netshoes/ML prioridade, Shopee por último, PET removido do fallback")
except Exception:
    pass

# ============================================================
# FIM BRIZZAH V11
# ============================================================
