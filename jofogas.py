# jofogas_pipeline.py
import os
import re
import time
import json
import random
import urllib3
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from supabase import create_client
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ----- FIGYELMEZTETÉS (SSL kikapcsolás miatt) -----
# Az oldal SSL ellenőrzésével baj van néha a gépeden — ezért letiltjuk a warningokat.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------- KONFIG (állítsd be környezeti változóban vagy cseréld ki itt) -----------------
BASE_SEARCH_TEMPLATE = "https://allas.jofogas.hu/magyarorszag/allasajanlat?pf=b&o={page}"
BASE_DOMAIN = "https://allas.jofogas.hu"

# mappák
BASE_DIR = os.path.join(os.getcwd(), "jofogas_data")
SEARCH_DIR = os.path.join(BASE_DIR, "search_pages")
JOB_DIR = os.path.join(BASE_DIR, "job_pages")

# Supabase - legyen ENV-ben, vagy írd be ide (nem ajánlott)
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TABLE_NAME = os.environ["TABLE_NAME"]

# Email beállítások - használd app-password-ot, ne a sima jelszót
EMAIL_SENDER = os.environ["EMAIL_SENDER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]
EMAIL_RECIPIENT = os.environ["EMAIL_RECIPIENT"]

# User-Agent lista (véletlenszerű, emberibb viselkedés)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0"
]

# hálózati beállítások
REQUEST_TIMEOUT = 30
RETRY_COUNT = 3
MIN_DELAY = 1.0
MAX_DELAY = 3.0

# ----------------- SEGÉDFÜGGVÉNYEK -----------------
def send_email(subject, message):
    """Összegző email küldése"""
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECIPIENT
        msg["Subject"] = subject
        msg.attach(MIMEText(message, "plain"))
        with smtplib.SMTP("smtp.gmail.com", 587, timeout=30) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print("✅ Email elküldve")
    except Exception as e:
        print(f"❌ Email hiba: {e}")

def supabase_client():
    """Supabase kliens létrehozása"""
    if not SUPABASE_URL or not SUPABASE_KEY or "YOUR_SUPABASE_KEY" in SUPABASE_KEY:
        raise RuntimeError("Állítsd be a SUPABASE_URL és SUPABASE_KEY környezeti változókat (vagy a scriptben).")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def safe_request(session, url, retries=RETRY_COUNT):
    """Requests lekérés verify=False-szal, 429 + hibák kezelése, egyszerű backoff"""
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    backoff = 1
    for attempt in range(1, retries+1):
        try:
            resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT, verify=False)
            if resp.status_code == 429:
                wait = 5 * attempt + random.random() * 3
                print(f"[429] Rate limit a {url} - várok {wait:.1f}s majd újrapróbálkozom")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.text
        except requests.exceptions.RequestException as e:
            print(f"[request] Hiba ({attempt}/{retries}) a {url} : {e}")
            if attempt < retries:
                time.sleep(backoff + random.random())
                backoff *= 2
                continue
            return None
    return None

def ensure_dirs():
    os.makedirs(SEARCH_DIR, exist_ok=True)
    os.makedirs(JOB_DIR, exist_ok=True)

# ----------------- LÉPÉS 1: TALÁLATI OLDALAK - UTOLSÓ OLDALSZÁM -----------------
def get_total_pages(session):
    """Lekéri az 1. találati oldalt és a paginationből kigyűjti az utolsó oldalszámot (o=XXX)"""
    url = BASE_SEARCH_TEMPLATE.format(page=1)
    html = safe_request(session, url)
    if not html:
        return 1, None
    # mentés debug célra
    with open(os.path.join(SEARCH_DIR, "search_page_1.html"), "w", encoding="utf-8") as f:
        f.write(html)
    soup = BeautifulSoup(html, "html.parser")
    last_link = soup.select_one("a.ad-list-pager-item-last")
    if last_link and last_link.get("href"):
        href = last_link["href"]
        # keressük az o= paramétert
        m = re.search(r"[?&]o=(\d+)", href)
        if m:
            return int(m.group(1)), html
    # ha nincs last, keressük az összes page-number elemet és vegyük a maxot
    nums = []
    for a in soup.select("a.ad-list-pager-page-number"):
        txt = a.get_text(strip=True)
        try:
            nums.append(int(txt))
        except:
            pass
    return (max(nums) if nums else 1), html

# ----------------- LÉPÉS 2: TALÁLATI OLDALAK LETÖLTÉSE -----------------
def download_search_pages(session, total_pages):
    """Letölti az összes search page-et véletlensorrendben (ha már megvan, kihagyja)"""
    pages = list(range(1, total_pages + 1))
    random.shuffle(pages)
    downloaded = []
    for p in pages:
        filename = os.path.join(SEARCH_DIR, f"search_page_{p}.html")
        if os.path.exists(filename):
            print(f"[skip] Már megvan: {filename}")
            downloaded.append(filename)
            continue
        url = BASE_SEARCH_TEMPLATE.format(page=p)
        print(f"[download] Találati oldal {p}/{total_pages} -> {url}")
        html = safe_request(session, url)
        if html:
            with open(filename, "w", encoding="utf-8") as f:
                f.write(html)
            downloaded.append(filename)
        else:
            print(f"[error] Nem sikerült letölteni: {url}")
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    return downloaded

# ----------------- LÉPÉS 3: LINKKINYERÉS A LEMENTETT TALÁLATI OLDALAKBÓL -----------------
def extract_links_from_search_pages():
    """Beolvassa a search HTML-eket és kigyűjti az állás linkeket (h3.item-title a.subject)"""
    links = set()
    files = [f for f in os.listdir(SEARCH_DIR) if f.endswith(".html")]
    for fname in files:
        path = os.path.join(SEARCH_DIR, fname)
        with open(path, encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")
        for a in soup.select("h3.item-title a.subject"):
            href = a.get("href")
            if not href:
                continue
            if href.startswith("/"):
                href = BASE_DOMAIN + href
            links.add(href)
    return sorted(links)

# ----------------- LÉPÉS 4: ÁLLÁSOLDALAK LETÖLTÉSE -----------------
def download_job_pages(session, links):
    """Letölti a job oldalak HTML-jeit job_pages mappába - visszaadja a sikeres és sikertelen listákat"""
    success = []
    failed = []
    for i, link in enumerate(links, 1):
        # fájlnév: utolsó path elem, ha ütközik, indexet teszünk elé
        last = link.rstrip("/").split("/")[-1]
        filename = os.path.join(JOB_DIR, last)
        # biztosítsuk, hogy .htm vagy .html végződés legyen
        if not filename.lower().endswith(".htm") and not filename.lower().endswith(".html"):
            filename = filename + ".html"
        if os.path.exists(filename):
            print(f"[job skip] Már megvan: {filename}")
            success.append(filename)
            continue
        print(f"[job dl {i}/{len(links)}] {link}")
        html = safe_request(session, link)
        if html:
            with open(filename, "w", encoding="utf-8") as f:
                f.write(html)
            success.append(filename)
        else:
            failed.append(link)
            print(f"[job fail] {link}")
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    return success, failed

# ----------------- LÉPÉS 5: PARSING ÁLLÁSOLDALAKBÓL -----------------
def parse_job_file(path):
    """Kiveszi a __NEXT_DATA__ JSON-ből a product objektumot és előállít egy dict-et a Supabase-hez"""
    with open(path, encoding="utf-8") as f:
        soup = BeautifulSoup(f, "html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not script_tag or not script_tag.string:
        return None
    try:
        data = json.loads(script_tag.string)
    except Exception as e:
        print(f"[parse error] JSON dekódolás sikertelen: {path} -> {e}")
        return None
    product = data.get("props", {}).get("pageProps", {}).get("product", {})
    if not product:
        return None

    parameters = {p.get("key"): p for p in product.get("parameters", [])}
    contact_members = product.get("param_groups", {}).get("contact_info", {}).get("members", [])
    raw_body = product.get("body", "") or ""
    soup_body = BeautifulSoup(raw_body, "html.parser")
    megjegyzes = soup_body.get_text(" ", strip=True)

    telefonok = re.findall(r'(?:\+36|06)\s?\d{1,2}\s?\d{3}\s?\d{4}', megjegyzes)
    emailek = re.findall(r'[\w\.-]+@[\w\.-]+', megjegyzes)
    contact_emails = [m.get("value") for m in contact_members if m.get("type") == "email"]
    all_emails = list(dict.fromkeys(emailek + contact_emails))  # order-preserving dedupe

    valid_names = [m.get("name") for m in contact_members if m.get("name") not in ["show_email","chat","contact_location","send_email"]]
    kepviselo_neve = valid_names[0] if valid_names else ""

    elerhetosegek = []
    elerhetosegek.extend(["telefon: "+t for t in telefonok])
    elerhetosegek.extend(["email: "+e for e in all_emails])

    hely = parameters.get("city", {}).get("values", [{}])[0].get("label", "") or ""
    if "kerület" in hely.lower():
        hely = "Budapest"

    row = {
        "munka_neve": product.get("subject"),
        "munka_tipusa": parameters.get("education", {}).get("values", [{}])[0].get("label",""),
        "hely": hely,
        "ceg": product.get("company_name"),
        "oldal": "2",
        "link": product.get("url"),
        "foglalkoztato_neve": product.get("company_name"),
        "kepviselo_neve": kepviselo_neve,
        "kepviselo_elerhetosegei": ", ".join(elerhetosegek),
        "felajanlott_havi_brutto_kereset": product.get("price", {}).get("label",""),
        "munkavegzes_helye": "",  # opcionális: bonyolultabb kinyerés kellhet
        "elvart_iskolai_vegzettseg": parameters.get("education", {}).get("values", [{}])[0].get("label",""),
        "megjegyzes": megjegyzes,
        "email": all_emails[0] if all_emails else "",
        "letrehozva": datetime.now().strftime("%Y.%m.%d %H:%M"),
        "utoljara_frissitve": datetime.now().strftime("%Y.%m.%d %H:%M"),
        "active": True,
        "keresesi_link": BASE_SEARCH_TEMPLATE.format(page=1),
        "teljes_resz_munkaido_ora": "",
        "munkaido_kezdete": "",
        "munkarend": "",
        "eu_allampolgar_javaslat": "",
        "attelepules_kovetelmeny": "",
        "speciális_követelmények": "",
        "speciális_körülmények": "",
        "a_munkakorhoz_kapcsolodo_juttatasok": "",
        "allas_egyeztes_helye": "",
        "allas_egyeztetes_ideje": "",
        "szarmazas": "jofogas2"
    }
    return row

# ----------------- DB MŰVELETEK -----------------
def db_active_links_for_jofogas(supabase):
    """Lekéri a Supabase-ból a jelenleg aktív, jofogas-os linkeket"""
    try:
        res = (
            supabase.table(TABLE_NAME)
            .select("link")
            .eq("szarmazas", "jofogas2")
            .eq("active", True)
            .execute()
        )
        return set(r["link"] for r in (res.data or []))
    except Exception as e:
        print(f"[DB hiba] Nem sikerült lekérni az adatbázist: {e}")
        return set()


def supabase_upsert_rows(supabase, rows):
    """Upsert a Supabase-ba (on_conflict=['link'])"""
    if not rows:
        return None
    try:
        res = (
            supabase.table(TABLE_NAME)
            .upsert(rows, on_conflict=["link"])  # <<< FONTOS!!!
            .execute()
        )
        return res
    except Exception as e:
        print(f"[DB hiba] Upsert sikertelen: {e}")
        return None


def supabase_deactivate_missing(supabase, current_links):
    """
    Inaktiválja azokat a DB rekordokat, amelyek jofogas2 származásúak
    és active=True, de nincsenek a current_links-ben
    """
    try:
        res = (
            supabase.table(TABLE_NAME)
            .select("link")
            .eq("szarmazas", "jofogas2")
            .eq("active", True)
            .execute()
        )
        db_links = set(r["link"] for r in (res.data or []))
        to_deactivate = list(db_links - set(current_links))
        count = 0
        if to_deactivate:
            # egyszerre update-elünk minden hiányzót
            supabase.table(TABLE_NAME).update({"active": False}).in_("link", to_deactivate).execute()
            count = len(to_deactivate)
        return count
    except Exception as e:
        print(f"[DB hiba] Inaktiválás sikertelen: {e}")
        return 0


# ----------------- FŐFUTTATÓ -----------------
def main():
    ensure_dirs()
    session = requests.Session()
    supabase = supabase_client()

    # 1) lekérjük a total pages-t
    total_pages, _ = get_total_pages(session)
    print(f"[info] Találati oldalak száma: {total_pages}")

    # 2) letöltjük a találati oldalakat
    downloaded_searches = download_search_pages(session, total_pages)

    # 3) linkek kinyerése
    all_links = extract_links_from_search_pages()
    print(f"[info] Kinyert linkek száma: {len(all_links)}")

    if not all_links:
        print("[warn] Nincsenek linkek — leállok")
        send_email("Jófogás pipeline hibajelzés", "Nem sikerült kinyerni linkeket a találati oldalakról.")
        return

    # 4) letöltjük az állásoldalakat
    job_success_files, job_failed_links = download_job_pages(session, all_links)
    print(f"[info] Sikeres állás letöltések: {len(job_success_files)}, sikertelen: {len(job_failed_links)}")

    # 5) DB előzetes lekérés - ELŐREHOZVA A PARSE ELÉ!
    db_links_before = db_active_links_for_jofogas(supabase)
    print(f"[info] DB-ben aktív jofogas linkek (futtatás előtt): {len(db_links_before)}")

    # 6) parse job oldalak - MÓDOSÍTVA!
    parsed_rows = []
    parsed_links = []
    for fpath in job_success_files:
        row = parse_job_file(fpath)
        if row:
            # ✅ HA MÁR LÉTEZIK A DB-BEN, NE FRISSÍTSÜK A letrehozva MEZŐT
            if row["link"] in db_links_before:
                row.pop("letrehozva", None)  # eltávolítjuk, így az upsert nem írja felül
            
            parsed_rows.append(row)
            parsed_links.append(row["link"])
    
    print(f"[info] Feldolgozott hirdetések száma (sikeres parse): {len(parsed_rows)}")

    # 7) upsert a Supabase-ba
    upsert_resp = supabase_upsert_rows(supabase, parsed_rows)
    print("[info] Upsert lefutott (ha volt mit feltölteni).")

    # 8) új vs meglévő számolás
    parsed_links_set = set(parsed_links)
    new_links = parsed_links_set - db_links_before
    existing_links = parsed_links_set & db_links_before
    print(f"[info] Új linkek a DB-hez: {len(new_links)}; Már létezett: {len(existing_links)}")

    # 9) inaktiválás (amiket már nem találunk)
    deactivated_count = supabase_deactivate_missing(supabase, parsed_links_set)
    print(f"[info] Inaktivált rekordok száma: {deactivated_count}")

    # 10) email összegzés
    email_body = (
        f"Jófogás pipeline összegzés\n\n"
        f"Találati oldalak száma: {total_pages}\n"
        f"Kinyert linkek: {len(all_links)}\n"
        f"Letöltött állásoldalak (sikeres): {len(job_success_files)}\n"
        f"Parse-olt hirdetések: {len(parsed_rows)}\n"
        f"Új rekordok a DB-ben: {len(new_links)}\n"
        f"Már meglévő frissített rekordok: {len(existing_links)}\n"
        f"Inaktivált rekordok (most): {deactivated_count}\n"
        f"Letöltési hibák (állások): {len(job_failed_links)}\n"
        f"\nTimestamp: {datetime.now().strftime('%Y.%m.%d %H:%M')}\n"
    )
    send_email("Jófogás álláspipeline - összegzés", email_body)
    print("[✓] Pipeline lefuttatva, összegzés elküldve.")


if __name__ == "__main__":
    main()

