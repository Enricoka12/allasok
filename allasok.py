import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import random
import sys
import os
from datetime import datetime, timezone
from supabase import create_client, Client
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# ---------------------------------------------------------
# Konfigurációk (ENV változókból)
# ---------------------------------------------------------
USERNAME = os.environ["USERNAME"]
PASSWORD = os.environ["PASSWORD"]

EMAIL_SENDER = os.environ["EMAIL_SENDER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]
EMAIL_RECIPIENT = os.environ["EMAIL_RECIPIENT"]

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
TABLE_NAME = os.environ["TABLE_NAME"]

LOGIN_URL = os.environ["LOGIN_URL"]

# ---------------------------------------------------------
# Keresési paraméterek
# ---------------------------------------------------------
if len(sys.argv) >= 2:
    LOCATION = sys.argv[1]
else:
    print("Hiba: nincs megadva városnév.")
    sys.exit(1)

DISTANCE = sys.argv[2] if len(sys.argv) > 2 else "50"

# ---------------------------------------------------------
# User-Agent lista
# ---------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0"
]

# ---------------------------------------------------------
# Email küldés
# ---------------------------------------------------------
def send_email(subject, message):
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECIPIENT
        msg["Subject"] = subject
        msg.attach(MIMEText(message, "plain"))
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print("✅ Email elküldve")
        return True
    except Exception as e:
        print(f"❌ Email hiba: {e}")
        return False

# ---------------------------------------------------------
# Supabase kapcsolat
# ---------------------------------------------------------
def supabase_kapcsolat():
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Supabase kapcsolat hiba: {e}")
        return None

# ---------------------------------------------------------
# DB lekérdezések
# ---------------------------------------------------------
def db_allasok_lekerese(supabase, keresesi_link):
    """Visszaadja az aktív állásokat az adott keresési linkhez (inaktiváláshoz)"""
    if not supabase:
        return {}
    try:
        result = supabase.table(TABLE_NAME).select("link, id").eq("keresesi_link", keresesi_link).eq("active", True).execute()
        return {allas["link"]: allas["id"] for allas in result.data}
    except Exception as e:
        print(f"❌ DB állások lekérése hiba: {e}")
        return {}

def osszes_aktiv_link_lekerese(supabase):
    """Visszaadja az ÖSSZES aktív állás linkjét az EGÉSZ adatbázisból"""
    if not supabase:
        return set()
    try:
        result = supabase.table(TABLE_NAME).select("link").eq("active", True).execute()
        return {allas["link"] for allas in result.data}
    except Exception as e:
        print(f"❌ Összes aktív link lekérése hiba: {e}")
        return set()

def inaktivalt_allasok(supabase, keresesi_link, scrapped_linkek):
    if not supabase:
        return 0
    try:
        db_linkek = db_allasok_lekerese(supabase, keresesi_link)
        inaktivalando_linkek = [link for link in db_linkek.keys() if link not in scrapped_linkek]
        
        most = datetime.now(timezone.utc).isoformat()
        
        if inaktivalando_linkek:
            for link in inaktivalando_linkek:
                supabase.table(TABLE_NAME).update({
                    "active": False,
                    "utoljara_frissitve": most
                }).eq("link", link).execute()
            print(f"✅ {len(inaktivalando_linkek)} állás inaktiválva")
            return len(inaktivalando_linkek)
        else:
            print("✅ Nincs inaktiválandó állás")
            return 0
    except Exception as e:
        print(f"❌ Inaktiválás hiba: {e}")
        return 0

# ---------------------------------------------------------
# Állás konvertálása
# ---------------------------------------------------------
def allas_adatok_konvertalasa(allas):
    most = datetime.now(timezone.utc).isoformat()
    return {
        "munka_neve": allas.get("Munka neve"),
        "munka_tipusa": allas.get("Munka típusa"),
        "hely": allas.get("Hely"),
        "ceg": allas.get("Cég"),
        "oldal": allas.get("Oldal", 1),
        "link": allas.get("Link"),
        "keresesi_link": allas.get("keresesi_link"),
        "foglalkoztato_neve": allas.get("Foglalkoztató neve"),
        "kepviselo_neve": allas.get("Képviselő neve"),
        "kepviselo_elerhetosegei": allas.get("Képviselő elérhetőségei"),
        "felajanlott_havi_brutto_kereset": allas.get("Felajánlott havi bruttó kereset (Ft)"),
        "munkavegzes_helye": allas.get("Munkavégzés helye"),
        "elvart_iskolai_vegzettseg": allas.get("Elvárt iskolai végzettség"),
        "megjegyzes": allas.get("Megjegyzés"),
        "email": allas.get("Email"),
        "teljes_resz_munkaido_ora": allas.get("teljes_resz_munkaido_ora"),
        "munkaido_kezdete": allas.get("munkaido_kezdete"),
        "munkarend": allas.get("munkarend"),
        "eu_allampolgar_javaslat": allas.get("eu_allampolgar_javaslat"),
        "attelepules_kovetelmeny": allas.get("attelepules_kovetelmeny"),
        "speciális_követelmények": allas.get("speciális_követelmények"),
        "speciális_körülmények": allas.get("speciális_körülmények"),
        "a_munkakorhoz_kapcsolodo_juttatasok": allas.get("a_munkakorhoz_kapcsolodo_juttatasok"),
        "allas_egyeztes_helye": allas.get("allas_egyeztes_helye"),
        "allas_egyeztetes_ideje": allas.get("allas_egyeztetes_ideje"),
        "active": True,
        "szarmazas": "virtuális munkaerő piac",
        "utoljara_frissitve": most  # ÚJ MEZŐ
    }

# ---------------------------------------------------------
# Aktív állások számának lekérése
# ---------------------------------------------------------
def get_aktiv_allasok_szama(supabase, keresesi_link):
    """Visszaadja az aktív állások számát az adott keresési linkhez"""
    if not supabase:
        return 0
    try:
        result = supabase.table(TABLE_NAME).select("id", count="exact").eq("keresesi_link", keresesi_link).eq("active", True).execute()
        szam = result.count if hasattr(result, 'count') and result.count is not None else 0
        return szam
    except Exception as e:
        print(f"❌ Aktív állások lekérése hiba: {e}")
        return 0

# ---------------------------------------------------------
# Feltöltés Supabase-ba EGYENKÉNT (batch nélkül)
# ---------------------------------------------------------
def allasok_feltoltese_supabase(supabase, allasok):
    if not supabase:
        print("❌ Nincs Supabase kapcsolat!")
        return 0

    adatok = [allas_adatok_konvertalasa(a) for a in allasok]

    # egyediség link alapján
    unique_adatok = []
    seen_links = set()
    for a in adatok:
        link = a.get("link")
        if link and link not in seen_links:
            unique_adatok.append(a)
            seen_links.add(link)

    if not unique_adatok:
        print("✅ Nincsenek új rekordok feltöltésre")
        return 0

    print(f"📝 {len(unique_adatok)} egyedi rekord feltöltése egyenként...")
    osszes_mentett = 0
    sikertelen = 0
    
    # Egyenként mentés
    for i, adat in enumerate(unique_adatok):
        try:
            resp = supabase.table(TABLE_NAME).upsert(
                adat,
                on_conflict="link"
            ).execute()
            
            if hasattr(resp, "data") and resp.data is not None and len(resp.data) > 0:
                osszes_mentett += 1
                if (i + 1) % 10 == 0:  # Minden 10. rekorднál kiírás
                    print(f"   ✅ Mentve: {i + 1}/{len(unique_adatok)}")
            else:
                sikertelen += 1
                print(f"   ⚠ Nem mentődött: {adat.get('munka_neve')} | Link: {adat.get('link')}")
                
        except Exception as e:
            sikertelen += 1
            print(f"   ❌ Hiba ({i + 1}/{len(unique_adatok)}): {e}")
            print(f"      Munka: {adat.get('munka_neve')}")
            print(f"      Link: {adat.get('link')}")

        # Rövid szünet minden 10. mentés után
        if (i + 1) % 10 == 0:
            time.sleep(random.uniform(1, 2))

    print(f"\n📊 MENTÉS EREDMÉNYE:")
    print(f"   ✅ Sikeres: {osszes_mentett}")
    print(f"   ❌ Sikertelen: {sikertelen}")
    print(f"   📋 Összesen: {len(unique_adatok)}")
    
    return osszes_mentett

# ---------------------------------------------------------
# Meglévő állások frissítése (ha már léteztek)
# ---------------------------------------------------------
def meglevo_allasok_frissitese(supabase, allasok_linkjei):
    """Frissíti a már létező állások utoljara_frissitve mezőjét"""
    if not supabase or not allasok_linkjei:
        return 0
    
    most = datetime.now(timezone.utc).isoformat()
    frissitett = 0
    
    for link in allasok_linkjei:
        try:
            supabase.table(TABLE_NAME).update({
                "utoljara_frissitve": most,
                "active": True
            }).eq("link", link).execute()
            frissitett += 1
        except Exception as e:
            print(f"❌ Frissítés hiba ({link}): {e}")
    
    if frissitett > 0:
        print(f"🔄 {frissitett} meglévő állás frissítve")
    
    return frissitett

# ---------------------------------------------------------
# Belépés
# ---------------------------------------------------------
def login_and_search(session):
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    login_data = {
        "login_username": USERNAME,
        "login_jelszo": PASSWORD,
        "login": "Belépés"
    }
    resp = session.post(LOGIN_URL, data=login_data, headers=headers)
    if resp.ok and "belepes" not in resp.url.lower():
        print("✅ Belépve")
        return True
    else:
        print("❌ Belépés sikertelen")
        return False

# ---------------------------------------------------------
# Oldalak bejárása
# ---------------------------------------------------------
def get_allasok_egy_oldalrol(session, oldal_szam):
    url = f"https://vmp.munka.hu/allas/talalatok?kulcsszo=&kategoria=&isk=&oszk=&feor=&helyseg={LOCATION}&tavolsag={DISTANCE}&munkaido=3&attelepules=&oldal={oldal_szam}&kereses=Keresés"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    resp = session.get(url, headers=headers)
    if not resp.ok:
        print(f"❌ Nem érhető el az oldal: {url}")
        return [], False

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    rows = soup.select("tbody tr")
    for row in rows:
        tds = row.find_all("td")
        if len(tds) >= 4:
            allas = {
                "Munka neve": tds[0].get_text(strip=True),
                "Munka típusa": tds[1].get_text(strip=True),
                "Hely": tds[2].get_text(strip=True),
                "Cég": tds[3].get_text(strip=True),
                "Oldal": oldal_szam,
                "Link": "",
                "keresesi_link": create_search_url()
            }
            link_elem = tds[0].find("a")
            if link_elem and link_elem.has_attr("href"):
                allas["Link"] = "https://vmp.munka.hu" + link_elem["href"]
            results.append(allas)

    van_kovetkezo = len(results) >= 40
    return results, van_kovetkezo

# ---------------------------------------------------------
# Részletes adatok
# ---------------------------------------------------------
def get_job_details(session, allas):
    print(f"Részletes adatok után: {allas['Munka neve']}")
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    resp = session.get(allas["Link"], headers=headers)
    if not resp.ok:
        print(f"❌ Nem sikerült az oldal letöltése: {allas['Link']}")
        return allas
    soup = BeautifulSoup(resp.text, "html.parser")
    
    tables = soup.find_all("table", class_="standardTable")
    
    for table in tables:
        tbody = table.find("tbody")
        if tbody:
            for row in tbody.find_all("tr"):
                tds = row.find_all("td")
                if len(tds) >= 2:
                    kulcs = " ".join(tds[0].get_text(strip=True).split())
                    ertek = tds[1].get_text(strip=True)
                    
                    if kulcs == "Foglalkoztató neve":
                        allas["Foglalkoztató neve"] = ertek
                    elif kulcs == "Képviselő neve":
                        allas["Képviselő neve"] = ertek
                    elif kulcs == "Képviselő elérhetőségei":
                        email_link = row.find("a", href=True)
                        if email_link and email_link['href'].startswith('mailto:'):
                            allas["Képviselő elérhetőségei"] = email_link['href'].replace("mailto:", "")
                        else:
                            allas["Képviselő elérhetőségei"] = ertek
                    elif kulcs == "Felajánlott havi bruttó kereset (Ft)":
                        allas["Felajánlott havi bruttó kereset (Ft)"] = ertek
                    elif kulcs == "Munkavégzés helye":
                        allas["Munkavégzés helye"] = ertek
                    elif kulcs == "Elvárt iskolai végzettség":
                        allas["Elvárt iskolai végzettség"] = ertek
                    elif kulcs == "Megjegyzés":
                        allas["Megjegyzés"] = ertek

    # email
    email_tag = soup.select_one("#tabs-1 a[href^='mailto:']")
    if email_tag:
        allas["Email"] = email_tag["href"].replace("mailto:", "")

    # tab2 extra adatok
    tab2_div = soup.find("div", id="tabs-2")
    if tab2_div:
        table = tab2_div.find("table", class_="standardTable")
        if table:
            tbody = table.find("tbody")
            if tbody:
                for row in tbody.find_all("tr"):
                    tds = row.find_all("td")
                    if len(tds) >= 2:
                        kulcs = " ".join(tds[0].get_text(strip=True).split())
                        ertek = tds[1].get_text(strip=True)
                        if kulcs == "Teljes/rész munkaidő (óra)":
                            allas["teljes_resz_munkaido_ora"] = ertek
                        elif kulcs == "Munkaidő kezdete (óra:perc)":
                            allas["munkaido_kezdete"] = ertek
                        elif kulcs == "Munkarend":
                            allas["munkarend"] = ertek
                        elif kulcs == "EU-s állampolgár figyelmébe ajánlja?":
                            allas["eu_allampolgar_javaslat"] = ertek
                        elif kulcs.startswith("Kéri-e az országon belüli áttelepülést"):
                            allas["attelepules_kovetelmeny"] = ertek
                        elif kulcs == "Speciális követelmények":
                            allas["speciális_követelmények"] = ertek
                        elif kulcs == "Speciális körülmények":
                            allas["speciális_körülmények"] = ertek
                        elif kulcs == "A munkakörhöz kapcsolódó juttatások":
                            allas["a_munkakorhoz_kapcsolodo_juttatasok"] = ertek
                        elif kulcs == "Állásegyeztetés helye":
                            allas["allas_egyeztes_helye"] = ertek
                        elif kulcs == "Állásegyeztetés ideje":
                            allas["allas_egyeztetes_ideje"] = ertek

    return allas

# ---------------------------------------------------------
# Segéd
# ---------------------------------------------------------
def create_search_url():
    return f"https://vmp.munka.hu/allas/talalatok?kulcsszo=&kategoria=&isk=&oszk=&feor=&helyseg={LOCATION}&tavolsag={DISTANCE}&munkaido=3&attelepules=&kereses=Keresés"

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
def main():
    session = requests.Session()
    if not login_and_search(session):
        return

    supabase = supabase_kapcsolat()
    keresesi_link = create_search_url()
    
    # KONTROLL: Aktív állások száma ELŐTTE
    aktiv_elotte = get_aktiv_allasok_szama(supabase, keresesi_link)
    print(f"📊 DB-ben {aktiv_elotte} aktív állás van ehhez a kereséshez FELTÖLTÉS ELŐTT")
    
    # Lekérjük az adott keresési linkhez tartozó állásokat (inaktiváláshoz)
    db_allasok_keresesi_link = db_allasok_lekerese(supabase, keresesi_link) if supabase else {}
    
    # Lekérjük az ÖSSZES aktív állás linkjét az EGÉSZ adatbázisból (duplikáció ellenőrzéshez)
    osszes_aktiv_link = osszes_aktiv_link_lekerese(supabase) if supabase else set()
    print(f"📊 Teljes adatbázisban {len(osszes_aktiv_link)} aktív állás van (összes keresésből)")

    allasok = []
    oldal_szam = 1

    # Csak a linkeket gyűjtjük (GYORS)
    print("\n🔍 LINKEK GYŰJTÉSE (részletes adatok nélkül)...")
    while True:
        print(f"🔍 Betöltés oldal: {oldal_szam}")
        page_allasok, van_kovetkezo = get_allasok_egy_oldalrol(session, oldal_szam)
        if not page_allasok:
            break
        allasok.extend(page_allasok)
        if van_kovetkezo:
            oldal_szam += 1
            time.sleep(random.uniform(25, 35))
        else:
            break

    print(f"📋 Összesen {len(allasok)} állás találva")

    # SZÉTVÁLOGATÁS: Tényleg új vs már létező (EGÉSZ adatbázis alapján!)
    tenyleg_uj_allasok = []
    mar_letezo_allasok_linkjei = []
    scrapped_linkek = set()
    
    for allas in allasok:
        link = allas["Link"]
        scrapped_linkek.add(link)
        
        if link not in osszes_aktiv_link:
            # TÉNYLEG ÚJ - nincs bent az EGÉSZ adatbázisban
            tenyleg_uj_allasok.append(allas)
        else:
            # MÁR LÉTEZIK valahol az adatbázisban
            mar_letezo_allasok_linkjei.append(link)

    print(f"\n🆕 {len(tenyleg_uj_allasok)} TÉNYLEG ÚJ állás (nincs bent az adatbázisban)")
    print(f"♻️ {len(mar_letezo_allasok_linkjei)} már létező állás (megvan más keresésből)")

    # Meglévő állások frissítése (utoljara_frissitve)
    frissitett_szam = meglevo_allasok_frissitese(supabase, mar_letezo_allasok_linkjei)
    
    # Inaktiválás (csak a keresési linkhez tartozó állások közül azok, amiket most NEM találtunk)
    inaktivalt_szam = inaktivalt_allasok(supabase, keresesi_link, scrapped_linkek)

    # Részletes adatok letöltése CSAK a TÉNYLEG ÚJ állásokhoz
    if tenyleg_uj_allasok:
        print(f"\n📖 RÉSZLETES ADATOK LETÖLTÉSE ({len(tenyleg_uj_allasok)} új álláshoz)...")
        for i, allas in enumerate(tenyleg_uj_allasok):
            print(f"📖 {i+1}/{len(tenyleg_uj_allasok)}: {allas['Munka neve']} - részletes adatletöltés...")
            detail = get_job_details(session, allas)
            allas.update(detail)
            time.sleep(random.uniform(25, 35))
    else:
        print("\n✅ Nincs új állás, nincs mit letölteni")

    # Új állások feltöltése
    mentett_db = 0
    if supabase and tenyleg_uj_allasok:
        print("\n💾 Új állások feltöltése DB-be...")
        mentett_db = allasok_feltoltese_supabase(supabase, tenyleg_uj_allasok)

    # KONTROLL: Aktív állások száma UTÁNA
    aktiv_utana = get_aktiv_allasok_szama(supabase, keresesi_link)
    print(f"\n📊 DB-ben {aktiv_utana} aktív állás van ehhez a kereséshez FELTÖLTÉS UTÁN")
    
    # ELLENŐRZÉS
    vart_szam = aktiv_elotte - inaktivalt_szam + len(tenyleg_uj_allasok)
    kulonbseg = aktiv_utana - vart_szam
    
    print(f"\n🔍 KONTROLL:")
    print(f"   Előtte (keresési link): {aktiv_elotte} db")
    print(f"   Inaktivált: -{inaktivalt_szam} db")
    print(f"   Tényleg új feltöltve: +{len(tenyleg_uj_allasok)} db")
    print(f"   Várt végeredmény: {vart_szam} db")
    print(f"   Tényleges végeredmény: {aktiv_utana} db")
    print(f"   Eltérés: {kulonbseg} db")
    
    if kulonbseg == 0:
        print(f"   ✅ MINDEN RENDBEN! Nincs eltérés.")
        kontroll_status = "✅ SIKERES"
    else:
        print(f"   ⚠️ FIGYELEM! {abs(kulonbseg)} db eltérés van!")
        kontroll_status = f"⚠️ ELTÉRÉS: {kulonbseg} db"

    email_uzenet = f"""
VMP Álláskereső eredmény - {LOCATION} ({DISTANCE}km)

📊 ÖSSZEGZÉS:
• Scrape találatok: {len(allasok)} db
• Tényleg új (nincs az adatbázisban): {len(tenyleg_uj_allasok)} db
• Már létező (megvan más keresésből): {len(mar_letezo_allasok_linkjei)} db
• Frissítve: {frissitett_szam} db
• Inaktivált: {inaktivalt_szam} db
• DB-be mentve: {mentett_db} db

📈 ADATBÁZIS KONTROLL:
• Aktív állások ehhez a kereséshez (előtte): {aktiv_elotte} db
• Aktív állások ehhez a kereséshez (utána): {aktiv_utana} db
• Összes aktív állás az adatbázisban: {len(osszes_aktiv_link)} db
• Várt végeredmény: {vart_szam} db
• Tényleges végeredmény: {aktiv_utana} db
• Státusz: {kontroll_status}

🔍 Keresési URL: {keresesi_link}

{'🎉 Vannak tényleg új állások!' if tenyleg_uj_allasok else '📋 Nincsenek új állások (minden már bent van az adatbázisban).'}
"""
    
    email_subject = f"VMP álláskeresés - {len(tenyleg_uj_allasok)} új állás - {LOCATION} {kontroll_status}"
    send_email(email_subject, email_uzenet)
    
    # Várakozás a script végén
    wait_time = random.uniform(35, 45)
    print(f"\n⏳ Várakozás {wait_time:.1f} másodperc a script végén...")
    time.sleep(wait_time)
    print("✅ Script befejezve")

if __name__ == "__main__":
    main()
