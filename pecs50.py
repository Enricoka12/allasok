import requests
from bs4 import BeautifulSoup
import json
import csv
import time
import random
import os
from dotenv import load_dotenv
from supabase import create_client, Client
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

load_dotenv()

# Konfigurációk
USERNAME = os.getenv("USERNAME") or "borose"
PASSWORD = os.getenv("PASSWORD") or "Enricoka1"

EMAIL_SENDER = "kettohallo@gmail.com"
EMAIL_PASSWORD = "ebrx abyh lbgd swlx"
EMAIL_RECIPIENT = "kettohallo@gmail.com"

SUPABASE_URL = "https://crvqiogrobnexbynhjug.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNydnFpb2dyb2JuZXhieW5oanVnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDkxMzQyNTYsImV4cCI6MjA2NDcxMDI1Nn0.ocraogPJgOnf54ALyJLoBb9JnotcWVDRWVVohdaoM0Q"
TABLE_NAME = "allasok"

LOGIN_URL = "https://vmp.munka.hu/belepes"
SEARCH_URL = "https://vmp.munka.hu/allas/talalatok"

# Keresési paraméterek
LOCATION = "pécs"
DISTANCE = "50"

# User-Agent lista
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/113.0"
]

# Email küldés
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

# Supabase kapcsolat
def supabase_kapcsolat():
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Supabase kapcsolat hiba: {e}")
        return None

# Állásadatok konvertálása
def allas_adatok_konvertalasa(allas):
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
        # Új mezők
        "teljes_resz_munkaido_ora": None,
        "munkaido_kezdete": None,
        "munkarend": None,
        "eu_allampolgar_javaslat": None,
        "attelepules_kovetelmeny": None,
        "speciális_követelmények": None,
        "speciális_körülmények": None,
        "a_munkakorhoz_kapcsolodo_juttatasok": None,
        "allas_egyeztes_helye": None,
        "allas_egyeztetes_ideje": None
        # stb.
        ,
        "active": True
    }

# Feltöltés a Supabase-ba
def allasok_feltoltese_supabase(supabase, allasok):
    if not supabase:
        print("❌ Nincs Supabase kapcsolat!")
        return False
    try:
        adatok = [allas_adatok_konvertalasa(a) for a in allasok]
        result = supabase.table(TABLE_NAME).upsert(adatok).execute()
        print(f"✅ Feltöltve: {len(adatok)} állás")
        return True
    except Exception as e:
        print(f"❌ Hiba a feltöltésben: {e}")
        return False

# Ellenőrzi, hogy létezik-e már az állás a DB-ben
def meglevo_allasok_ellenorzese(supabase, link):
    if not supabase or not link:
        return False
    try:
        result = supabase.table(TABLE_NAME).select("id").eq("link", link).execute()
        return len(result.data) > 0
    except:
        return False

# Belépés
def login_and_search(session):
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    login_data = {
        "login_username": USERNAME,
        "login_jelszo": PASSWORD,
        "login": "Belépés"
    }
    resp = session.post(LOGIN_URL, data=login_data, headers=headers)
    if resp.ok and "belepes" not in resp.url:
        print("✅ Belépve")
        return True
    else:
        print("❌ Belépés sikertelen")
        return False

# Új: oldal lekérése oldal számmal
def get_allasok_egy_oldalrol(session, oldal_szam):
    url = f"https://vmp.munka.hu/allas/talalatok?kulcsszo=&kategoria=&isk=&oszk=&feor=&helyseg={LOCATION}&tavolsag={DISTANCE}&munkaido=3&attelepules=&oldal={oldal_szam}&kereses=Keresés"
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    resp = session.get(url, headers=headers)
    if not resp.ok:
        print(f"❌ Nem érhető el az oldal: {url}")
        return [], False

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    # Találatok feldolgozása
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

    # Találatok száma
    talalatok_szama = len(results)
    # Ha van legalább 40 találat, valószínű, van következő oldal
    van_kovetkezo = talalatok_szama >= 40

    return results, van_kovetkezo

def get_job_details(session, allas):
    print("Részletes adatok után:", allas)
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
                    kulcs = tds[0].get_text(strip=True)
                    ertek = tds[1].get_text(strip=True)
                    kulcs = " ".join(kulcs.split())
                    
                    if kulcs == "Foglalkoztató neve":
                        allas["Foglalkoztató neve"] = ertek
                    elif kulcs == "Képviselő neve":
                        allas["Képviselő neve"] = ertek
                    elif kulcs == "Képviselő elérhetőségei":
                        # keresd meg a mailto linket, ha van
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
    # email külön keresése
    email_tag = soup.select_one("#tabs-1 a[href^='mailto:']")
    if email_tag:
        allas["Email"] = email_tag["href"].replace("mailto:", "")

# Új: a részletes adatok
    tab2_div = soup.find("div", id="tabs-2")
    if tab2_div:
        table = tab2_div.find("table", class_="standardTable")
        if table:
            tbody = table.find("tbody")
            if tbody:
                for row in tbody.find_all("tr"):
                    tds = row.find_all("td")
                    if len(tds) >= 2:
                        kulcs = tds[0].get_text(strip=True)
                        ertek = tds[1].get_text(strip=True)

                        # Tisztítjuk a kulcsot
                        kulcs = " ".join(kulcs.split())

                        # Értékek hozzárendelése
                        if kulcs == "Teljes/rész munkaidő (óra)":
                            allas["teljes_resz_munkaido_ora"] = ertek
                        elif kulcs == "Munkaidő kezdete (óra:perc)":
                            allas["munkaido_kezdete"] = ertek
                        elif kulcs == "Munkarend":
                            allas["munkarend"] = ertek
                        elif kulcs == "EU-s állampolgár figyelmébe ajánlja?":
                            allas["eu_allampolgar_javaslat"] = ertek
                        elif kulcs == "Kéri-e az országon belüli áttelepülést és munkát vállaló személyek leválogatását az álláslehetőségről?":
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


 






# create_search_url ugyanaz marad
def create_search_url():
    return f"https://vmp.munka.hu/allas/talalatok?kulcsszo=&kategoria=&isk=&oszk=&feor=&helyseg={LOCATION}&tavolsag={DISTANCE}&munkaido=3&attelepules=&kereses=Keresés"

# Fő program
def main():
    session = requests.Session()
    if not login_and_search(session):
        print("❌ Nem sikerült bejelentkezni")
        return

    allasok = []
    oldal_szam = 1

    while True:
        print(f"Betöltés oldal: {oldal_szam}")
        page_allasok, van_kovetkezo = get_allasok_egy_oldalrol(session, oldal_szam)
        if not page_allasok:
            break
        allasok.extend(page_allasok)
        if van_kovetkezo:
            oldal_szam += 1
            time.sleep(random.uniform(12, 16))
        else:
            break

    # Részletes adatok gyűjtése
    for i, allas in enumerate(allasok):
        print(f"{i+1}/{len(allasok)}: {allas['Munka neve']}")
        detail = get_job_details(session, allas)
        allas.update(detail)
        time.sleep(random.uniform(12, 16))
        
  

    # Feltöltés a DB-be
    supabase = supabase_kapcsolat()
    if supabase:
        print("Feltöltés a DB-be...")
        allasok_feltoltese_supabase(supabase, allasok)

    # E-mail összegzés
    send_email("VMP álláskereső eredmény", f"{len(allasok)} állást gyűjtöttünk össze.")

if __name__ == "__main__":
    main()