import os
import math
import datetime
from io import BytesIO
import pandas as pd
from flask import Flask, request, render_template, send_file, flash, redirect, url_for, session
from werkzeug.utils import secure_filename
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
import random # Nouveau: pour simuler le scoring

# -------------------- CONFIG --------------------
ALLOWED_EXTENSIONS = {'xls', 'xlsx'}
UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
DAYS = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi"]
MAX_FREE_TRIALS = 2 

# -------------------- FLASK --------------------
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
# CLÉ SECRÈTE À CHANGER LORS DE LA MISE EN PRODUCTION RÉELLE
app.secret_key = "cle_tres_secrete_pour_west_ops" 

# -------------------- UTILITAIRES --------------------
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def haversine_km(a, b):
    lat1, lon1 = a; lat2, lon2 = b
    R = 6371.0
    phi1, phi2 = map(math.radians, (lat1, lat2))
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    x = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2*R*math.asin(math.sqrt(x))

def load_clients_from_excel(path):
    try:
        return pd.read_excel(path, engine="openpyxl")
    except:
        return pd.read_excel(path, engine="xlrd")

def detect_columns(df):
    cols = {c.lower(): c for c in df.columns}
    mapping = {}
    for name, keys in {
        'name': ["nom client","client","nom","name"],
        'addr': ["adresse 2","adresse","address"],
        'zip': ["code postal","postal","zip"],
        'city': ["ville","city"]
    }.items():
        for k in keys:
            if k in cols:
                mapping[name] = cols[k]; break
    return mapping

def build_full_address(row, mapping):
    parts = []
    for key in ['addr','zip','city']:
        col = mapping.get(key)
        if col and pd.notnull(row.get(col)):
            parts.append(str(row[col]).strip())
    parts.append("France")
    return ", ".join(parts)

def geocode_addresses(df, mapping):
    geolocator = Nominatim(user_agent="west_ops_app_v1_contact_westops@gmail.com")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1) 
    
    df['full_address'] = df.apply(lambda r: build_full_address(r, mapping), axis=1)
    lat, lon = [], []
    
    limit_safety = 50 
    
    for i, addr in enumerate(tqdm(df['full_address'], desc="Géocodage")):
        if i >= limit_safety:
            lat.append(None); lon.append(None)
            continue
            
        try:
            loc = geocode(addr, timeout=10)
            if loc: 
                lat.append(loc.latitude); lon.append(loc.longitude)
            else: 
                lat.append(None); lon.append(None)
        except Exception as e:
            print(f"Erreur sur {addr}: {e}") 
            lat.append(None); lon.append(None)
            
    df['lat'], df['lon'] = lat, lon
    return df.dropna(subset=['lat','lon'])

def cluster_clients(df, max_clients_per_day):
    df = df.sort_values(by=['lat'], ascending=False).reset_index(drop=True)
    clusters = []
    num_clusters = max(1, math.ceil(len(df) / max_clients_per_day))
    chunk_size = math.ceil(len(df) / num_clusters)
    for i in range(len(df)):
        clusters.append(i // chunk_size)
        
    df['cluster'] = clusters
    return df

def plan_tours(df, start_coords, min_clients, max_clients):
    clusters = sorted(df['cluster'].unique())
    
    cluster_centroids = {}
    for c in clusters:
        sub = df[df['cluster']==c]
        cluster_centroids[c] = (sub['lat'].mean(), sub['lon'].mean())

    clusters_sorted = sorted(clusters, key=lambda c: haversine_km(start_coords, cluster_centroids[c]))

    tours = []; idx_day = 0
    for c in clusters_sorted:
        group = df[df['cluster'] == c].copy()
        points_pendants = group.to_dict('records')
        current_pos = start_coords
        
        day_name = DAYS[idx_day % len(DAYS)]
        week_num = (idx_day // len(DAYS)) + 1
        ordre = 1
        
        while points_pendants:
            best_dist = float('inf'); best_idx = -1
            for i, p in enumerate(points_pendants):
                dist = haversine_km(current_pos, (p['lat'], p['lon']))
                if dist < best_dist: best_dist = dist; best_idx = i
            
            next_client = points_pendants.pop(best_idx)
            tours.append({
                "Semaine": week_num, "Jour": day_name, "Ordre": ordre,
                "Nom client": next_client.get("Nom client") or next_client.get("client") or "",
                "Adresse": next_client.get("Adresse 2") or next_client.get("Adresse") or "",
                "Code postal": next_client.get("Code postal") or "",
                "Ville": next_client.get("Ville") or "",
                "Latitude": next_client["lat"], "Longitude": next_client["lon"],
                "Distance trajet (km)": round(best_dist, 2)
            })
            current_pos = (next_client['lat'], next_client['lon'])
            ordre += 1
        idx_day += 1
    return pd.DataFrame(tours)

def format_excel(df):
    output = BytesIO()
    df.to_excel(output, index=False, sheet_name="Planning")
    output.seek(0)
    wb = load_workbook(output)
    ws = wb.active
    header_fill = PatternFill("solid", fgColor="4472C4")
    header_font = Font(color="FFFFFF", bold=True)
    for cell in ws[1]:
        cell.fill = header_fill; cell.font = header_font; cell.alignment = Alignment(horizontal="center")
    for i, row in enumerate(ws.iter_rows(min_row=2), start=2):
        if i % 2 == 0:
            for cell in row: cell.fill = PatternFill("solid", fgColor="E9EDF7")
    for col in ws.columns:
        length = max(len(str(cell.value or "")) for cell in col)
        ws.column_dimensions[get_column_letter(col[0].column)].width = length + 3
    ws.auto_filter.ref = ws.dimensions
    out = BytesIO(); wb.save(out); out.seek(0)
    return out

# -------------------- ROUTES --------------------

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/planner')
def planner():
    if 'uploads_count' not in session:
        session['uploads_count'] = 0
    remaining = max(0, MAX_FREE_TRIALS - session['uploads_count'])
    limit_reached = (remaining == 0)
    return render_template('planner.html', remaining=remaining, limit_reached=limit_reached)


@app.route('/dashboard')
def dashboard():
    data = session.get('planning_data', [])
    summary = session.get('planning_summary', {'total_clients': 0, 'total_distance': 0})
    return render_template('dashboard.html', data=data[:50], summary=summary)


@app.route('/smart_email')
def smart_email():
    planning_data = session.get('planning_data', [])
    clients = sorted(list(set(p.get('Nom client') for p in planning_data if p.get('Nom client'))))
    return render_template('smart_email.html', clients=clients)


# -------------------- NOUVELLE ROUTE 1 : SCORING PROSPECTS --------------------
@app.route('/scoring_prospects')
def scoring_prospects():
    # Récupère les données brutes
    data = session.get('planning_data', [])
    
    # Simule un scoring pour l'MVP
    # On ajoute une colonne 'score' basée sur un nombre aléatoire pour simuler l'IA
    scored_data = []
    if data:
        for client in data:
            # Score de 60 à 100 pour la simulation
            client_score = random.randint(60, 100)
            client['Score'] = client_score
            scored_data.append(client)
        
        # Trie les clients par score décroissant
        scored_data = sorted(scored_data, key=lambda x: x['Score'], reverse=True)

    return render_template('scoring_prospects.html', data=scored_data)


# -------------------- NOUVELLE ROUTE 2 : ASSISTANT IA --------------------
@app.route('/assistant_ia')
def assistant_ia():
    # Pas besoin de données pour cette page MVP, juste l'interface
    return render_template('assistant_ia.html')


@app.route('/upload', methods=['POST'])
def upload():
    # GESTION DES CRÉDITS (Freemium)
    if 'uploads_count' not in session:
        session['uploads_count'] = 0
    
    if session['uploads_count'] >= MAX_FREE_TRIALS:
        flash("Vous avez épuisé vos 2 essais gratuits. Passez à la vitesse supérieure !")
        return redirect(url_for('planner', _anchor='pricing')) 

    min_clients = int(request.form.get('min_clients', 4))
    max_clients = int(request.form.get('max_clients', 6))
    start_city = request.form.get('start_city', '').strip()
    file = request.files.get('file')

    if not file or not allowed_file(file.filename):
        flash("Fichier invalide (seulement .xlsx ou .xls).")
        return redirect(url_for('planner'))
    
    if not start_city:
        flash("Ville de départ manquante.")
        return redirect(url_for('planner'))

    filename = secure_filename(file.filename)
    path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(path)

    df = load_clients_from_excel(path)
    mapping = detect_columns(df)
    df_geo = geocode_addresses(df, mapping)
    
    if df_geo.empty:
        flash("Impossible de localiser les adresses. Vérifiez le format.")
        return redirect(url_for('planner'))

    df_clustered = cluster_clients(df_geo, max_clients)
    
    geolocator = Nominatim(user_agent="start_point")
    try:
        loc = geolocator.geocode(start_city + ", France", timeout=10)
    except:
        loc = None

    if not loc:
        flash("Ville de départ introuvable.")
        return redirect(url_for('planner'))
    start_coords = (loc.latitude, loc.longitude)

    # Lancement de l'optimisation
    df_plan = plan_tours(df_clustered, start_coords, min_clients, max_clients)
    excel = format_excel(df_plan)
    
    # --- STOCKAGE DES DONNÉES EN SESSION POUR LE DASHBOARD ET SCORING ---
    session['planning_data'] = df_plan.to_dict('records')
    session['planning_summary'] = {
        'total_clients': len(df_plan),
        'total_distance': round(df_plan['Distance trajet (km)'].sum(), 2)
    }
    # ---------------------------------------------------------------------

    session['uploads_count'] += 1
    
    out_name = f"Planning_WestOps_{datetime.datetime.now().strftime('%Y%m%d')}.xlsx"
    return send_file(excel, as_attachment=True, download_name=out_name, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

if __name__ == "__main__":
    app.run(debug=True)