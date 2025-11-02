import os
import json
import sqlite3
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Form
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from passlib.context import CryptContext

from rapidfuzz import process, fuzz

# ------------------------------
# Configuración básica
# ------------------------------
app = FastAPI()

# Montar frontend
frontend_path = os.path.join(os.path.dirname(__file__), "..", "frontend")
frontend_path = os.path.abspath(frontend_path)
app.mount("/frontend", StaticFiles(directory=frontend_path), name="frontend")


# ------------------------------
# Base de datos de usuarios
# ------------------------------
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_db():
    conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), "users.db"))
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        )
    """)
    conn.commit()

# Llamamos al inicializador de la DB
init_db()

# ------------------------------
# Rutas del frontend
# ------------------------------
@app.get("/")
def root():
    """Sirve la página principal del frontend"""
    return FileResponse(os.path.join(frontend_path, "login.html"))

# ------------------------------
# Endpoints de autenticación
# ------------------------------
@app.post("/signup")
def signup(username: str = Form(...), password: str = Form(...)):
    """Registro de un nuevo usuario"""
    conn = get_db()
    password_hash = pwd_context.hash(password)
    try:
        conn.execute(
            "INSERT INTO users (username, password_hash) VALUES (?, ?)",
            (username, password_hash)
        )
        conn.commit()
        return {"message": "✅ Usuario creado correctamente"}
    except sqlite3.IntegrityError:
        raise HTTPException(status_code=400, detail="❌ El usuario ya existe")

@app.post("/login")
def login(username: str = Form(...), password: str = Form(...)):
    """Login de usuario"""
    conn = get_db()
    user = conn.execute(
        "SELECT * FROM users WHERE username = ?", (username,)
    ).fetchone()

    if not user or not pwd_context.verify(password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="❌ Credenciales inválidas")

    # Aquí podrías devolver un JWT en lugar de un simple mensaje
    return {"message": "🎉 Login exitoso"}

# ------------------------------
# Endpoints de datos de fútbol
# ------------------------------
@app.get("/matches", response_model=List[Dict[str, Any]])
def get_matches():
    """Devuelve fixtures del día actual"""
    today = datetime.today().strftime("%Y-%m-%d")
    file_path = f"results/fixtures/fixtures_{today}.json"

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Fixtures not available yet")

    with open(file_path, "r", encoding="utf-8") as f:
        matches = json.load(f)
    return matches


@app.get("/predictions", response_model=List[Dict[str, Any]])
def get_predictions():
    """Devuelve predicciones del día actual con player_id usando fuzzy matching"""
    today = datetime.today().strftime("%Y-%m-%d")
    predictions_path = f"results/predictions_{today}.csv"
    players_csv_path = "delfos/frontend/photos/players_ids.csv"

    if not os.path.exists(predictions_path):
        raise HTTPException(status_code=404, detail="Predictions not available yet")
    if not os.path.exists(players_csv_path):
        raise HTTPException(status_code=500, detail="Players CSV not found")

    # Leer CSVs
    df_pred = pd.read_csv(predictions_path)
    df_players = pd.read_csv(players_csv_path)

    # Crear un diccionario para acceso rápido: nombre -> id
    player_name_to_id = dict(zip(df_players['player_name'], df_players['player_id']))

    # Función para obtener el ID por nombre aproximado
    def get_player_id_fuzzy(name):
        if pd.isna(name):
            return None
        match, score, _ = process.extractOne(
            name, player_name_to_id.keys(), scorer=fuzz.token_sort_ratio
        )
        # Puedes ajustar el umbral si quieres
        if score >= 80:
            return player_name_to_id[match]
        return None

    # Aplicar a cada predicción
    df_pred['player_id'] = df_pred['player_'].apply(get_player_id_fuzzy)

    predictions = df_pred.to_dict(orient="records")
    df_pred.to_csv(f'results/predictions_{today}_fotos.csv', index=False)
    return predictions
