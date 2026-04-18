from flask import Flask, render_template, redirect, url_for, session, request, flash
from authlib.integrations.flask_client import OAuth
from datetime import datetime
import os
import traceback

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-me')

# ========== GOOGLE OAUTH ==========
oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get('GOOGLE_CLIENT_ID'),
    client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

# ========== MASTER DATA ==========
CAR_BRANDS = sorted([
    'Audi', 'BMW', 'Ford', 'Honda', 'Hyundai', 'Jaguar', 'Kia',
    'Land Rover', 'Lexus', 'Mahindra', 'Maruti Suzuki', 'Mercedes-Benz',
    'MG', 'Nissan', 'Renault', 'Skoda', 'Tata', 'Toyota', 'Volkswagen', 'Volvo'
])

MODELS_BY_BRAND = {
    'Audi': sorted(['A3', 'A4', 'A6', 'Q3', 'Q5', 'Q7']),
    'BMW': sorted(['3 Series', '5 Series', 'X1', 'X3', 'X5', 'X7']),
    'Ford': sorted(['EcoSport', 'Endeavour', 'Figo', 'Freestyle']),
    'Honda': sorted(['Amaze', 'City', 'Civic', 'Elevate', 'Jazz', 'WR-V']),
    'Hyundai': sorted(['Alcazar', 'Aura', 'Creta', 'Exter', 'Grand i10 Nios', 'i20', 'Kona Electric', 'Tucson', 'Venue', 'Verna']),
    'Jaguar': sorted(['F-Pace', 'XE', 'XF']),
    'Kia': sorted(['Carens', 'Carnival', 'EV6', 'Seltos', 'Sonet']),
    'Land Rover': sorted(['Defender', 'Discovery', 'Range Rover', 'Range Rover Evoque', 'Range Rover Sport']),
    'Lexus': sorted(['ES', 'LX', 'NX', 'RX']),
    'Mahindra': sorted(['Bolero', 'Scorpio', 'Scorpio N', 'Thar', 'XUV300', 'XUV400', 'XUV700', 'XUV3XO']),
    'Maruti Suzuki': sorted(['Alto K10', 'Baleno', 'Brezza', 'Celerio', 'Dzire', 'Eeco', 'Ertiga', 'Fronx', 'Grand Vitara', 'Ignis', 'Jimny', 'S-Presso', 'Swift', 'Wagon R', 'XL6']),
    'Mercedes-Benz': sorted(['A-Class', 'C-Class', 'E-Class', 'GLA', 'GLC', 'GLE', 'S-Class']),
    'MG': sorted(['Astor', 'Gloster', 'Hector', 'Hector Plus', 'ZS EV']),
    'Nissan': sorted(['Kicks', 'Magnite']),
    'Renault': sorted(['Kiger', 'Kwid', 'Triber']),
    'Skoda': sorted(['Kushaq', 'Octavia', 'Slavia', 'Superb']),
    'Tata': sorted(['Altroz', 'Harrier', 'Nexon', 'Nexon EV', 'Punch', 'Safari', 'Tiago', 'Tigor']),
    'Toyota': sorted(['Camry', 'Fortuner', 'Glanza', 'Hilux', 'Hyryder', 'Innova Crysta', 'Innova Hycross', 'Vellfire']),
    'Volkswagen': sorted(['Taigun', 'Tiguan', 'Virtus']),
    'Volvo': sorted(['XC40', 'XC60', 'XC90'])
}

# ========== REAL PER-MODEL VARIANTS ==========
VARIANTS_BY_MODEL = {
    # Maruti Suzuki
    'Alto K10': ['LXi', 'VXi', 'VXi+', 'VXi AGS', 'VXi+ AGS'],
    'Baleno': ['Sigma', 'Delta', 'Zeta', 'Alpha'],
    'Brezza': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
    'Celerio': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
    'Dzire': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
    'Eeco': ['5 Seater', '7 Seater', 'Cargo'],
    'Ertiga': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
    'Fronx': ['Sigma', 'Delta', 'Delta+', 'Zeta', 'Alpha'],
    'Grand Vitara': ['Sigma', 'Delta', 'Delta+', 'Zeta', 'Alpha', 'Alpha+'],
    'Ignis': ['Sigma', 'Delta', 'Zeta', 'Alpha'],
    'Jimny': ['Zeta', 'Alpha'],
    'S-Presso': ['LXi', 'VXi', 'VXi+'],
    'Swift': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
    'Wagon R': ['LXi', 'VXi', 'ZXi'],
    'XL6': ['Zeta', 'Alpha', 'Alpha+'],

    # Hyundai
    'Alcazar': ['Prestige', 'Platinum', 'Signature'],
    'Aura': ['E', 'S', 'SX', 'SX+', 'SX(O)'],
    'Creta': ['E', 'EX', 'S', 'S+', 'SX', 'SX(O)'],
    'Exter': ['EX', 'S', 'SX', 'SX(O)'],
    'Grand i10 Nios': ['Era', 'Magna', 'Sportz', 'Asta'],
    'i20': ['Magna', 'Sportz', 'Asta', 'Asta(O)'],
    'Kona Electric': ['Premium', 'Premium Dual Tone'],
    'Tucson': ['Platinum', 'Signature'],
    'Venue': ['E', 'S', 'S+', 'SX', 'SX(O)'],
    'Verna': ['EX', 'S', 'SX', 'SX(O)'],

    # Tata
    'Altroz': ['XE', 'XM+', 'XT', 'XZ', 'XZ+'],
    'Harrier': ['Smart', 'Pure', 'Adventure', 'Fearless', 'Fearless+'],
    'Nexon': ['Smart', 'Pure', 'Creative', 'Fearless', 'Fearless+'],
    'Nexon EV': ['Creative', 'Fearless', 'Empowered'],
    'Punch': ['Pure', 'Adventure', 'Accomplished', 'Creative'],
    'Safari': ['Smart', 'Pure', 'Adventure', 'Accomplished', 'Fearless+'],
    'Tiago': ['XE', 'XM', 'XT', 'XZ+'],
    'Tigor': ['XE', 'XM', 'XZ', 'XZ+'],

    # Mahindra
    'Bolero': ['B4', 'B6', 'B6(O)'],
    'Scorpio': ['S3', 'S5', 'S7', 'S9', 'S11'],
    'Scorpio N': ['Z2', 'Z4', 'Z6', 'Z8', 'Z8L'],
    'Thar': ['AX', 'AX(O)', 'LX'],
    'XUV300': ['W4', 'W6', 'W8', 'W8(O)'],
    'XUV400': ['EC', 'EL'],
    'XUV700': ['MX', 'AX3', 'AX5', 'AX7', 'AX7L'],
    'XUV3XO': ['MX1', 'MX2', 'MX3', 'AX5', 'AX7', 'AX7L'],

    # Toyota
    'Camry': ['Hybrid'],
    'Fortuner': ['2.7', '2.8 4x2', '2.8 4x4', 'Legender'],
    'Glanza': ['E', 'S', 'G', 'V'],
