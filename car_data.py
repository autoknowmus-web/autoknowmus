"""
AutoKnowMus — Car master data + hybrid pricing formula.

Data structure:
  CAR_DATA = {
    'Maruti Suzuki': {
      'Swift': {
        'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
        'base_price': 750000,       # new price (₹) for most common variant
        'fuels': ['Petrol', 'CNG'],
      },
      ...
    },
    ...
  }

Pricing formula (apply in this order to the base_price):
  1. Age depreciation: ~15% first year, ~10% each subsequent year (capped at 15yr)
  2. Mileage adjustment: deduct 2% per 10,000 km above age-expected mileage
  3. Condition: Excellent = +5%, Good = 0%, Fair = -10%
  4. Owner: 1st = 0%, 2nd = -5%, 3rd+ = -10%
  5. Fuel premium: Diesel +5%, BEV +10%, PHEV +8%, HEV +5%, Petrol 0%, CNG -3%
  6. Variant offset: base=0%, mid-tier +7%, top-tier +15% (relative position in list)

Then price_low  = price * 0.92
     price_high = price * 1.08
     estimated  = price
"""

from datetime import datetime

CURRENT_YEAR = datetime.now().year
EXPECTED_KM_PER_YEAR = 10000  # Indian city average

# ============================================================
# CAR MASTER DATA — 20 brands
# Prices are ex-showroom Bangalore (2024), base variant approx.
# ============================================================

CAR_DATA = {
    'Audi': {
        'A4':       {'variants': ['Premium', 'Premium Plus', 'Technology'], 'base_price': 4500000, 'fuels': ['Petrol', 'Diesel']},
        'A6':       {'variants': ['Premium Plus', 'Technology'],             'base_price': 6500000, 'fuels': ['Petrol']},
        'Q3':       {'variants': ['Premium', 'Premium Plus', 'Technology'], 'base_price': 4500000, 'fuels': ['Petrol']},
        'Q5':       {'variants': ['Premium Plus', 'Technology'],             'base_price': 6800000, 'fuels': ['Petrol']},
        'Q7':       {'variants': ['Premium Plus', 'Technology'],             'base_price': 8800000, 'fuels': ['Petrol']},
        'e-tron':   {'variants': ['55 Quattro', 'Sportback'],                'base_price': 11500000, 'fuels': ['BEV']},
    },
    'BMW': {
        '3 Series': {'variants': ['320d', '330i', 'M340i'],                  'base_price': 5200000, 'fuels': ['Petrol', 'Diesel']},
        '5 Series': {'variants': ['520d', '530i'],                           'base_price': 6800000, 'fuels': ['Petrol', 'Diesel']},
        'X1':       {'variants': ['sDrive18i', 'sDrive20d', 'xDrive20d'],    'base_price': 4800000, 'fuels': ['Petrol', 'Diesel']},
        'X3':       {'variants': ['xDrive20d', 'xDrive30i', 'M40i'],         'base_price': 6700000, 'fuels': ['Petrol', 'Diesel']},
        'X5':       {'variants': ['xDrive30d', 'xDrive40i'],                 'base_price': 9500000, 'fuels': ['Petrol', 'Diesel']},
        'iX':       {'variants': ['xDrive40', 'xDrive50'],                   'base_price': 12000000, 'fuels': ['BEV']},
    },
    'Ford': {
        # Ford exited India; listed for used market
        'EcoSport': {'variants': ['Ambiente', 'Trend', 'Titanium', 'Titanium+'], 'base_price': 900000,  'fuels': ['Petrol', 'Diesel']},
        'Endeavour':{'variants': ['Titanium', 'Titanium+', 'Sport'],             'base_price': 3300000, 'fuels': ['Diesel']},
        'Figo':     {'variants': ['Ambiente', 'Trend', 'Titanium'],              'base_price': 600000,  'fuels': ['Petrol', 'Diesel']},
        'Aspire':   {'variants': ['Ambiente', 'Trend', 'Titanium'],              'base_price': 650000,  'fuels': ['Petrol', 'Diesel']},
        'Freestyle':{'variants': ['Ambiente', 'Trend', 'Titanium', 'Titanium+'], 'base_price': 700000,  'fuels': ['Petrol', 'Diesel']},
    },
    'Honda': {
        'City':     {'variants': ['V', 'VX', 'ZX'],                          'base_price': 1300000, 'fuels': ['Petrol', 'HEV']},
        'Amaze':    {'variants': ['E', 'S', 'V', 'VX'],                      'base_price': 750000,  'fuels': ['Petrol']},
        'Jazz':     {'variants': ['V', 'VX', 'ZX'],                          'base_price': 850000,  'fuels': ['Petrol']},
        'WR-V':     {'variants': ['S', 'SV', 'VX'],                          'base_price': 950000,  'fuels': ['Petrol', 'Diesel']},
        'Elevate':  {'variants': ['SV', 'V', 'VX', 'ZX'],                    'base_price': 1250000, 'fuels': ['Petrol']},
    },
    'Hyundai': {
        'i20':      {'variants': ['Era', 'Magna', 'Sportz', 'Asta'],         'base_price': 750000,  'fuels': ['Petrol']},
        'Verna':    {'variants': ['EX', 'S', 'SX', 'SX(O)'],                 'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Creta':    {'variants': ['E', 'EX', 'S', 'SX', 'SX(O)'],            'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Venue':    {'variants': ['E', 'S', 'SX', 'SX(O)'],                  'base_price': 800000,  'fuels': ['Petrol', 'Diesel']},
        'Alcazar':  {'variants': ['Prestige', 'Signature', 'Platinum'],      'base_price': 1700000, 'fuels': ['Petrol', 'Diesel']},
        'Tucson':   {'variants': ['Platinum', 'Signature'],                  'base_price': 3000000, 'fuels': ['Petrol', 'Diesel']},
        'Kona Electric': {'variants': ['Premium', 'Premium Dual Tone'],      'base_price': 2400000, 'fuels': ['BEV']},
        'Ioniq 5':  {'variants': ['RWD', 'AWD'],                             'base_price': 4600000, 'fuels': ['BEV']},
    },
    'Jaguar': {
        'XE':       {'variants': ['S', 'SE', 'HSE'],                         'base_price': 5000000, 'fuels': ['Petrol']},
        'XF':       {'variants': ['Prestige', 'Portfolio', 'R-Sport'],       'base_price': 7500000, 'fuels': ['Petrol', 'Diesel']},
        'F-Pace':   {'variants': ['S', 'SE', 'HSE'],                         'base_price': 8200000, 'fuels': ['Petrol', 'Diesel']},
        'I-Pace':   {'variants': ['S', 'SE', 'HSE'],                         'base_price': 12600000,'fuels': ['BEV']},
    },
    'Kia': {
        'Sonet':    {'variants': ['HTE', 'HTK', 'HTX', 'GTX+'],              'base_price': 800000,  'fuels': ['Petrol', 'Diesel']},
        'Seltos':   {'variants': ['HTE', 'HTK', 'HTX', 'GTX+', 'X-Line'],    'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Carens':   {'variants': ['Premium', 'Prestige', 'Luxury', 'Luxury Plus'], 'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Carnival': {'variants': ['Premium', 'Prestige', 'Limousine'],       'base_price': 3500000, 'fuels': ['Diesel']},
        'EV6':      {'variants': ['GT Line', 'GT Line AWD'],                 'base_price': 6000000, 'fuels': ['BEV']},
    },
    'Land Rover': {
        'Range Rover Evoque':   {'variants': ['S', 'SE', 'HSE'],             'base_price': 7000000, 'fuels': ['Petrol', 'Diesel']},
        'Discovery Sport':      {'variants': ['S', 'SE', 'HSE'],             'base_price': 6500000, 'fuels': ['Petrol', 'Diesel']},
        'Range Rover Velar':    {'variants': ['S', 'SE', 'HSE'],             'base_price': 8500000, 'fuels': ['Petrol', 'Diesel']},
        'Range Rover Sport':    {'variants': ['SE', 'HSE', 'Autobiography'], 'base_price': 13500000,'fuels': ['Petrol', 'Diesel']},
        'Range Rover':          {'variants': ['SE', 'HSE', 'Autobiography'], 'base_price': 23000000,'fuels': ['Petrol', 'Diesel']},
        'Defender':             {'variants': ['S', 'SE', 'HSE', 'X'],        'base_price': 9500000, 'fuels': ['Petrol', 'Diesel']},
    },
    'Lexus': {
        'ES':       {'variants': ['Luxury', 'F-Sport'],                      'base_price': 6500000, 'fuels': ['HEV']},
        'NX':       {'variants': ['Luxury', 'F-Sport'],                      'base_price': 7200000, 'fuels': ['HEV']},
        'RX':       {'variants': ['Luxury', 'F-Sport'],                      'base_price': 9900000, 'fuels': ['HEV']},
        'LX':       {'variants': ['VX', 'Ultra Luxury'],                     'base_price': 28000000,'fuels': ['Petrol', 'Diesel']},
        'LS':       {'variants': ['Ultra Luxury'],                           'base_price': 20000000,'fuels': ['HEV']},
    },
    'Mahindra': {
        'Bolero':   {'variants': ['B4', 'B6', 'B6(O)'],                      'base_price': 950000,  'fuels': ['Diesel']},
        'Scorpio-N':{'variants': ['Z2', 'Z4', 'Z6', 'Z8', 'Z8 L'],           'base_price': 1400000, 'fuels': ['Petrol', 'Diesel']},
        'XUV300':   {'variants': ['W4', 'W6', 'W8', 'W8(O)'],                'base_price': 900000,  'fuels': ['Petrol', 'Diesel']},
        'XUV700':   {'variants': ['MX', 'AX3', 'AX5', 'AX7', 'AX7 L'],       'base_price': 1400000, 'fuels': ['Petrol', 'Diesel']},
        'Thar':     {'variants': ['AX', 'AX(O)', 'LX'],                      'base_price': 1400000, 'fuels': ['Petrol', 'Diesel']},
        'Marazzo':  {'variants': ['M2', 'M4', 'M6', 'M8'],                   'base_price': 1100000, 'fuels': ['Diesel']},
        'XEV 9e':   {'variants': ['Pack 1', 'Pack 2', 'Pack 3'],             'base_price': 2200000, 'fuels': ['BEV']},
        'BE 6e':    {'variants': ['Pack 1', 'Pack 2', 'Pack 3'],             'base_price': 1900000, 'fuels': ['BEV']},
    },
    'Maruti Suzuki': {
        'Alto K10': {'variants': ['Std', 'LXi', 'VXi', 'VXi+'],              'base_price': 450000,  'fuels': ['Petrol', 'CNG']},
        'S-Presso': {'variants': ['Std', 'LXi', 'VXi', 'VXi+'],              'base_price': 500000,  'fuels': ['Petrol', 'CNG']},
        'Wagon R':  {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],              'base_price': 580000,  'fuels': ['Petrol', 'CNG']},
        'Swift':    {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],              'base_price': 700000,  'fuels': ['Petrol', 'CNG']},
        'Celerio':  {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],              'base_price': 550000,  'fuels': ['Petrol', 'CNG']},
        'Dzire':    {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],              'base_price': 700000,  'fuels': ['Petrol', 'CNG']},
        'Baleno':   {'variants': ['Sigma', 'Delta', 'Zeta', 'Alpha'],        'base_price': 750000,  'fuels': ['Petrol', 'CNG']},
        'Ignis':    {'variants': ['Sigma', 'Delta', 'Zeta', 'Alpha'],        'base_price': 620000,  'fuels': ['Petrol']},
        'Brezza':   {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],              'base_price': 950000,  'fuels': ['Petrol', 'CNG']},
        'Ertiga':   {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],              'base_price': 950000,  'fuels': ['Petrol', 'CNG']},
        'XL6':      {'variants': ['Zeta', 'Alpha', 'Alpha+'],                'base_price': 1250000, 'fuels': ['Petrol', 'CNG']},
        'Grand Vitara':{'variants': ['Sigma', 'Delta', 'Zeta', 'Alpha'],     'base_price': 1100000, 'fuels': ['Petrol', 'HEV', 'CNG']},
        'Invicto':  {'variants': ['Zeta+', 'Alpha+'],                        'base_price': 2500000, 'fuels': ['HEV']},
    },
    'Mercedes-Benz': {
        'A-Class':  {'variants': ['A 200', 'A 200 d'],                       'base_price': 4500000, 'fuels': ['Petrol', 'Diesel']},
        'C-Class':  {'variants': ['C 200', 'C 220 d', 'C 300'],               'base_price': 5500000, 'fuels': ['Petrol', 'Diesel']},
        'E-Class':  {'variants': ['E 200', 'E 220 d', 'E 350'],               'base_price': 7800000, 'fuels': ['Petrol', 'Diesel']},
        'GLA':      {'variants': ['GLA 200', 'GLA 220 d'],                    'base_price': 5000000, 'fuels': ['Petrol', 'Diesel']},
        'GLC':      {'variants': ['GLC 220 d', 'GLC 300'],                    'base_price': 7500000, 'fuels': ['Petrol', 'Diesel']},
        'GLE':      {'variants': ['GLE 300 d', 'GLE 450'],                    'base_price': 10000000,'fuels': ['Petrol', 'Diesel']},
        'GLS':      {'variants': ['GLS 450 4MATIC', 'Maybach GLS'],           'base_price': 13500000,'fuels': ['Petrol', 'Diesel']},
        'EQS':      {'variants': ['580 4MATIC'],                              'base_price': 16500000,'fuels': ['BEV']},
    },
    'MG': {
        'Astor':    {'variants': ['Style', 'Super', 'Smart', 'Sharp', 'Savvy'], 'base_price': 1100000, 'fuels': ['Petrol']},
        'Hector':   {'variants': ['Style', 'Super', 'Smart', 'Sharp'],       'base_price': 1450000, 'fuels': ['Petrol', 'Diesel']},
        'Hector Plus':{'variants': ['Style', 'Super', 'Smart', 'Sharp'],     'base_price': 1700000, 'fuels': ['Petrol', 'Diesel']},
        'Gloster':  {'variants': ['Super', 'Smart', 'Sharp', 'Savvy'],       'base_price': 3900000, 'fuels': ['Diesel']},
        'ZS EV':    {'variants': ['Excite', 'Exclusive'],                    'base_price': 2300000, 'fuels': ['BEV']},
        'Comet EV': {'variants': ['Executive', 'Exclusive', 'Excite'],       'base_price': 800000,  'fuels': ['BEV']},
        'Windsor EV':{'variants': ['Excite', 'Exclusive'],                   'base_price': 1400000, 'fuels': ['BEV']},
    },
    'Nissan': {
        'Magnite':  {'variants': ['XE', 'XL', 'XV', 'XV Premium'],           'base_price': 650000,  'fuels': ['Petrol']},
        'Kicks':    {'variants': ['XL', 'XV', 'XV Premium'],                 'base_price': 1000000, 'fuels': ['Petrol']},
        'GT-R':     {'variants': ['Premium Edition'],                        'base_price': 22000000,'fuels': ['Petrol']},
    },
    'Renault': {
        'Kwid':     {'variants': ['RXE', 'RXL', 'RXT', 'Climber'],           'base_price': 500000,  'fuels': ['Petrol']},
        'Kiger':    {'variants': ['RXE', 'RXL', 'RXT', 'RXZ'],               'base_price': 650000,  'fuels': ['Petrol']},
        'Triber':   {'variants': ['RXE', 'RXL', 'RXT', 'RXZ'],               'base_price': 600000,  'fuels': ['Petrol']},
        'Duster':   {'variants': ['RXE', 'RXS', 'RXZ'],                      'base_price': 950000,  'fuels': ['Petrol', 'Diesel']},
    },
    'Skoda': {
        'Kushaq':   {'variants': ['Active', 'Ambition', 'Style', 'Monte Carlo'], 'base_price': 1200000, 'fuels': ['Petrol']},
        'Slavia':   {'variants': ['Active', 'Ambition', 'Style', 'Monte Carlo'], 'base_price': 1100000, 'fuels': ['Petrol']},
        'Kodiaq':   {'variants': ['Style', 'Sportline', 'L&K'],              'base_price': 4000000, 'fuels': ['Petrol']},
        'Superb':   {'variants': ['Sportline', 'L&K'],                       'base_price': 5700000, 'fuels': ['Petrol']},
        'Octavia':  {'variants': ['Style', 'L&K'],                           'base_price': 2700000, 'fuels': ['Petrol']},
    },
    'Tata': {
        'Tiago':    {'variants': ['XE', 'XM', 'XT', 'XZ', 'XZ+'],            'base_price': 550000,  'fuels': ['Petrol', 'CNG']},
        'Tiago EV': {'variants': ['XE', 'XT', 'XZ+', 'XZ+ LUX'],             'base_price': 850000,  'fuels': ['BEV']},
        'Tigor':    {'variants': ['XE', 'XM', 'XT', 'XZ', 'XZ+'],            'base_price': 620000,  'fuels': ['Petrol', 'CNG']},
        'Altroz':   {'variants': ['XE', 'XM', 'XT', 'XZ', 'XZ+'],            'base_price': 680000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Punch':    {'variants': ['Pure', 'Adventure', 'Accomplished', 'Creative'], 'base_price': 650000, 'fuels': ['Petrol', 'CNG']},
        'Punch EV': {'variants': ['Smart', 'Adventure', 'Empowered'],        'base_price': 1100000, 'fuels': ['BEV']},
        'Nexon':    {'variants': ['Smart', 'Pure', 'Creative', 'Fearless'],  'base_price': 900000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Nexon EV': {'variants': ['Creative', 'Fearless', 'Empowered'],      'base_price': 1500000, 'fuels': ['BEV']},
        'Curvv':    {'variants': ['Smart', 'Pure', 'Creative', 'Accomplished'], 'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Curvv EV': {'variants': ['Creative', 'Accomplished', 'Empowered'],  'base_price': 1800000, 'fuels': ['BEV']},
        'Harrier':  {'variants': ['Smart', 'Pure', 'Adventure', 'Fearless'], 'base_price': 1550000, 'fuels': ['Diesel']},
        'Safari':   {'variants': ['Smart', 'Pure', 'Adventure', 'Fearless'], 'base_price': 1650000, 'fuels': ['Diesel']},
    },
    'Toyota': {
        'Glanza':   {'variants': ['E', 'S', 'G', 'V'],                       'base_price': 700000,  'fuels': ['Petrol', 'CNG']},
        'Urban Cruiser Taisor':{'variants': ['E', 'S', 'G', 'V'],            'base_price': 750000,  'fuels': ['Petrol', 'CNG']},
        'Urban Cruiser Hyryder':{'variants': ['E', 'S', 'G', 'V'],           'base_price': 1100000, 'fuels': ['Petrol', 'HEV', 'CNG']},
        'Innova Crysta':{'variants': ['GX', 'VX', 'ZX'],                     'base_price': 2000000, 'fuels': ['Diesel']},
        'Innova Hycross':{'variants': ['GX', 'VX', 'ZX', 'ZX(O)'],           'base_price': 1900000, 'fuels': ['Petrol', 'HEV']},
        'Fortuner': {'variants': ['4x2 AT', '4x4 AT', 'Legender', 'GR-Sport'],'base_price': 3400000, 'fuels': ['Diesel']},
        'Camry':    {'variants': ['Hybrid'],                                  'base_price': 4600000, 'fuels': ['HEV']},
        'Vellfire': {'variants': ['VIP Lounge'],                              'base_price': 12000000,'fuels': ['HEV']},
    },
    'Volkswagen': {
        'Virtus':   {'variants': ['Comfortline', 'Highline', 'Topline', 'GT'],'base_price': 1150000, 'fuels': ['Petrol']},
        'Taigun':   {'variants': ['Comfortline', 'Highline', 'Topline', 'GT'],'base_price': 1200000, 'fuels': ['Petrol']},
        'Tiguan':   {'variants': ['Elegance', 'R-Line'],                      'base_price': 3700000, 'fuels': ['Petrol']},
    },
    'Volvo': {
        'XC40':     {'variants': ['Ultimate'],                                'base_price': 5000000, 'fuels': ['Petrol']},
        'XC60':     {'variants': ['Plus', 'Ultimate'],                        'base_price': 6700000, 'fuels': ['Petrol']},
        'XC90':     {'variants': ['Ultimate'],                                'base_price': 9800000, 'fuels': ['Petrol', 'HEV']},
        'S90':      {'variants': ['Ultimate'],                                'base_price': 6800000, 'fuels': ['Petrol']},
        'XC40 Recharge':{'variants': ['Ultimate'],                            'base_price': 5600000, 'fuels': ['BEV']},
        'C40 Recharge':{'variants': ['Ultimate'],                             'base_price': 6300000, 'fuels': ['BEV']},
    },
}


# ============================================================
# HELPERS FOR DROPDOWN POPULATION
# ============================================================

def get_makes():
    """Return sorted list of all Makes for first dropdown."""
    return sorted(CAR_DATA.keys())

def get_models(make: str):
    """Return sorted list of models for a given Make."""
    if make not in CAR_DATA:
        return []
    return sorted(CAR_DATA[make].keys())

def get_variants(make: str, model: str):
    """Return sorted list of variants for a given Make + Model."""
    if make not in CAR_DATA or model not in CAR_DATA[make]:
        return []
    return sorted(CAR_DATA[make][model]['variants'])

def get_fuels(make: str, model: str):
    """Return fuel options for a given Make + Model in fixed order."""
    if make not in CAR_DATA or model not in CAR_DATA[make]:
        return []
    fuel_order = ['Petrol', 'Diesel', 'CNG', 'HEV', 'PHEV', 'BEV']
    model_fuels = CAR_DATA[make][model].get('fuels', [])
    return [f for f in fuel_order if f in model_fuels]

def get_base_price(make: str, model: str):
    """Return base new price for a given Make + Model (₹)."""
    if make not in CAR_DATA or model not in CAR_DATA[make]:
        return None
    return CAR_DATA[make][model].get('base_price')


# ============================================================
# PRICING FORMULA (HYBRID — base + later adjusted by DB matches)
# ============================================================

def compute_base_valuation(make, model, variant, fuel, year, mileage, condition, owner):
    """
    Apply the hybrid formula to produce a single estimated price.
    Returns an integer ₹ value. All inputs validated upstream.
    """
    base = get_base_price(make, model)
    if base is None:
        return None

    # --- 1. Age depreciation ---
    age = max(0, CURRENT_YEAR - int(year))
    age = min(age, 15)  # cap at 15 years

    if age == 0:
        dep_factor = 1.00
    elif age == 1:
        dep_factor = 0.85
    else:
        # 15% first year, ~10% each subsequent
        dep_factor = 0.85 * (0.90 ** (age - 1))

    price = base * dep_factor

    # --- 2. Mileage adjustment (only above expected) ---
    t
