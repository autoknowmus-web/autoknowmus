"""
AutoKnowMus — Used car master data + hybrid pricing formula.

This is a USED CAR platform. Fuel options reflect all fuel types the model
has been sold with in India over the past ~15 years, not just currently
available new-car fuels. Example: Maruti Swift had a Diesel variant until
2020 — so Diesel must be available for Swift valuations.

Data structure:
  CAR_DATA = {
    'Maruti Suzuki': {
      'Swift': {
        'variants': ['LDi', 'LXi', 'VDi', 'VXi', 'ZDi', 'ZXi', 'ZXi+'],
        'base_price': 750000,
        'fuels': ['Petrol', 'Diesel', 'CNG'],   # Diesel discontinued 2020 but common in used market
      },
      ...
    }
  }

Pricing formula (applied in order to base_price):
  1. Age depreciation: ~15% first year, ~10% each subsequent year (cap 15yr)
  2. Mileage adjustment: -2% per 10k km above age-expected mileage
  3. Condition: Excellent +5%, Good 0%, Fair -10%
  4. Owner: 1st 0%, 2nd -5%, 3rd+ -10%
  5. Fuel: Diesel +5%, BEV +10%, PHEV +8%, HEV +5%, Petrol 0%, CNG -3%
  6. Variant position: base 0% → top +15%
Then low = price * 0.92, high = price * 1.08
"""

from datetime import datetime

CURRENT_YEAR = datetime.now().year
EXPECTED_KM_PER_YEAR = 10000

# ============================================================
# USED-CAR MASTER DATA — 20 brands
# Covers models sold in India in the past ~15 years.
# Fuel lists include historical variants still present in the used market.
# ============================================================

CAR_DATA = {
    'Audi': {
        'A3':        {'variants': ['Premium', 'Premium Plus', 'Technology'],  'base_price': 4000000, 'fuels': ['Petrol', 'Diesel']},
        'A4':        {'variants': ['Premium', 'Premium Plus', 'Technology'],  'base_price': 4500000, 'fuels': ['Petrol', 'Diesel']},
        'A6':        {'variants': ['Premium Plus', 'Technology'],              'base_price': 6500000, 'fuels': ['Petrol', 'Diesel']},
        'A8':        {'variants': ['L Celebration', 'L 55 TFSI'],              'base_price': 15500000,'fuels': ['Petrol', 'Diesel']},
        'Q3':        {'variants': ['Premium', 'Premium Plus', 'Technology'],  'base_price': 4500000, 'fuels': ['Petrol', 'Diesel']},
        'Q5':        {'variants': ['Premium Plus', 'Technology'],              'base_price': 6800000, 'fuels': ['Petrol', 'Diesel']},
        'Q7':        {'variants': ['Premium Plus', 'Technology'],              'base_price': 8800000, 'fuels': ['Petrol', 'Diesel']},
        'e-tron':    {'variants': ['55 Quattro', 'Sportback'],                 'base_price': 11500000,'fuels': ['BEV']},
    },
    'BMW': {
        '3 Series':  {'variants': ['320d', '330i', 'M340i'],                   'base_price': 5200000, 'fuels': ['Petrol', 'Diesel']},
        '5 Series':  {'variants': ['520d', '530i'],                            'base_price': 6800000, 'fuels': ['Petrol', 'Diesel']},
        '7 Series':  {'variants': ['730Ld', '740Li', 'M760Li'],                'base_price': 14500000,'fuels': ['Petrol', 'Diesel']},
        'X1':        {'variants': ['sDrive18i', 'sDrive20d', 'xDrive20d'],     'base_price': 4800000, 'fuels': ['Petrol', 'Diesel']},
        'X3':        {'variants': ['xDrive20d', 'xDrive30i', 'M40i'],          'base_price': 6700000, 'fuels': ['Petrol', 'Diesel']},
        'X5':        {'variants': ['xDrive30d', 'xDrive40i'],                  'base_price': 9500000, 'fuels': ['Petrol', 'Diesel']},
        'X7':        {'variants': ['xDrive30d', 'xDrive40i', 'M50i'],          'base_price': 12200000,'fuels': ['Petrol', 'Diesel']},
        'iX':        {'variants': ['xDrive40', 'xDrive50'],                    'base_price': 12000000,'fuels': ['BEV']},
    },
    'Ford': {
        # Ford exited India 2021; HUGE used-market presence
        'Aspire':    {'variants': ['Ambiente', 'Trend', 'Titanium'],           'base_price': 650000,  'fuels': ['Petrol', 'Diesel']},
        'EcoSport':  {'variants': ['Ambiente', 'Trend', 'Titanium', 'Titanium+'], 'base_price': 900000,  'fuels': ['Petrol', 'Diesel']},
        'Endeavour': {'variants': ['Titanium', 'Titanium+', 'Sport'],          'base_price': 3300000, 'fuels': ['Diesel']},
        'Figo':      {'variants': ['Ambiente', 'Trend', 'Titanium'],           'base_price': 600000,  'fuels': ['Petrol', 'Diesel']},
        'Freestyle': {'variants': ['Ambiente', 'Trend', 'Titanium', 'Titanium+'], 'base_price': 700000,  'fuels': ['Petrol', 'Diesel']},
    },
    'Honda': {
        'Amaze':     {'variants': ['E', 'S', 'V', 'VX'],                       'base_price': 750000,  'fuels': ['Petrol', 'Diesel']},
        'BR-V':      {'variants': ['S', 'V', 'VX'],                            'base_price': 1000000, 'fuels': ['Petrol', 'Diesel']},
        'City':      {'variants': ['V', 'VX', 'ZX'],                           'base_price': 1300000, 'fuels': ['Petrol', 'Diesel', 'HEV']},
        'Civic':     {'variants': ['V', 'VX', 'ZX'],                           'base_price': 1800000, 'fuels': ['Petrol', 'Diesel']},
        'CR-V':      {'variants': ['2WD', '4WD'],                              'base_price': 2900000, 'fuels': ['Petrol', 'Diesel']},
        'Elevate':   {'variants': ['SV', 'V', 'VX', 'ZX'],                     'base_price': 1250000, 'fuels': ['Petrol']},
        'Jazz':      {'variants': ['V', 'VX', 'ZX'],                           'base_price': 850000,  'fuels': ['Petrol', 'Diesel']},
        'Mobilio':   {'variants': ['S', 'V', 'RS'],                            'base_price': 900000,  'fuels': ['Petrol', 'Diesel']},
        'WR-V':      {'variants': ['S', 'SV', 'VX'],                           'base_price': 950000,  'fuels': ['Petrol', 'Diesel']},
    },
    'Hyundai': {
        'Alcazar':   {'variants': ['Prestige', 'Signature', 'Platinum'],       'base_price': 1700000, 'fuels': ['Petrol', 'Diesel']},
        'Aura':      {'variants': ['E', 'S', 'SX', 'SX(O)'],                   'base_price': 650000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Creta':     {'variants': ['E', 'EX', 'S', 'SX', 'SX(O)'],             'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Elantra':   {'variants': ['S', 'SX', 'SX(O)'],                        'base_price': 1800000, 'fuels': ['Petrol', 'Diesel']},
        'Elite i20': {'variants': ['Era', 'Magna', 'Sportz', 'Asta'],          'base_price': 700000,  'fuels': ['Petrol', 'Diesel']},
        'Eon':       {'variants': ['Era', 'Magna', 'Sportz'],                  'base_price': 400000,  'fuels': ['Petrol']},
        'Exter':     {'variants': ['EX', 'S', 'SX', 'SX(O)'],                  'base_price': 650000,  'fuels': ['Petrol', 'CNG']},
        'Grand i10': {'variants': ['Era', 'Magna', 'Sportz', 'Asta'],          'base_price': 550000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Grand i10 Nios':{'variants': ['Era', 'Magna', 'Sportz', 'Asta'],      'base_price': 600000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'i10':       {'variants': ['Era', 'Magna', 'Sportz', 'Asta'],          'base_price': 500000,  'fuels': ['Petrol']},
        'i20':       {'variants': ['Era', 'Magna', 'Sportz', 'Asta'],          'base_price': 750000,  'fuels': ['Petrol', 'Diesel']},
        'i20 Active':{'variants': ['S', 'SX'],                                 'base_price': 800000,  'fuels': ['Petrol', 'Diesel']},
        'Ioniq 5':   {'variants': ['RWD', 'AWD'],                              'base_price': 4600000, 'fuels': ['BEV']},
        'Kona Electric':{'variants': ['Premium', 'Premium Dual Tone'],         'base_price': 2400000, 'fuels': ['BEV']},
        'Santa Fe':  {'variants': ['GLS', 'Signature'],                        'base_price': 3000000, 'fuels': ['Diesel']},
        'Santro':    {'variants': ['D-Lite', 'Era', 'Magna', 'Sportz', 'Asta'],'base_price': 500000,  'fuels': ['Petrol', 'CNG']},
        'Tucson':    {'variants': ['Platinum', 'Signature'],                   'base_price': 3000000, 'fuels': ['Petrol', 'Diesel']},
        'Venue':     {'variants': ['E', 'S', 'SX', 'SX(O)'],                   'base_price': 800000,  'fuels': ['Petrol', 'Diesel']},
        'Verna':     {'variants': ['EX', 'S', 'SX', 'SX(O)'],                  'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Xcent':     {'variants': ['E', 'S', 'SX'],                            'base_price': 550000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
    },
    'Jaguar': {
        'F-Pace':    {'variants': ['S', 'SE', 'HSE'],                          'base_price': 8200000, 'fuels': ['Petrol', 'Diesel']},
        'I-Pace':    {'variants': ['S', 'SE', 'HSE'],                          'base_price': 12600000,'fuels': ['BEV']},
        'XE':        {'variants': ['S', 'SE', 'HSE'],                          'base_price': 5000000, 'fuels': ['Petrol', 'Diesel']},
        'XF':        {'variants': ['Prestige', 'Portfolio', 'R-Sport'],        'base_price': 7500000, 'fuels': ['Petrol', 'Diesel']},
        'XJ':        {'variants': ['L Portfolio'],                             'base_price': 11000000,'fuels': ['Petrol', 'Diesel']},
    },
    'Kia': {
        'Carens':    {'variants': ['Premium', 'Prestige', 'Luxury', 'Luxury Plus'], 'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Carnival':  {'variants': ['Premium', 'Prestige', 'Limousine'],        'base_price': 3500000, 'fuels': ['Diesel']},
        'EV6':       {'variants': ['GT Line', 'GT Line AWD'],                  'base_price': 6000000, 'fuels': ['BEV']},
        'EV9':       {'variants': ['GT Line', 'GT Line AWD'],                  'base_price': 13000000,'fuels': ['BEV']},
        'Seltos':    {'variants': ['HTE', 'HTK', 'HTX', 'GTX+', 'X-Line'],     'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Sonet':     {'variants': ['HTE', 'HTK', 'HTX', 'GTX+'],               'base_price': 800000,  'fuels': ['Petrol', 'Diesel']},
    },
    'Land Rover': {
        'Defender':              {'variants': ['S', 'SE', 'HSE', 'X'],          'base_price': 9500000, 'fuels': ['Petrol', 'Diesel']},
        'Discovery':             {'variants': ['S', 'SE', 'HSE'],               'base_price': 9000000, 'fuels': ['Petrol', 'Diesel']},
        'Discovery Sport':       {'variants': ['S', 'SE', 'HSE'],               'base_price': 6500000, 'fuels': ['Petrol', 'Diesel']},
        'Freelander':            {'variants': ['SE', 'HSE'],                    'base_price': 4500000, 'fuels': ['Diesel']},
        'Range Rover':           {'variants': ['SE', 'HSE', 'Autobiography'],   'base_price': 23000000,'fuels': ['Petrol', 'Diesel']},
        'Range Rover Evoque':    {'variants': ['S', 'SE', 'HSE'],               'base_price': 7000000, 'fuels': ['Petrol', 'Diesel']},
        'Range Rover Sport':     {'variants': ['SE', 'HSE', 'Autobiography'],   'base_price': 13500000,'fuels': ['Petrol', 'Diesel']},
        'Range Rover Velar':     {'variants': ['S', 'SE', 'HSE'],               'base_price': 8500000, 'fuels': ['Petrol', 'Diesel']},
    },
    'Lexus': {
        'ES':        {'variants': ['Luxury', 'F-Sport'],                       'base_price': 6500000, 'fuels': ['HEV']},
        'LS':        {'variants': ['Ultra Luxury'],                            'base_price': 20000000,'fuels': ['HEV']},
        'LX':        {'variants': ['VX', 'Ultra Luxury'],                      'base_price': 28000000,'fuels': ['Petrol', 'Diesel']},
        'NX':        {'variants': ['Luxury', 'F-Sport'],                       'base_price': 7200000, 'fuels': ['HEV']},
        'RX':        {'variants': ['Luxury', 'F-Sport'],                       'base_price': 9900000, 'fuels': ['HEV']},
    },
    'Mahindra': {
        'Alturas G4':{'variants': ['4x2 AT', '4x4 AT'],                        'base_price': 3000000, 'fuels': ['Diesel']},
        'BE 6e':     {'variants': ['Pack 1', 'Pack 2', 'Pack 3'],              'base_price': 1900000, 'fuels': ['BEV']},
        'Bolero':    {'variants': ['B4', 'B6', 'B6(O)'],                       'base_price': 950000,  'fuels': ['Diesel']},
        'KUV100':    {'variants': ['K2', 'K4', 'K6', 'K8'],                    'base_price': 600000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Marazzo':   {'variants': ['M2', 'M4', 'M6', 'M8'],                    'base_price': 1100000, 'fuels': ['Diesel']},
        'Scorpio':   {'variants': ['S3', 'S5', 'S7', 'S9', 'S11'],             'base_price': 1200000, 'fuels': ['Diesel']},
        'Scorpio-N': {'variants': ['Z2', 'Z4', 'Z6', 'Z8', 'Z8 L'],            'base_price': 1400000, 'fuels': ['Petrol', 'Diesel']},
        'Thar':      {'variants': ['AX', 'AX(O)', 'LX'],                       'base_price': 1400000, 'fuels': ['Petrol', 'Diesel']},
        'TUV300':    {'variants': ['T4', 'T6', 'T8', 'T10'],                   'base_price': 800000,  'fuels': ['Diesel']},
        'XEV 9e':    {'variants': ['Pack 1', 'Pack 2', 'Pack 3'],              'base_price': 2200000, 'fuels': ['BEV']},
        'XUV 3XO':   {'variants': ['MX1', 'MX3', 'AX5', 'AX7', 'AX7 L'],       'base_price': 800000,  'fuels': ['Petrol', 'Diesel']},
        'XUV300':    {'variants': ['W4', 'W6', 'W8', 'W8(O)'],                 'base_price': 900000,  'fuels': ['Petrol', 'Diesel']},
        'XUV400':    {'variants': ['EC', 'EL'],                                'base_price': 1600000, 'fuels': ['BEV']},
        'XUV500':    {'variants': ['W5', 'W7', 'W9', 'W11'],                   'base_price': 1400000, 'fuels': ['Diesel']},
        'XUV700':    {'variants': ['MX', 'AX3', 'AX5', 'AX7', 'AX7 L'],        'base_price': 1400000, 'fuels': ['Petrol', 'Diesel']},
        'Xylo':      {'variants': ['D2', 'D4', 'H4', 'H8', 'H9'],              'base_price': 850000,  'fuels': ['Diesel']},
    },
    'Maruti Suzuki': {
        # Diesel discontinued April 2020, but widely present in used market for 2015-2020 cars
        'A-Star':    {'variants': ['LXi', 'VXi', 'ZXi'],                       'base_price': 450000,  'fuels': ['Petrol']},
        'Alto':      {'variants': ['Std', 'LXi', 'VXi'],                       'base_price': 350000,  'fuels': ['Petrol', 'CNG']},
        'Alto 800':  {'variants': ['Std', 'LXi', 'VXi', 'VXi+'],               'base_price': 400000,  'fuels': ['Petrol', 'CNG']},
        'Alto K10':  {'variants': ['Std', 'LXi', 'VXi', 'VXi+'],               'base_price': 450000,  'fuels': ['Petrol', 'CNG']},
        'Baleno':    {'variants': ['Sigma', 'Delta', 'Zeta', 'Alpha'],         'base_price': 750000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Brezza':    {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],               'base_price': 950000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Celerio':   {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],               'base_price': 550000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Ciaz':      {'variants': ['Sigma', 'Delta', 'Zeta', 'Alpha'],         'base_price': 900000,  'fuels': ['Petrol', 'Diesel']},
        'Dzire':     {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],               'base_price': 700000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Eeco':      {'variants': ['5 Seater', '7 Seater'],                    'base_price': 550000,  'fuels': ['Petrol', 'CNG']},
        'Ertiga':    {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],               'base_price': 950000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Fronx':     {'variants': ['Sigma', 'Delta', 'Zeta', 'Alpha'],         'base_price': 800000,  'fuels': ['Petrol', 'CNG']},
        'Grand Vitara':{'variants': ['Sigma', 'Delta', 'Zeta', 'Alpha'],       'base_price': 1100000, 'fuels': ['Petrol', 'HEV', 'CNG']},
        'Ignis':     {'variants': ['Sigma', 'Delta', 'Zeta', 'Alpha'],         'base_price': 620000,  'fuels': ['Petrol', 'Diesel']},
        'Invicto':   {'variants': ['Zeta+', 'Alpha+'],                         'base_price': 2500000, 'fuels': ['HEV']},
        'Jimny':     {'variants': ['Zeta', 'Alpha'],                           'base_price': 1300000, 'fuels': ['Petrol']},
        'Omni':      {'variants': ['5 Seater', '8 Seater'],                    'base_price': 300000,  'fuels': ['Petrol', 'CNG']},
        'Ritz':      {'variants': ['LXi', 'VXi', 'ZXi'],                       'base_price': 450000,  'fuels': ['Petrol', 'Diesel']},
        'S-Cross':   {'variants': ['Sigma', 'Delta', 'Zeta', 'Alpha'],         'base_price': 850000,  'fuels': ['Petrol', 'Diesel']},
        'S-Presso':  {'variants': ['Std', 'LXi', 'VXi', 'VXi+'],               'base_price': 500000,  'fuels': ['Petrol', 'CNG']},
        'Swift':     {'variants': ['LDi', 'LXi', 'VDi', 'VXi', 'ZDi', 'ZXi', 'ZXi+'], 'base_price': 700000, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'SX4':       {'variants': ['VXi', 'ZXi'],                              'base_price': 650000,  'fuels': ['Petrol']},
        'Wagon R':   {'variants': ['LXi', 'VXi', 'ZXi', 'ZXi+'],               'base_price': 580000,  'fuels': ['Petrol', 'CNG']},
        'XL6':       {'variants': ['Zeta', 'Alpha', 'Alpha+'],                 'base_price': 1250000, 'fuels': ['Petrol', 'CNG']},
        'Zen Estilo':{'variants': ['LXi', 'VXi'],                              'base_price': 350000,  'fuels': ['Petrol']},
    },
    'Mercedes-Benz': {
        'A-Class':   {'variants': ['A 200', 'A 200 d'],                        'base_price': 4500000, 'fuels': ['Petrol', 'Diesel']},
        'B-Class':   {'variants': ['B 180', 'B 200 CDI'],                      'base_price': 3500000, 'fuels': ['Petrol', 'Diesel']},
        'C-Class':   {'variants': ['C 200', 'C 220 d', 'C 300'],               'base_price': 5500000, 'fuels': ['Petrol', 'Diesel']},
        'CLA':       {'variants': ['CLA 200', 'CLA 220 d'],                    'base_price': 4500000, 'fuels': ['Petrol', 'Diesel']},
        'E-Class':   {'variants': ['E 200', 'E 220 d', 'E 350'],               'base_price': 7800000, 'fuels': ['Petrol', 'Diesel']},
        'EQS':       {'variants': ['580 4MATIC'],                              'base_price': 16500000,'fuels': ['BEV']},
        'GLA':       {'variants': ['GLA 200', 'GLA 220 d'],                    'base_price': 5000000, 'fuels': ['Petrol', 'Diesel']},
        'GLC':       {'variants': ['GLC 220 d', 'GLC 300'],                    'base_price': 7500000, 'fuels': ['Petrol', 'Diesel']},
        'GLE':       {'variants': ['GLE 300 d', 'GLE 450'],                    'base_price': 10000000,'fuels': ['Petrol', 'Diesel']},
        'GLS':       {'variants': ['GLS 450 4MATIC', 'Maybach GLS'],           'base_price': 13500000,'fuels': ['Petrol', 'Diesel']},
        'M-Class':   {'variants': ['ML 250 CDI', 'ML 350'],                    'base_price': 6500000, 'fuels': ['Petrol', 'Diesel']},
        'S-Class':   {'variants': ['S 350d', 'S 450', 'Maybach S 580'],        'base_price': 18500000,'fuels': ['Petrol', 'Diesel']},
    },
    'MG': {
        'Astor':     {'variants': ['Style', 'Super', 'Smart', 'Sharp', 'Savvy'], 'base_price': 1100000, 'fuels': ['Petrol']},
        'Comet EV':  {'variants': ['Executive', 'Exclusive', 'Excite'],        'base_price': 800000,  'fuels': ['BEV']},
        'Gloster':   {'variants': ['Super', 'Smart', 'Sharp', 'Savvy'],        'base_price': 3900000, 'fuels': ['Diesel']},
        'Hector':    {'variants': ['Style', 'Super', 'Smart', 'Sharp'],        'base_price': 1450000, 'fuels': ['Petrol', 'Diesel']},
        'Hector Plus':{'variants': ['Style', 'Super', 'Smart', 'Sharp'],       'base_price': 1700000, 'fuels': ['Petrol', 'Diesel']},
        'Windsor EV':{'variants': ['Excite', 'Exclusive'],                     'base_price': 1400000, 'fuels': ['BEV']},
        'ZS EV':     {'variants': ['Excite', 'Exclusive'],                     'base_price': 2300000, 'fuels': ['BEV']},
    },
    'Nissan': {
        'GT-R':      {'variants': ['Premium Edition'],                         'base_price': 22000000,'fuels': ['Petrol']},
        'Kicks':     {'variants': ['XL', 'XV', 'XV Premium'],                  'base_price': 1000000, 'fuels': ['Petrol', 'Diesel']},
        'Magnite':   {'variants': ['XE', 'XL', 'XV', 'XV Premium'],            'base_price': 650000,  'fuels': ['Petrol']},
        'Micra':     {'variants': ['XE', 'XL', 'XV'],                          'base_price': 550000,  'fuels': ['Petrol', 'Diesel']},
        'Sunny':     {'variants': ['XE', 'XL', 'XV'],                          'base_price': 700000,  'fuels': ['Petrol', 'Diesel']},
        'Terrano':   {'variants': ['XE', 'XL', 'XV'],                          'base_price': 1000000, 'fuels': ['Diesel']},
    },
    'Renault': {
        'Captur':    {'variants': ['RXE', 'RXL', 'RXT', 'Platine'],            'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Duster':    {'variants': ['RXE', 'RXS', 'RXZ'],                       'base_price': 950000,  'fuels': ['Petrol', 'Diesel']},
        'Fluence':   {'variants': ['E2', 'E4'],                                'base_price': 1400000, 'fuels': ['Petrol', 'Diesel']},
        'Kiger':     {'variants': ['RXE', 'RXL', 'RXT', 'RXZ'],                'base_price': 650000,  'fuels': ['Petrol']},
        'Kwid':      {'variants': ['RXE', 'RXL', 'RXT', 'Climber'],            'base_price': 500000,  'fuels': ['Petrol']},
        'Lodgy':     {'variants': ['RXE', 'RXL', 'RXZ', 'Stepway'],            'base_price': 950000,  'fuels': ['Diesel']},
        'Pulse':     {'variants': ['RxE', 'RxL', 'RxZ'],                       'base_price': 500000,  'fuels': ['Petrol', 'Diesel']},
        'Scala':     {'variants': ['RxE', 'RxL', 'RxZ'],                       'base_price': 650000,  'fuels': ['Petrol', 'Diesel']},
        'Triber':    {'variants': ['RXE', 'RXL', 'RXT', 'RXZ'],                'base_price': 600000,  'fuels': ['Petrol']},
    },
    'Skoda': {
        'Fabia':     {'variants': ['Active', 'Ambition', 'Elegance'],          'base_price': 600000,  'fuels': ['Petrol', 'Diesel']},
        'Kodiaq':    {'variants': ['Style', 'Sportline', 'L&K'],               'base_price': 4000000, 'fuels': ['Petrol', 'Diesel']},
        'Kushaq':    {'variants': ['Active', 'Ambition', 'Style', 'Monte Carlo'], 'base_price': 1200000, 'fuels': ['Petrol']},
        'Laura':     {'variants': ['Ambiente', 'Elegance', 'L&K'],             'base_price': 1400000, 'fuels': ['Petrol', 'Diesel']},
        'Octavia':   {'variants': ['Style', 'L&K'],                            'base_price': 2700000, 'fuels': ['Petrol', 'Diesel']},
        'Rapid':     {'variants': ['Active', 'Ambition', 'Style', 'Monte Carlo'], 'base_price': 900000, 'fuels': ['Petrol', 'Diesel']},
        'Slavia':    {'variants': ['Active', 'Ambition', 'Style', 'Monte Carlo'], 'base_price': 1100000, 'fuels': ['Petrol']},
        'Superb':    {'variants': ['Sportline', 'L&K'],                        'base_price': 5700000, 'fuels': ['Petrol', 'Diesel']},
        'Yeti':      {'variants': ['Active', 'Ambition', 'Elegance'],          'base_price': 1800000, 'fuels': ['Diesel']},
    },
    'Tata': {
        'Altroz':    {'variants': ['XE', 'XM', 'XT', 'XZ', 'XZ+'],             'base_price': 680000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Bolt':      {'variants': ['XE', 'XM', 'XT', 'XMS'],                   'base_price': 500000,  'fuels': ['Petrol', 'Diesel']},
        'Curvv':     {'variants': ['Smart', 'Pure', 'Creative', 'Accomplished'], 'base_price': 1100000, 'fuels': ['Petrol', 'Diesel']},
        'Curvv EV':  {'variants': ['Creative', 'Accomplished', 'Empowered'],   'base_price': 1800000, 'fuels': ['BEV']},
        'Harrier':   {'variants': ['Smart', 'Pure', 'Adventure', 'Fearless'],  'base_price': 1550000, 'fuels': ['Diesel']},
        'Hexa':      {'variants': ['XE', 'XM', 'XT', 'XT4x4'],                 'base_price': 1800000, 'fuels': ['Diesel']},
        'Indica':    {'variants': ['LS', 'LX', 'LXi', 'VX'],                   'base_price': 350000,  'fuels': ['Petrol', 'Diesel']},
        'Indigo':    {'variants': ['LS', 'LX', 'VX'],                          'base_price': 400000,  'fuels': ['Petrol', 'Diesel']},
        'Manza':     {'variants': ['Aqua', 'Aura', 'Club Class'],              'base_price': 500000,  'fuels': ['Petrol', 'Diesel']},
        'Nano':      {'variants': ['Std', 'CX', 'LX', 'XT'],                   'base_price': 200000,  'fuels': ['Petrol']},
        'Nexon':     {'variants': ['Smart', 'Pure', 'Creative', 'Fearless'],   'base_price': 900000,  'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Nexon EV':  {'variants': ['Creative', 'Fearless', 'Empowered'],       'base_price': 1500000, 'fuels': ['BEV']},
        'Punch':     {'variants': ['Pure', 'Adventure', 'Accomplished', 'Creative'], 'base_price': 650000, 'fuels': ['Petrol', 'CNG']},
        'Punch EV':  {'variants': ['Smart', 'Adventure', 'Empowered'],         'base_price': 1100000, 'fuels': ['BEV']},
        'Safari':    {'variants': ['Smart', 'Pure', 'Adventure', 'Fearless'],  'base_price': 1650000, 'fuels': ['Diesel']},
        'Safari Storme':{'variants': ['LX', 'EX', 'VX'],                       'base_price': 1100000, 'fuels': ['Diesel']},
        'Sumo':      {'variants': ['CX', 'EX', 'GX'],                          'base_price': 600000,  'fuels': ['Diesel']},
        'Tiago':     {'variants': ['XE', 'XM', 'XT', 'XZ', 'XZ+'],             'base_price': 550000,  'fuels': ['Petrol', 'CNG']},
        'Tiago EV':  {'variants': ['XE', 'XT', 'XZ+', 'XZ+ LUX'],              'base_price': 850000,  'fuels': ['BEV']},
        'Tigor':     {'variants': ['XE', 'XM', 'XT', 'XZ', 'XZ+'],             'base_price': 620000,  'fuels': ['Petrol', 'CNG']},
        'Zest':      {'variants': ['XE', 'XM', 'XT', 'XMA'],                   'base_price': 550000,  'fuels': ['Petrol', 'Diesel']},
    },
    'Toyota': {
        'Camry':     {'variants': ['Hybrid'],                                  'base_price': 4600000, 'fuels': ['Petrol', 'HEV']},
        'Corolla Altis':{'variants': ['J', 'G', 'GL', 'VL'],                   'base_price': 1800000, 'fuels': ['Petrol', 'Diesel']},
        'Etios':     {'variants': ['J', 'G', 'V', 'VX'],                       'base_price': 600000,  'fuels': ['Petrol', 'Diesel']},
        'Etios Liva':{'variants': ['J', 'G', 'V', 'VX'],                       'base_price': 550000,  'fuels': ['Petrol', 'Diesel']},
        'Fortuner':  {'variants': ['4x2 AT', '4x4 AT', 'Legender', 'GR-Sport'],'base_price': 3400000, 'fuels': ['Petrol', 'Diesel']},
        'Glanza':    {'variants': ['E', 'S', 'G', 'V'],                        'base_price': 700000,  'fuels': ['Petrol', 'CNG']},
        'Hilux':     {'variants': ['Standard', 'High'],                        'base_price': 3100000, 'fuels': ['Diesel']},
        'Innova':    {'variants': ['GX', 'VX', 'ZX'],                          'base_price': 1400000, 'fuels': ['Diesel']},
        'Innova Crysta':{'variants': ['GX', 'VX', 'ZX'],                       'base_price': 2000000, 'fuels': ['Petrol', 'Diesel']},
        'Innova Hycross':{'variants': ['GX', 'VX', 'ZX', 'ZX(O)'],             'base_price': 1900000, 'fuels': ['Petrol', 'HEV']},
        'Land Cruiser':{'variants': ['LC300', 'LC200'],                        'base_price': 22000000,'fuels': ['Petrol', 'Diesel']},
        'Prado':     {'variants': ['VX', 'VX-L'],                              'base_price': 9000000, 'fuels': ['Diesel']},
        'Urban Cruiser':{'variants': ['Mid', 'High', 'Premium'],               'base_price': 850000,  'fuels': ['Petrol']},
        'Urban Cruiser Hyryder':{'variants': ['E', 'S', 'G', 'V'],             'base_price': 1100000, 'fuels': ['Petrol', 'HEV', 'CNG']},
        'Urban Cruiser Taisor':{'variants': ['E', 'S', 'G', 'V'],              'base_price': 750000,  'fuels': ['Petrol', 'CNG']},
        'Vellfire':  {'variants': ['VIP Lounge'],                              'base_price': 12000000,'fuels': ['HEV']},
        'Yaris':     {'variants': ['J', 'G', 'V', 'VX'],                       'base_price': 950000,  'fuels': ['Petrol']},
    },
    'Volkswagen': {
        'Ameo':      {'variants': ['Trendline', 'Comfortline', 'Highline'],    'base_price': 600000,  'fuels': ['Petrol', 'Diesel']},
        'Beetle':    {'variants': ['Standard'],                                'base_price': 2800000, 'fuels': ['Petrol']},
        'Jetta':     {'variants': ['Trendline', 'Comfortline', 'Highline'],    'base_price': 1500000, 'fuels': ['Petrol', 'Diesel']},
        'Passat':    {'variants': ['Comfortline', 'Highline'],                 'base_price': 2800000, 'fuels': ['Petrol', 'Diesel']},
        'Polo':      {'variants': ['Trendline', 'Comfortline', 'Highline', 'GT TSI'], 'base_price': 700000,  'fuels': ['Petrol', 'Diesel']},
        'Taigun':    {'variants': ['Comfortline', 'Highline', 'Topline', 'GT'],'base_price': 1200000, 'fuels': ['Petrol']},
        'Tiguan':    {'variants': ['Elegance', 'R-Line'],                      'base_price': 3700000, 'fuels': ['Petrol', 'Diesel']},
        'Vento':     {'variants': ['Trendline', 'Comfortline', 'Highline'],    'base_price': 900000,  'fuels': ['Petrol', 'Diesel']},
        'Virtus':    {'variants': ['Comfortline', 'Highline', 'Topline', 'GT'],'base_price': 1150000, 'fuels': ['Petrol']},
    },
    'Volvo': {
        'C40 Recharge':{'variants': ['Ultimate'],                              'base_price': 6300000, 'fuels': ['BEV']},
        'S60':       {'variants': ['Momentum', 'Inscription'],                 'base_price': 4500000, 'fuels': ['Petrol', 'Diesel']},
        'S90':       {'variants': ['Ultimate'],                                'base_price': 6800000, 'fuels': ['Petrol']},
        'V40':       {'variants': ['Kinetic', 'Momentum'],                     'base_price': 3000000, 'fuels': ['Diesel']},
        'XC40':      {'variants': ['Ultimate'],                                'base_price': 5000000, 'fuels': ['Petrol']},
        'XC40 Recharge':{'variants': ['Ultimate'],                             'base_price': 5600000, 'fuels': ['BEV']},
        'XC60':      {'variants': ['Plus', 'Ultimate'],                        'base_price': 6700000, 'fuels': ['Petrol', 'Diesel']},
        'XC90':      {'variants': ['Ultimate'],                                'base_price': 9800000, 'fuels': ['Petrol', 'Diesel', 'HEV']},
    },
}


# ============================================================
# HELPERS FOR DROPDOWN POPULATION
# ============================================================

def get_makes():
    return sorted(CAR_DATA.keys())

def get_models(make: str):
    if make not in CAR_DATA:
        return []
    return sorted(CAR_DATA[make].keys())

def get_variants(make: str, model: str):
    if make not in CAR_DATA or model not in CAR_DATA[make]:
        return []
    return sorted(CAR_DATA[make][model]['variants'])

def get_fuels(make: str, model: str):
    if make not in CAR_DATA or model not in CAR_DATA[make]:
        return []
    fuel_order = ['Petrol', 'Diesel', 'CNG', 'HEV', 'PHEV', 'BEV']
    model_fuels = CAR_DATA[make][model].get('fuels', [])
    return [f for f in fuel_order if f in model_fuels]

def get_base_price(make: str, model: str):
    if make not in CAR_DATA or model not in CAR_DATA[make]:
        return None
    return CAR_DATA[make][model].get('base_price')


# ============================================================
# PRICING FORMULA (HYBRID)
# ============================================================

def compute_base_valuation(make, model, variant, fuel, year, mileage, condition, owner):
    base = get_base_price(make, model)
    if base is None:
        return None

    age = max(0, CURRENT_YEAR - int(year))
    age = min(age, 15)

    if age == 0:
        dep_factor = 1.00
    elif age == 1:
        dep_factor = 0.85
    else:
        dep_factor = 0.85 * (0.90 ** (age - 1))

    price = base * dep_factor

    try:
        mileage = int(mileage or 0)
    except (TypeError, ValueError):
        mileage = 0
    expected_km = age * EXPECTED_KM_PER_YEAR
    excess_km = max(0, mileage - expected_km)
    mileage_penalty = (excess_km / 10000) * 0.02
    mileage_penalty = min(mileage_penalty, 0.25)
    price *= (1 - mileage_penalty)

    condition_map = {'Excellent': 1.05, 'Good': 1.00, 'Fair': 0.90}
    price *= condition_map.get(condition, 1.00)

    owner_map = {'1st Owner': 1.00, '2nd Owner': 0.95, '3rd Owner or more': 0.90}
    price *= owner_map.get(owner, 1.00)

    fuel_map = {'Petrol': 1.00, 'Diesel': 1.05, 'CNG': 0.97,
                'HEV': 1.05, 'PHEV': 1.08, 'BEV': 1.10}
    price *= fuel_map.get(fuel, 1.00)

    variants = get_variants(make, model)
    if variants and variant in variants:
        idx = variants.index(variant)
        n = len(variants)
        if n == 1:
            variant_adj = 1.00
        else:
            pct = (idx / (n - 1)) * 0.15
            variant_adj = 1.00 + pct
        price *= variant_adj

    return int(round(price))


def compute_price_range(estimated_price):
    if estimated_price is None:
        return (None, None)
    low = int(round(estimated_price * 0.92))
    high = int(round(estimated_price * 1.08))
    return (low, high)


def adjust_with_deals(estimated_price, similar_deals):
    if not similar_deals or estimated_price is None:
        return estimated_price, 50

    n = len(similar_deals)
    sorted_prices = sorted(similar_deals)
    median = sorted_prices[n // 2] if n % 2 == 1 else (sorted_prices[n // 2 - 1] + sorted_prices[n // 2]) // 2

    if n >= 20:
        real_weight = 0.7
    elif n >= 10:
        real_weight = 0.55
    elif n >= 5:
        real_weight = 0.4
    else:
        real_weight = 0.25

    adjusted = int(round(estimated_price * (1 - real_weight) + median * real_weight))
    confidence = min(95, 50 + n * 2)
    return adjusted, confidence
