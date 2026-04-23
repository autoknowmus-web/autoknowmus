"""
AutoKnowMus — Used car master data + hybrid pricing engine.

Architecture:
  CAR_DATA[make][model] = {
      'variants': {variant_name: variant_base_price, ...}  # trim order (base→top)
      'fuels':    [Petrol, Diesel, ...]                    # display order
  }

Pricing pipeline (per valuation):
  1. Look up variant_base_price for (make, model, variant)
  2. Apply age depreciation (Year 0: 1.00, Year 1: 0.85, then ×0.90/yr, cap 15)
  3. Apply mileage penalty (-2% per 10k km over age×10k, cap 25%)
  4. Apply condition multiplier (Excellent 1.05, Good 1.00, Fair 0.90)
  5. Apply owner multiplier (1st 1.00, 2nd 0.95, 3rd+ 0.90)
  6. Apply fuel multiplier (Petrol 1.00, Diesel 1.05, CNG 0.97,
                           HEV 1.05, PHEV 1.08, BEV 1.10)
  7. Blend with recent verified deals via adjust_with_deals()
     (phase-based weighting, guardrails, outlier trimming)
  8. Compute price range width based on phase (±12% → ±5%)

Base-price calibration version: 2026-Q1 (update quarterly).
"""

from datetime import datetime

CURRENT_YEAR = datetime.now().year
EXPECTED_KM_PER_YEAR = 10000

# Version marker — surface in admin dashboard & footer
BASE_PRICE_DATA_VERSION = "2026-Q1"
BASE_PRICE_LAST_UPDATED = "2026-01-15"

# ============================================================
# PHASE SYSTEM — per-model blending of formula + real deals
# ============================================================

PHASE_THRESHOLDS = {
    # phase: (min_deals_up, min_distinct_users_up, min_deals_down)
    2: (5,  4,  3),
    3: (15, 10, 10),
    4: (50, 25, 35),
}

PHASE_BLEND = {
    1: {'formula_weight': 1.00, 'real_weight': 0.00,
        'conf_base': 55, 'range_pct': 0.12,
        'badge': 'Estimate',
        'badge_detail': 'Based on our pricing model',
        'tooltip': ('This valuation is based on our pricing model calibrated '
                    'for the Bangalore used-car market. As more verified '
                    'transactions are submitted for this car, accuracy will '
                    'improve.')},
    2: {'formula_weight': 0.70, 'real_weight': 0.30,
        'conf_base': 68, 'range_pct': 0.09,
        'badge': 'Calibrated',
        'badge_detail': 'Adjusted with recent market data',
        'tooltip': ('This valuation has been calibrated using recent verified '
                    'transactions for this car in Bangalore, giving a more '
                    'accurate market-aligned price.')},
    3: {'formula_weight': 0.35, 'real_weight': 0.65,
        'conf_base': 82, 'range_pct': 0.07,
        'badge': 'Market-verified',
        'badge_detail': 'Based primarily on verified transactions',
        'tooltip': ('This valuation is primarily based on verified transactions '
                    'for this car in the Bangalore market over the last 6 '
                    'months.')},
    4: {'formula_weight': 0.10, 'real_weight': 0.90,
        'conf_base': 92, 'range_pct': 0.05,
        'badge': 'Real-Market Pricing',
        'badge_detail': 'Based on extensive verified transaction data',
        'tooltip': ('This valuation reflects the real Bangalore market price, '
                    'based on a large volume of recent verified transactions.')},
}

GUARDRAIL_MAX_DEVIATION = 0.20  # ±20% cap on real-deal deviation from formula
OUTLIER_TRIM_FRAC = 0.10
OUTLIER_TRIM_MIN_DEALS = 20
CONF_CEILING = {1: 62, 2: 77, 3: 90, 4: 96}


# ============================================================
# BRAND DEMAND TIERS
# ============================================================

HIGH_DEMAND_BRANDS = {'Maruti Suzuki', 'Hyundai', 'Honda', 'Toyota', 'Tata', 'Kia', 'Mahindra'}
MEDIUM_DEMAND_BRANDS = {'Ford', 'Renault', 'Nissan', 'Volkswagen', 'Skoda', 'MG'}
LUXURY_BRANDS = {'Audi', 'BMW', 'Mercedes-Benz', 'Jaguar', 'Land Rover', 'Lexus', 'Volvo'}


# ============================================================
# USED-CAR MASTER DATA — 20 brands, variant-level base prices
# Variants in TRIM ORDER (base → top); source dict order preserved.
# ============================================================

CAR_DATA = {

    'Audi': {
        'A3':     {'variants': {'Premium': 3200000, 'Premium Plus': 4008000, 'Technology': 5200000}, 'fuels': ['Petrol', 'Diesel']},
        'A4':     {'variants': {'Premium': 3600000, 'Premium Plus': 4509000, 'Technology': 5850000}, 'fuels': ['Petrol', 'Diesel']},
        'A6':     {'variants': {'Premium Plus': 5525000, 'Technology': 8450000}, 'fuels': ['Petrol', 'Diesel']},
        'A8':     {'variants': {'L Celebration': 13175000, 'L 55 TFSI': 20150000}, 'fuels': ['Petrol', 'Diesel']},
        'Q3':     {'variants': {'Premium': 3600000, 'Premium Plus': 4509000, 'Technology': 5850000}, 'fuels': ['Petrol', 'Diesel']},
        'Q5':     {'variants': {'Premium Plus': 5780000, 'Technology': 8840000}, 'fuels': ['Petrol', 'Diesel']},
        'Q7':     {'variants': {'Premium Plus': 7480000, 'Technology': 11440000}, 'fuels': ['Petrol', 'Diesel']},
        'e-tron': {'variants': {'55 Quattro': 10580000, 'Sportback': 12880000}, 'fuels': ['BEV']},
    },

    'BMW': {
        '3 Series': {'variants': {'320d': 4493000, '330i': 5216000, 'M340i': 6760000}, 'fuels': ['Petrol', 'Diesel']},
        '5 Series': {'variants': {'520d': 5872000, '530i': 8160000}, 'fuels': ['Petrol', 'Diesel']},
        '7 Series': {'variants': {'730Ld': 12528000, '740Li': 14500000, 'M760Li': 18850000}, 'fuels': ['Petrol', 'Diesel']},
        'X1':       {'variants': {'sDrive18i': 4080000, 'sDrive20d': 4824000, 'xDrive20d': 5994000}, 'fuels': ['Petrol', 'Diesel']},
        'X3':       {'variants': {'xDrive20d': 5787000, 'xDrive30i': 6720000, 'M40i': 8710000}, 'fuels': ['Petrol', 'Diesel']},
        'X5':       {'variants': {'xDrive30d': 8208000, 'xDrive40i': 11400000}, 'fuels': ['Petrol', 'Diesel']},
        'X7':       {'variants': {'xDrive30d': 10541000, 'xDrive40i': 12224000, 'M50i': 15860000}, 'fuels': ['Petrol', 'Diesel']},
        'iX':       {'variants': {'xDrive40': 11040000, 'xDrive50': 13440000}, 'fuels': ['BEV']},
    },

    'Ford': {
        'Aspire':    {'variants': {'Ambiente': 552000, 'Trend': 610000, 'Titanium': 793000}, 'fuels': ['Petrol', 'Diesel']},
        'EcoSport':  {'variants': {'Ambiente': 765000, 'Trend': 846000, 'Titanium': 962000, 'Titanium+': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'Endeavour': {'variants': {'Titanium': 2805000, 'Titanium+': 3099000, 'Sport': 3526000}, 'fuels': ['Diesel']},
        'Figo':      {'variants': {'Ambiente': 510000, 'Trend': 563000, 'Titanium': 732000}, 'fuels': ['Petrol', 'Diesel']},
        'Freestyle': {'variants': {'Ambiente': 595000, 'Trend': 658000, 'Titanium': 748000, 'Titanium+': 854000}, 'fuels': ['Petrol', 'Diesel']},
    },

    'Honda': {
        'Amaze':   {'variants': {'E': 638000, 'S': 705000, 'V': 803000, 'VX': 915000}, 'fuels': ['Petrol', 'Diesel']},
        'BR-V':    {'variants': {'S': 850000, 'V': 1000000, 'VX': 1220000}, 'fuels': ['Petrol', 'Diesel']},
        'City':    {'variants': {'V': 1105000, 'VX': 1300000, 'ZX': 1586000}, 'fuels': ['Petrol', 'Diesel', 'HEV']},
        'Civic':   {'variants': {'V': 1530000, 'VX': 1800000, 'ZX': 2196000}, 'fuels': ['Petrol', 'Diesel']},
        'CR-V':    {'variants': {'2WD': 2610000, '4WD': 3190000}, 'fuels': ['Petrol', 'Diesel']},
        'Elevate': {'variants': {'SV': 1062000, 'V': 1172000, 'VX': 1336000, 'ZX': 1525000}, 'fuels': ['Petrol']},
        'Jazz':    {'variants': {'V': 722000, 'VX': 850000, 'ZX': 1037000}, 'fuels': ['Petrol', 'Diesel']},
        'Mobilio': {'variants': {'S': 765000, 'V': 900000, 'RS': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'WR-V':    {'variants': {'S': 808000, 'SV': 891000, 'VX': 1159000}, 'fuels': ['Petrol', 'Diesel']},
    },

    'Hyundai': {
        'Alcazar':         {'variants': {'Prestige': 1445000, 'Signature': 1700000, 'Platinum': 2074000}, 'fuels': ['Petrol', 'Diesel']},
        'Aura':            {'variants': {'E': 553000, 'S': 610000, 'SX': 694000, 'SX(O)': 793000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Creta':           {'variants': {'E': 935000, 'EX': 1002000, 'S': 1100000, 'SX': 1215000, 'SX(O)': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Elantra':         {'variants': {'S': 1530000, 'SX': 1800000, 'SX(O)': 2196000}, 'fuels': ['Petrol', 'Diesel']},
        'Elite i20':       {'variants': {'Era': 595000, 'Magna': 657000, 'Sportz': 748000, 'Asta': 854000}, 'fuels': ['Petrol', 'Diesel']},
        'Eon':             {'variants': {'Era': 340000, 'Magna': 400000, 'Sportz': 488000}, 'fuels': ['Petrol']},
        'Exter':           {'variants': {'EX': 553000, 'S': 610000, 'SX': 694000, 'SX(O)': 793000}, 'fuels': ['Petrol', 'CNG']},
        'Grand i10':       {'variants': {'Era': 468000, 'Magna': 517000, 'Sportz': 587000, 'Asta': 671000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Grand i10 Nios':  {'variants': {'Era': 510000, 'Magna': 563000, 'Sportz': 640000, 'Asta': 732000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'i10':             {'variants': {'Era': 425000, 'Magna': 470000, 'Sportz': 534000, 'Asta': 610000}, 'fuels': ['Petrol']},
        'i20':             {'variants': {'Era': 637000, 'Magna': 704000, 'Sportz': 801000, 'Asta': 915000}, 'fuels': ['Petrol', 'Diesel']},
        'i20 Active':      {'variants': {'S': 720000, 'SX': 976000}, 'fuels': ['Petrol', 'Diesel']},
        'Ioniq 5':         {'variants': {'RWD': 4232000, 'AWD': 5152000}, 'fuels': ['BEV']},
        'Kona Electric':   {'variants': {'Premium': 2208000, 'Premium Dual Tone': 2688000}, 'fuels': ['BEV']},
        'Santa Fe':        {'variants': {'GLS': 2550000, 'Signature': 3660000}, 'fuels': ['Diesel']},
        'Santro':          {'variants': {'D-Lite': 425000, 'Era': 456000, 'Magna': 488000, 'Sportz': 534000, 'Asta': 610000}, 'fuels': ['Petrol', 'CNG']},
        'Tucson':          {'variants': {'Platinum': 2550000, 'Signature': 3660000}, 'fuels': ['Petrol', 'Diesel']},
        'Venue':           {'variants': {'E': 680000, 'S': 751000, 'SX': 854000, 'SX(O)': 976000}, 'fuels': ['Petrol', 'Diesel']},
        'Verna':           {'variants': {'EX': 935000, 'S': 1002000, 'SX': 1215000, 'SX(O)': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Xcent':           {'variants': {'E': 468000, 'S': 545000, 'SX': 671000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
    },

    'Jaguar': {
        'F-Pace': {'variants': {'S': 6560000, 'SE': 8224000, 'HSE': 10660000}, 'fuels': ['Petrol', 'Diesel']},
        'I-Pace': {'variants': {'S': 11592000, 'SE': 12600000, 'HSE': 14112000}, 'fuels': ['BEV']},
        'XE':     {'variants': {'S': 4000000, 'SE': 5015000, 'HSE': 6500000}, 'fuels': ['Petrol', 'Diesel']},
        'XF':     {'variants': {'Prestige': 6000000, 'Portfolio': 7523000, 'R-Sport': 9750000}, 'fuels': ['Petrol', 'Diesel']},
        'XJ':     {'variants': {'L Portfolio': 11000000}, 'fuels': ['Petrol', 'Diesel']},
    },

    'Kia': {
        'Carens':   {'variants': {'Premium': 935000, 'Prestige': 1031000, 'Luxury': 1175000, 'Luxury Plus': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Carnival': {'variants': {'Premium': 2975000, 'Prestige': 3508000, 'Limousine': 4270000}, 'fuels': ['Diesel']},
        'EV6':      {'variants': {'GT Line': 5520000, 'GT Line AWD': 6720000}, 'fuels': ['BEV']},
        'EV9':      {'variants': {'GT Line': 11960000, 'GT Line AWD': 14560000}, 'fuels': ['BEV']},
        'Seltos':   {'variants': {'HTE': 935000, 'HTK': 993000, 'HTX': 1079000, 'GTX+': 1185000, 'X-Line': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Sonet':    {'variants': {'HTE': 680000, 'HTK': 751000, 'HTX': 854000, 'GTX+': 976000}, 'fuels': ['Petrol', 'Diesel']},
    },

    'Land Rover': {
        'Defender':              {'variants': {'S': 7600000, 'SE': 9526000, 'HSE': 11115000, 'X': 12350000}, 'fuels': ['Petrol', 'Diesel']},
        'Discovery':             {'variants': {'S': 7200000, 'SE': 9027000, 'HSE': 11700000}, 'fuels': ['Petrol', 'Diesel']},
        'Discovery Sport':       {'variants': {'S': 5200000, 'SE': 6519000, 'HSE': 8450000}, 'fuels': ['Petrol', 'Diesel']},
        'Freelander':            {'variants': {'SE': 3870000, 'HSE': 5850000}, 'fuels': ['Diesel']},
        'Range Rover':           {'variants': {'SE': 18400000, 'HSE': 23061000, 'Autobiography': 29900000}, 'fuels': ['Petrol', 'Diesel']},
        'Range Rover Evoque':    {'variants': {'S': 5600000, 'SE': 7021000, 'HSE': 9100000}, 'fuels': ['Petrol', 'Diesel']},
        'Range Rover Sport':     {'variants': {'SE': 10800000, 'HSE': 13541000, 'Autobiography': 17550000}, 'fuels': ['Petrol', 'Diesel']},
        'Range Rover Velar':     {'variants': {'S': 6800000, 'SE': 8526000, 'HSE': 11050000}, 'fuels': ['Petrol', 'Diesel']},
    },

    'Lexus': {
        'ES': {'variants': {'Luxury': 5200000, 'F-Sport': 8450000}, 'fuels': ['HEV']},
        'LS': {'variants': {'Ultra Luxury': 20000000}, 'fuels': ['HEV']},
        'LX': {'variants': {'VX': 22400000, 'Ultra Luxury': 36400000}, 'fuels': ['Petrol', 'Diesel']},
        'NX': {'variants': {'Luxury': 5760000, 'F-Sport': 9360000}, 'fuels': ['HEV']},
        'RX': {'variants': {'Luxury': 7920000, 'F-Sport': 12870000}, 'fuels': ['HEV']},
    },

    'Mahindra': {
        'Alturas G4': {'variants': {'4x2 AT': 2550000, '4x4 AT': 3660000}, 'fuels': ['Diesel']},
        'BE 6e':      {'variants': {'Pack 1': 1748000, 'Pack 2': 1900000, 'Pack 3': 2128000}, 'fuels': ['BEV']},
        'Bolero':     {'variants': {'B4': 808000, 'B6': 892000, 'B6(O)': 1159000}, 'fuels': ['Diesel']},
        'KUV100':     {'variants': {'K2': 510000, 'K4': 563000, 'K6': 640000, 'K8': 732000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Marazzo':    {'variants': {'M2': 935000, 'M4': 1031000, 'M6': 1175000, 'M8': 1342000}, 'fuels': ['Diesel']},
        'Scorpio':    {'variants': {'S3': 1020000, 'S5': 1095000, 'S7': 1200000, 'S9': 1335000, 'S11': 1464000}, 'fuels': ['Diesel']},
        'Scorpio-N':  {'variants': {'Z2': 1190000, 'Z4': 1278000, 'Z6': 1400000, 'Z8': 1558000, 'Z8 L': 1708000}, 'fuels': ['Petrol', 'Diesel']},
        'Thar':       {'variants': {'AX': 1190000, 'AX(O)': 1400000, 'LX': 1708000}, 'fuels': ['Petrol', 'Diesel']},
        'TUV300':     {'variants': {'T4': 680000, 'T6': 751000, 'T8': 854000, 'T10': 976000}, 'fuels': ['Diesel']},
        'XEV 9e':     {'variants': {'Pack 1': 2024000, 'Pack 2': 2200000, 'Pack 3': 2464000}, 'fuels': ['BEV']},
        'XUV 3XO':    {'variants': {'MX1': 680000, 'MX3': 727000, 'AX5': 800000, 'AX7': 888000, 'AX7 L': 976000}, 'fuels': ['Petrol', 'Diesel']},
        'XUV300':     {'variants': {'W4': 765000, 'W6': 846000, 'W8': 962000, 'W8(O)': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'XUV400':     {'variants': {'EC': 1472000, 'EL': 1792000}, 'fuels': ['BEV']},
        'XUV500':     {'variants': {'W5': 1190000, 'W7': 1306000, 'W9': 1446000, 'W11': 1708000}, 'fuels': ['Diesel']},
        'XUV700':     {'variants': {'MX': 1190000, 'AX3': 1278000, 'AX5': 1400000, 'AX7': 1558000, 'AX7 L': 1708000}, 'fuels': ['Petrol', 'Diesel']},
        'Xylo':       {'variants': {'D2': 723000, 'D4': 772000, 'H4': 835000, 'H8': 925000, 'H9': 1037000}, 'fuels': ['Diesel']},
    },

    'Maruti Suzuki': {
        'A-Star':     {'variants': {'LXi': 382000, 'VXi': 450000, 'ZXi': 549000}, 'fuels': ['Petrol']},
        'Alto':       {'variants': {'Std': 297000, 'LXi': 350000, 'VXi': 427000}, 'fuels': ['Petrol', 'CNG']},
        'Alto 800':   {'variants': {'Std': 340000, 'LXi': 376000, 'VXi': 427000, 'VXi+': 488000}, 'fuels': ['Petrol', 'CNG']},
        'Alto K10':   {'variants': {'Std': 382000, 'LXi': 423000, 'VXi': 481000, 'VXi+': 549000}, 'fuels': ['Petrol', 'CNG']},
        'Baleno':     {'variants': {'Sigma': 637000, 'Delta': 704000, 'Zeta': 801000, 'Alpha': 915000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Brezza':     {'variants': {'LXi': 808000, 'VXi': 892000, 'ZXi': 1015000, 'ZXi+': 1159000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Celerio':    {'variants': {'LXi': 468000, 'VXi': 517000, 'ZXi': 587000, 'ZXi+': 671000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Ciaz':       {'variants': {'Sigma': 765000, 'Delta': 846000, 'Zeta': 962000, 'Alpha': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'Dzire':      {'variants': {'LXi': 595000, 'VXi': 657000, 'ZXi': 748000, 'ZXi+': 854000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Eeco':       {'variants': {'5 Seater': 495000, '7 Seater': 605000}, 'fuels': ['Petrol', 'CNG']},
        'Ertiga':     {'variants': {'LXi': 808000, 'VXi': 892000, 'ZXi': 1015000, 'ZXi+': 1159000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Fronx':      {'variants': {'Sigma': 680000, 'Delta': 751000, 'Zeta': 854000, 'Alpha': 976000}, 'fuels': ['Petrol', 'CNG']},
        'Grand Vitara':{'variants': {'Sigma': 935000, 'Delta': 1031000, 'Zeta': 1175000, 'Alpha': 1342000}, 'fuels': ['Petrol', 'HEV', 'CNG']},
        'Ignis':      {'variants': {'Sigma': 527000, 'Delta': 582000, 'Zeta': 663000, 'Alpha': 756000}, 'fuels': ['Petrol', 'Diesel']},
        'Invicto':    {'variants': {'Zeta+': 2125000, 'Alpha+': 3050000}, 'fuels': ['HEV']},
        'Jimny':      {'variants': {'Zeta': 1105000, 'Alpha': 1586000}, 'fuels': ['Petrol']},
        'Omni':       {'variants': {'5 Seater': 270000, '8 Seater': 330000}, 'fuels': ['Petrol', 'CNG']},
        'Ritz':       {'variants': {'LXi': 382000, 'VXi': 423000, 'ZXi': 549000}, 'fuels': ['Petrol', 'Diesel']},
        'S-Cross':    {'variants': {'Sigma': 722000, 'Delta': 798000, 'Zeta': 908000, 'Alpha': 1037000}, 'fuels': ['Petrol', 'Diesel']},
        'S-Presso':   {'variants': {'Std': 425000, 'LXi': 470000, 'VXi': 534000, 'VXi+': 610000}, 'fuels': ['Petrol', 'CNG']},
        'Swift':      {'variants': {'LXi': 595000, 'LDi': 643000, 'VXi': 700000, 'VDi': 756000, 'ZXi': 799000, 'ZDi': 863000, 'ZXi+': 854000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'SX4':        {'variants': {'VXi': 553000, 'ZXi': 793000}, 'fuels': ['Petrol']},
        'Wagon R':    {'variants': {'LXi': 493000, 'VXi': 545000, 'ZXi': 619000, 'ZXi+': 708000}, 'fuels': ['Petrol', 'CNG']},
        'XL6':        {'variants': {'Zeta': 1063000, 'Alpha': 1250000, 'Alpha+': 1525000}, 'fuels': ['Petrol', 'CNG']},
        'Zen Estilo': {'variants': {'LXi': 297000, 'VXi': 427000}, 'fuels': ['Petrol']},
    },

    'Mercedes-Benz': {
        'A-Class':  {'variants': {'A 200': 3600000, 'A 200 d': 4860000}, 'fuels': ['Petrol', 'Diesel']},
        'B-Class':  {'variants': {'B 180': 2800000, 'B 200 CDI': 3780000}, 'fuels': ['Petrol', 'Diesel']},
        'C-Class':  {'variants': {'C 200': 4400000, 'C 220 d': 5940000, 'C 300': 7150000}, 'fuels': ['Petrol', 'Diesel']},
        'CLA':      {'variants': {'CLA 200': 3600000, 'CLA 220 d': 4860000}, 'fuels': ['Petrol', 'Diesel']},
        'E-Class':  {'variants': {'E 200': 6240000, 'E 220 d': 8424000, 'E 350': 10140000}, 'fuels': ['Petrol', 'Diesel']},
        'EQS':      {'variants': {'580 4MATIC': 16500000}, 'fuels': ['BEV']},
        'GLA':      {'variants': {'GLA 200': 4000000, 'GLA 220 d': 5400000}, 'fuels': ['Petrol', 'Diesel']},
        'GLC':      {'variants': {'GLC 220 d': 6480000, 'GLC 300': 9000000}, 'fuels': ['Petrol', 'Diesel']},
        'GLE':      {'variants': {'GLE 300 d': 8640000, 'GLE 450': 12000000}, 'fuels': ['Petrol', 'Diesel']},
        'GLS':      {'variants': {'GLS 450 4MATIC': 10800000, 'Maybach GLS': 17550000}, 'fuels': ['Petrol', 'Diesel']},
        'M-Class':  {'variants': {'ML 250 CDI': 5616000, 'ML 350': 7150000}, 'fuels': ['Petrol', 'Diesel']},
        'S-Class':  {'variants': {'S 350d': 15984000, 'S 450': 18538000, 'Maybach S 580': 24050000}, 'fuels': ['Petrol', 'Diesel']},
    },

    'MG': {
        'Astor':       {'variants': {'Style': 935000, 'Super': 993000, 'Smart': 1079000, 'Sharp': 1185000, 'Savvy': 1342000}, 'fuels': ['Petrol']},
        'Comet EV':    {'variants': {'Executive': 736000, 'Exclusive': 800000, 'Excite': 896000}, 'fuels': ['BEV']},
        'Gloster':     {'variants': {'Super': 3315000, 'Smart': 3667000, 'Sharp': 4095000, 'Savvy': 4758000}, 'fuels': ['Diesel']},
        'Hector':      {'variants': {'Style': 1233000, 'Super': 1362000, 'Smart': 1549000, 'Sharp': 1769000}, 'fuels': ['Petrol', 'Diesel']},
        'Hector Plus': {'variants': {'Style': 1445000, 'Super': 1598000, 'Smart': 1815000, 'Sharp': 2074000}, 'fuels': ['Petrol', 'Diesel']},
        'Windsor EV':  {'variants': {'Excite': 1288000, 'Exclusive': 1568000}, 'fuels': ['BEV']},
        'ZS EV':       {'variants': {'Excite': 2116000, 'Exclusive': 2576000}, 'fuels': ['BEV']},
    },

    'Nissan': {
        'GT-R':    {'variants': {'Premium Edition': 22000000}, 'fuels': ['Petrol']},
        'Kicks':   {'variants': {'XL': 850000, 'XV': 1000000, 'XV Premium': 1220000}, 'fuels': ['Petrol', 'Diesel']},
        'Magnite': {'variants': {'XE': 553000, 'XL': 610000, 'XV': 694000, 'XV Premium': 793000}, 'fuels': ['Petrol']},
        'Micra':   {'variants': {'XE': 468000, 'XL': 517000, 'XV': 671000}, 'fuels': ['Petrol', 'Diesel']},
        'Sunny':   {'variants': {'XE': 595000, 'XL': 657000, 'XV': 854000}, 'fuels': ['Petrol', 'Diesel']},
        'Terrano': {'variants': {'XE': 850000, 'XL': 939000, 'XV': 1220000}, 'fuels': ['Diesel']},
    },

    'Renault': {
        'Captur':  {'variants': {'RXE': 935000, 'RXL': 1031000, 'RXT': 1175000, 'Platine': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Duster':  {'variants': {'RXE': 808000, 'RXS': 908000, 'RXZ': 1159000}, 'fuels': ['Petrol', 'Diesel']},
        'Fluence': {'variants': {'E2': 1190000, 'E4': 1708000}, 'fuels': ['Petrol', 'Diesel']},
        'Kiger':   {'variants': {'RXE': 553000, 'RXL': 610000, 'RXT': 694000, 'RXZ': 793000}, 'fuels': ['Petrol']},
        'Kwid':    {'variants': {'RXE': 425000, 'RXL': 470000, 'RXT': 534000, 'Climber': 610000}, 'fuels': ['Petrol']},
        'Lodgy':   {'variants': {'RXE': 808000, 'RXL': 892000, 'RXZ': 1015000, 'Stepway': 1159000}, 'fuels': ['Diesel']},
        'Pulse':   {'variants': {'RxE': 425000, 'RxL': 470000, 'RxZ': 610000}, 'fuels': ['Petrol', 'Diesel']},
        'Scala':   {'variants': {'RxE': 553000, 'RxL': 610000, 'RxZ': 793000}, 'fuels': ['Petrol', 'Diesel']},
        'Triber':  {'variants': {'RXE': 510000, 'RXL': 563000, 'RXT': 640000, 'RXZ': 732000}, 'fuels': ['Petrol']},
    },

    'Skoda': {
        'Fabia':    {'variants': {'Active': 510000, 'Ambition': 582000, 'Elegance': 732000}, 'fuels': ['Petrol', 'Diesel']},
        'Kodiaq':   {'variants': {'Style': 3400000, 'Sportline': 3760000, 'L&K': 4880000}, 'fuels': ['Petrol', 'Diesel']},
        'Kushaq':   {'variants': {'Active': 1020000, 'Ambition': 1125000, 'Style': 1280000, 'Monte Carlo': 1464000}, 'fuels': ['Petrol']},
        'Laura':    {'variants': {'Ambiente': 1190000, 'Elegance': 1400000, 'L&K': 1708000}, 'fuels': ['Petrol', 'Diesel']},
        'Octavia':  {'variants': {'Style': 2295000, 'L&K': 3294000}, 'fuels': ['Petrol', 'Diesel']},
        'Rapid':    {'variants': {'Active': 765000, 'Ambition': 846000, 'Style': 962000, 'Monte Carlo': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'Slavia':   {'variants': {'Active': 935000, 'Ambition': 1031000, 'Style': 1175000, 'Monte Carlo': 1342000}, 'fuels': ['Petrol']},
        'Superb':   {'variants': {'Sportline': 4845000, 'L&K': 6954000}, 'fuels': ['Petrol', 'Diesel']},
        'Yeti':     {'variants': {'Active': 1530000, 'Ambition': 1692000, 'Elegance': 2196000}, 'fuels': ['Diesel']},
    },

    'Tata': {
        'Altroz':        {'variants': {'XE': 578000, 'XM': 634000, 'XT': 720000, 'XZ': 823000, 'XZ+': 940000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Bolt':          {'variants': {'XE': 425000, 'XM': 470000, 'XT': 534000, 'XMS': 610000}, 'fuels': ['Petrol', 'Diesel']},
        'Curvv':         {'variants': {'Smart': 935000, 'Pure': 1031000, 'Creative': 1175000, 'Accomplished': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Curvv EV':      {'variants': {'Creative': 1656000, 'Accomplished': 1800000, 'Empowered': 2016000}, 'fuels': ['BEV']},
        'Harrier':       {'variants': {'Smart': 1318000, 'Pure': 1456000, 'Adventure': 1661000, 'Fearless': 1891000}, 'fuels': ['Diesel']},
        'Hexa':          {'variants': {'XE': 1530000, 'XM': 1690000, 'XT': 1923000, 'XT4x4': 2196000}, 'fuels': ['Diesel']},
        'Indica':        {'variants': {'LS': 298000, 'LX': 330000, 'LXi': 376000, 'VX': 427000}, 'fuels': ['Petrol', 'Diesel']},
        'Indigo':        {'variants': {'LS': 340000, 'LX': 376000, 'VX': 488000}, 'fuels': ['Petrol', 'Diesel']},
        'Manza':         {'variants': {'Aqua': 425000, 'Aura': 470000, 'Club Class': 610000}, 'fuels': ['Petrol', 'Diesel']},
        'Nano':          {'variants': {'Std': 170000, 'CX': 188000, 'LX': 214000, 'XT': 244000}, 'fuels': ['Petrol']},
        'Nexon':         {'variants': {'Smart': 765000, 'Pure': 845000, 'Creative': 962000, 'Fearless': 1098000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Nexon EV':      {'variants': {'Creative': 1380000, 'Fearless': 1500000, 'Empowered': 1680000}, 'fuels': ['BEV']},
        'Punch':         {'variants': {'Pure': 553000, 'Adventure': 610000, 'Accomplished': 694000, 'Creative': 793000}, 'fuels': ['Petrol', 'CNG']},
        'Punch EV':      {'variants': {'Smart': 1012000, 'Adventure': 1100000, 'Empowered': 1232000}, 'fuels': ['BEV']},
        'Safari':        {'variants': {'Smart': 1403000, 'Pure': 1550000, 'Adventure': 1768000, 'Fearless': 2013000}, 'fuels': ['Diesel']},
        'Safari Storme': {'variants': {'LX': 935000, 'EX': 1031000, 'VX': 1342000}, 'fuels': ['Diesel']},
        'Sumo':          {'variants': {'CX': 510000, 'EX': 563000, 'GX': 732000}, 'fuels': ['Diesel']},
        'Tiago':         {'variants': {'XE': 468000, 'XM': 514000, 'XT': 584000, 'XZ': 667000, 'XZ+': 761000}, 'fuels': ['Petrol', 'CNG']},
        'Tiago EV':      {'variants': {'XE': 782000, 'XT': 833000, 'XZ+': 922000, 'XZ+ LUX': 1020000}, 'fuels': ['BEV']},
        'Tigor':         {'variants': {'XE': 527000, 'XM': 580000, 'XT': 658000, 'XZ': 752000, 'XZ+': 859000}, 'fuels': ['Petrol', 'CNG']},
        'Zest':          {'variants': {'XE': 468000, 'XM': 517000, 'XT': 587000, 'XMA': 671000}, 'fuels': ['Petrol', 'Diesel']},
    },

    'Toyota': {
        'Camry':                 {'variants': {'Hybrid': 4600000}, 'fuels': ['Petrol', 'HEV']},
        'Corolla Altis':         {'variants': {'J': 1530000, 'G': 1693000, 'GL': 1924000, 'VL': 2196000}, 'fuels': ['Petrol', 'Diesel']},
        'Etios':                 {'variants': {'J': 510000, 'G': 564000, 'V': 641000, 'VX': 732000}, 'fuels': ['Petrol', 'Diesel']},
        'Etios Liva':            {'variants': {'J': 468000, 'G': 517000, 'V': 587000, 'VX': 671000}, 'fuels': ['Petrol', 'Diesel']},
        'Fortuner':              {'variants': {'4x2 AT': 2890000, '4x4 AT': 3196000, 'Legender': 3640000, 'GR-Sport': 4148000}, 'fuels': ['Petrol', 'Diesel']},
        'Glanza':                {'variants': {'E': 595000, 'S': 658000, 'G': 748000, 'V': 854000}, 'fuels': ['Petrol', 'CNG']},
        'Hilux':                 {'variants': {'Standard': 2635000, 'High': 3782000}, 'fuels': ['Diesel']},
        'Innova':                {'variants': {'GX': 1190000, 'VX': 1400000, 'ZX': 1708000}, 'fuels': ['Diesel']},
        'Innova Crysta':         {'variants': {'GX': 1700000, 'VX': 2000000, 'ZX': 2440000}, 'fuels': ['Petrol', 'Diesel']},
        'Innova Hycross':        {'variants': {'GX': 1615000, 'VX': 1785000, 'ZX': 2029000, 'ZX(O)': 2318000}, 'fuels': ['Petrol', 'HEV']},
        'Land Cruiser':          {'variants': {'LC300': 20900000, 'LC200': 26840000}, 'fuels': ['Petrol', 'Diesel']},
        'Prado':                 {'variants': {'VX': 7650000, 'VX-L': 10980000}, 'fuels': ['Diesel']},
        'Urban Cruiser':         {'variants': {'Mid': 723000, 'High': 796000, 'Premium': 1037000}, 'fuels': ['Petrol']},
        'Urban Cruiser Hyryder': {'variants': {'E': 935000, 'S': 1031000, 'G': 1175000, 'V': 1342000}, 'fuels': ['Petrol', 'HEV', 'CNG']},
        'Urban Cruiser Taisor':  {'variants': {'E': 637000, 'S': 704000, 'G': 801000, 'V': 915000}, 'fuels': ['Petrol', 'CNG']},
        'Vellfire':              {'variants': {'VIP Lounge': 12000000}, 'fuels': ['HEV']},
        'Yaris':                 {'variants': {'J': 808000, 'G': 892000, 'V': 1015000, 'VX': 1159000}, 'fuels': ['Petrol']},
    },

    'Volkswagen': {
        'Ameo':   {'variants': {'Trendline': 510000, 'Comfortline': 563000, 'Highline': 732000}, 'fuels': ['Petrol', 'Diesel']},
        'Beetle': {'variants': {'Standard': 2800000}, 'fuels': ['Petrol']},
        'Jetta':  {'variants': {'Trendline': 1275000, 'Comfortline': 1410000, 'Highline': 1830000}, 'fuels': ['Petrol', 'Diesel']},
        'Passat': {'variants': {'Comfortline': 2380000, 'Highline': 3416000}, 'fuels': ['Petrol', 'Diesel']},
        'Polo':   {'variants': {'Trendline': 595000, 'Comfortline': 657000, 'Highline': 748000, 'GT TSI': 854000}, 'fuels': ['Petrol', 'Diesel']},
        'Taigun': {'variants': {'Comfortline': 1020000, 'Highline': 1125000, 'Topline': 1280000, 'GT': 1464000}, 'fuels': ['Petrol']},
        'Tiguan': {'variants': {'Elegance': 3145000, 'R-Line': 4810000}, 'fuels': ['Petrol', 'Diesel']},
        'Vento':  {'variants': {'Trendline': 765000, 'Comfortline': 846000, 'Highline': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'Virtus': {'variants': {'Comfortline': 977000, 'Highline': 1077000, 'Topline': 1227000, 'GT': 1403000}, 'fuels': ['Petrol']},
    },

    'Volvo': {
        'C40 Recharge':  {'variants': {'Ultimate': 6300000}, 'fuels': ['BEV']},
        'S60':           {'variants': {'Momentum': 3600000, 'Inscription': 5850000}, 'fuels': ['Petrol', 'Diesel']},
        'S90':           {'variants': {'Ultimate': 6800000}, 'fuels': ['Petrol']},
        'V40':           {'variants': {'Kinetic': 2400000, 'Momentum': 3900000}, 'fuels': ['Diesel']},
        'XC40':          {'variants': {'Ultimate': 5000000}, 'fuels': ['Petrol']},
        'XC40 Recharge': {'variants': {'Ultimate': 5600000}, 'fuels': ['BEV']},
        'XC60':          {'variants': {'Plus': 5360000, 'Ultimate': 8710000}, 'fuels': ['Petrol', 'Diesel']},
        'XC90':          {'variants': {'Ultimate': 9800000}, 'fuels': ['Petrol', 'Diesel', 'HEV']},
    },
}


# ============================================================
# HELPERS FOR DROPDOWN POPULATION
# ============================================================

def get_makes():
    """Alphabetical list of all makes."""
    return sorted(CAR_DATA.keys())


def get_models(make: str):
    """Alphabetical list of models for a make."""
    if make not in CAR_DATA:
        return []
    return sorted(CAR_DATA[make].keys())


def get_variants(make: str, model: str):
    """
    Variants in TRIM ORDER (base → top). Source dict order preserved.
    Overrides the "alphabetical dropdowns" rule because for variants the
    order carries pricing meaning (cheapest → most expensive).
    """
    if make not in CAR_DATA or model not in CAR_DATA[make]:
        return []
    return list(CAR_DATA[make][model]['variants'].keys())


def get_fuels(make: str, model: str):
    """Fuel types in fixed order (Petrol/Diesel/CNG/HEV/PHEV/BEV)."""
    if make not in CAR_DATA or model not in CAR_DATA[make]:
        return []
    fuel_order = ['Petrol', 'Diesel', 'CNG', 'HEV', 'PHEV', 'BEV']
    model_fuels = CAR_DATA[make][model].get('fuels', [])
    return [f for f in fuel_order if f in model_fuels]


def get_variant_base_price(make: str, model: str, variant: str):
    """Returns the variant-specific base price, or None if not found."""
    if make not in CAR_DATA or model not in CAR_DATA[make]:
        return None
    variants = CAR_DATA[make][model].get('variants', {})
    return variants.get(variant)


def get_base_price(make: str, model: str):
    """
    Legacy shim — returns base-trim variant price as rough model anchor.
    Kept for backwards compatibility; new code uses get_variant_base_price().
    """
    if make not in CAR_DATA or model not in CAR_DATA[make]:
        return None
    variants = CAR_DATA[make][model].get('variants', {})
    if not variants:
        return None
    return next(iter(variants.values()))


# ============================================================
# PRICING FORMULA
# ============================================================

def compute_base_valuation(make, model, variant, fuel, year, mileage, condition, owner):
    """Pure formula-based valuation. Returns int rupees or None."""
    base = get_variant_base_price(make, model, variant)
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

    return int(round(price))


def compute_price_range(estimated_price, phase=1):
    """
    Range width scales with phase: ±12% at Phase 1 → ±5% at Phase 4.
    Phase defaults to 1 for backwards compatibility.
    """
    if estimated_price is None:
        return (None, None)
    range_pct = PHASE_BLEND.get(phase, PHASE_BLEND[1])['range_pct']
    low = int(round(estimated_price * (1 - range_pct)))
    high = int(round(estimated_price * (1 + range_pct)))
    return (low, high)


# ============================================================
# PHASE DETERMINATION
# ============================================================

def determine_phase(deal_count, distinct_users, previous_phase=1):
    """
    Compute phase (1-4) from deal_count + distinct_users in last 180 days.
    Upgrade requires BOTH thresholds. Hysteresis on downgrade.
    """
    phase = 1
    if deal_count >= PHASE_THRESHOLDS[2][0] and distinct_users >= PHASE_THRESHOLDS[2][1]:
        phase = 2
    if deal_count >= PHASE_THRESHOLDS[3][0] and distinct_users >= PHASE_THRESHOLDS[3][1]:
        phase = 3
    if deal_count >= PHASE_THRESHOLDS[4][0] and distinct_users >= PHASE_THRESHOLDS[4][1]:
        phase = 4

    if previous_phase and previous_phase > phase:
        if previous_phase >= 4 and deal_count >= PHASE_THRESHOLDS[4][2]:
            return 4
        if previous_phase >= 3 and deal_count >= PHASE_THRESHOLDS[3][2]:
            return max(phase, 3)
        if previous_phase >= 2 and deal_count >= PHASE_THRESHOLDS[2][2]:
            return max(phase, 2)

    return phase


# ============================================================
# BLENDING + GUARDRAILS
# ============================================================

def _trim_outliers(sorted_prices, trim_frac=OUTLIER_TRIM_FRAC):
    n = len(sorted_prices)
    k = int(n * trim_frac)
    if k == 0:
        return sorted_prices
    return sorted_prices[k:n - k]


def _median(sorted_prices):
    n = len(sorted_prices)
    if n == 0:
        return None
    if n % 2 == 1:
        return sorted_prices[n // 2]
    return (sorted_prices[n // 2 - 1] + sorted_prices[n // 2]) // 2


def adjust_with_deals(formula_price, similar_deals, phase=1):
    """
    Blend formula_price with median of similar_deals per phase weights.
    Applies outlier trimming (≥20 deals) and ±20% deviation guardrail.
    Returns (adjusted_price, confidence_score).
    """
    if formula_price is None:
        return None, PHASE_BLEND[1]['conf_base']

    phase_cfg = PHASE_BLEND.get(phase, PHASE_BLEND[1])
    n = len(similar_deals) if similar_deals else 0

    if phase == 1 or n == 0:
        return int(formula_price), phase_cfg['conf_base']

    sorted_prices = sorted(similar_deals)
    if n >= OUTLIER_TRIM_MIN_DEALS:
        sorted_prices = _trim_outliers(sorted_prices)
    median = _median(sorted_prices)

    if median is None:
        return int(formula_price), phase_cfg['conf_base']

    fw = phase_cfg['formula_weight']
    rw = phase_cfg['real_weight']
    blended = formula_price * fw + median * rw

    upper_cap = formula_price * (1 + GUARDRAIL_MAX_DEVIATION)
    lower_cap = formula_price * (1 - GUARDRAIL_MAX_DEVIATION)
    if blended > upper_cap:
        blended = upper_cap
    elif blended < lower_cap:
        blended = lower_cap

    conf = phase_cfg['conf_base'] + min(8, n // 4)
    conf = min(conf, CONF_CEILING[phase])

    return int(round(blended)), conf


def get_phase_display(phase):
    """Returns {'phase', 'badge', 'detail', 'tooltip'} for template rendering."""
    cfg = PHASE_BLEND.get(phase, PHASE_BLEND[1])
    return {
        'phase': phase,
        'badge': cfg['badge'],
        'detail': cfg['badge_detail'],
        'tooltip': cfg['tooltip'],
    }
