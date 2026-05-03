"""
car_data.py
-----------
AutoKnowMus — Used car master data + hybrid pricing engine + listings cache.

DATA SOURCE: Google Sheets (published as CSV) with full bundled fallback.

How it works:
  1. On first use (and every PRICE_CACHE_TTL_SECONDS), fetches 5 sources:
     - car_prices: make/model/variant/fuel/ex_showroom_price rows
     - depreciation_curve: age → retention % rows
     - multipliers: condition/owner/fuel multiplier rows
     - meta: data_version, last_updated, etc.
     - listings: current market listings for the 740Li engine
  2. If a Google Sheet is reachable, its data is used.
  3. If any tab fails to fetch or is empty, falls back to bundled FALLBACK_*
     data in this file. The app NEVER breaks for users.
  4. Admin can force a refresh via /admin/refresh-prices.

Environment variables required:
  GSHEET_PRICES_URL
  GSHEET_DEPRECIATION_URL
  GSHEET_MULTIPLIERS_URL
  GSHEET_META_URL
  GSHEET_LISTINGS_URL  (separate spreadsheet, single tab named 'listings')

Optional:
  PRICE_CACHE_TTL_SECONDS (default 3600 = 1 hour)

CHANGES IN THIS VERSION (per-fuel pricing fix, v3.6.0):
  - PARSER FIX: _parse_car_prices() now stores per-(variant, fuel) prices
    in a new nested dict `variant_fuel_prices`, alongside the existing flat
    `variants` dict. The flat dict still holds the FIRST-seen-fuel price for
    each variant (preserves backward compatibility for any code path that
    reads `variants` directly, e.g. JSON to templates, dropdown population).
  - get_variant_base_price() now uses `variant_fuel_prices` when fuel is
    provided AND a per-fuel price exists, otherwise falls through to the old
    `variants` lookup. Means: for cars where the sheet has distinct Petrol/
    Diesel/HEV prices for the same variant (e.g. Honda City, Hyundai Creta),
    the engine now uses the correct fuel-specific ex-showroom price instead
    of silently dropping all but the first parsed fuel's price.
  - _merge_with_fallback() carries `variant_fuel_prices` through the merge.
    Fallback dict is NOT updated to per-fuel; it stays at the old flat shape.
    The lookup gracefully falls through to flat when per-fuel is unavailable.

PRIOR CHANGES (Phase 1A migration, retained):
  - get_variant_base_price() accepts optional `fuel` parameter (now actually
    used, not just a no-op as it was before this fix)
  - get_base_price() returns the lowest-priced variant deterministically
  - listings cache layer for the 740Li pricing engine
  - get_listings_for_car() helper that the engine consumes
  - LISTINGS_DATA_FRESH module flag for app.py routing logic

MIGRATION PATH: To move from public CSV to private Google Sheets API later:
  1. Create a GCP service account, enable Google Sheets API
  2. Replace _fetch_csv() with gspread-based private fetch
  3. Unpublish the sheet from web
"""

import os
import csv
import io
import time
import threading
import requests
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# ============================================================
# CONSTANTS
# ============================================================
CURRENT_YEAR = datetime.now().year
EXPECTED_KM_PER_YEAR = 10000

PRICE_CACHE_TTL_SECONDS = int(os.environ.get("PRICE_CACHE_TTL_SECONDS", 3600))

# Google Sheets CSV URLs
GSHEET_PRICES_URL       = os.environ.get("GSHEET_PRICES_URL", "")
GSHEET_DEPRECIATION_URL = os.environ.get("GSHEET_DEPRECIATION_URL", "")
GSHEET_MULTIPLIERS_URL  = os.environ.get("GSHEET_MULTIPLIERS_URL", "")
GSHEET_META_URL         = os.environ.get("GSHEET_META_URL", "")
GSHEET_LISTINGS_URL     = os.environ.get("GSHEET_LISTINGS_URL", "")

# Listings engine threshold (locked decision — N>=5 effective listings to use 740Li engine)
LISTINGS_MIN_FOR_ENGINE = 5

# Default listing freshness window (days). Listings older than this are excluded
# unless they have an explicit expires_date that overrides this.
LISTINGS_DEFAULT_FRESHNESS_DAYS = 60


# ============================================================
# PHASE SYSTEM — per-model blending of formula + real deals
# ============================================================

PHASE_THRESHOLDS = {
    2: (5, 4, 3),
    3: (15, 10, 10),
    4: (50, 25, 35),
}

PHASE_BLEND = {
    1: {"formula_weight": 1.00, "real_weight": 0.00, "conf_base": 55, "range_pct": 0.12,
        "badge": "Estimate", "badge_detail": "Based on our pricing model",
        "tooltip": ("This valuation is based on our pricing model calibrated "
                    "for the local used-car market. As more verified transactions "
                    "are submitted for this car, accuracy will improve.")},
    2: {"formula_weight": 0.70, "real_weight": 0.30, "conf_base": 68, "range_pct": 0.09,
        "badge": "Calibrated", "badge_detail": "Adjusted with recent market data",
        "tooltip": ("This valuation has been calibrated using recent verified "
                    "transactions for this car in the local used-car market.")},
    3: {"formula_weight": 0.35, "real_weight": 0.65, "conf_base": 82, "range_pct": 0.07,
        "badge": "Market-verified", "badge_detail": "Based primarily on verified transactions",
        "tooltip": ("This valuation is primarily based on verified transactions "
                    "for this car in the local used-car market over the last 6 months.")},
    4: {"formula_weight": 0.10, "real_weight": 0.90, "conf_base": 92, "range_pct": 0.05,
        "badge": "Real-Market Pricing", "badge_detail": "Based on extensive verified transaction data",
        "tooltip": ("This valuation reflects the real local used-car market price, "
                    "based on a large volume of recent verified transactions.")},
}

GUARDRAIL_MAX_DEVIATION = 0.20
OUTLIER_TRIM_FRAC = 0.10
OUTLIER_TRIM_MIN_DEALS = 20
CONF_CEILING = {1: 62, 2: 77, 3: 90, 4: 96}


# ============================================================
# BRAND DEMAND TIERS
# ============================================================

HIGH_DEMAND_BRANDS = {"Maruti Suzuki", "Hyundai", "Honda", "Toyota", "Tata", "Kia", "Mahindra"}
MEDIUM_DEMAND_BRANDS = {"Ford", "Renault", "Nissan", "Volkswagen", "Skoda", "MG"}
LUXURY_BRANDS = {"Audi", "BMW", "Mercedes-Benz", "Jaguar", "Land Rover", "Lexus", "Volvo"}


# ============================================================
# FALLBACK DATA — used whenever Google Sheets is unreachable or empty.
# This is the complete current dataset. Keep it in sync with the sheet.
# Updating this requires a code deploy; updating the sheet does not.
#
# NOTE: FALLBACK_CAR_DATA uses the old flat-per-variant shape. It does NOT
# carry per-fuel prices — that data lives only in the live sheet (and only
# for variants where the sheet has distinct fuel rows with distinct prices).
# When the sheet is unreachable and we run on fallback alone, get_variant_
# base_price() gracefully falls through to the flat lookup. This means
# fallback gives a less-precise answer for fuel-differentiated variants, but
# it never crashes and never returns nothing.
# ============================================================

FALLBACK_CAR_DATA = {
    'Audi': {
        'A3': {'variants': {'Premium': 3200000, 'Premium Plus': 4008000, 'Technology': 5200000}, 'fuels': ['Petrol', 'Diesel']},
        'A4': {'variants': {'Premium': 3600000, 'Premium Plus': 4509000, 'Technology': 5850000}, 'fuels': ['Petrol', 'Diesel']},
        'A6': {'variants': {'Premium Plus': 5525000, 'Technology': 8450000}, 'fuels': ['Petrol', 'Diesel']},
        'A8': {'variants': {'L Celebration': 13175000, 'L 55 TFSI': 20150000}, 'fuels': ['Petrol', 'Diesel']},
        'Q3': {'variants': {'Premium': 3600000, 'Premium Plus': 4509000, 'Technology': 5850000}, 'fuels': ['Petrol', 'Diesel']},
        'Q5': {'variants': {'Premium Plus': 5780000, 'Technology': 8840000}, 'fuels': ['Petrol', 'Diesel']},
        'Q7': {'variants': {'Premium Plus': 7480000, 'Technology': 11440000}, 'fuels': ['Petrol', 'Diesel']},
        'e-tron': {'variants': {'55 Quattro': 10580000, 'Sportback': 12880000}, 'fuels': ['BEV']}
    },

    'BMW': {
        '3 Series': {'variants': {'320d': 4493000, '330i': 5216000, 'M340i': 6760000}, 'fuels': ['Petrol', 'Diesel']},
        '5 Series': {'variants': {'520d': 5872000, '530i': 8160000}, 'fuels': ['Petrol', 'Diesel']},
        '7 Series': {'variants': {'730Ld': 12528000, '740Li': 14500000, 'M760Li': 18850000}, 'fuels': ['Petrol', 'Diesel']},
        'X1': {'variants': {'sDrive18i': 4080000, 'sDrive20d': 4824000, 'xDrive20d': 5994000}, 'fuels': ['Petrol', 'Diesel']},
        'X3': {'variants': {'xDrive20d': 5787000, 'xDrive30i': 6720000, 'M40i': 8710000}, 'fuels': ['Petrol', 'Diesel']},
        'X5': {'variants': {'xDrive30d': 8208000, 'xDrive40i': 11400000}, 'fuels': ['Petrol', 'Diesel']},
        'X7': {'variants': {'xDrive30d': 10541000, 'xDrive40i': 12224000, 'M50i': 15860000}, 'fuels': ['Petrol', 'Diesel']},
        'iX': {'variants': {'xDrive40': 11040000, 'xDrive50': 13440000}, 'fuels': ['BEV']}
    },

    'Ford': {
        'Aspire': {'variants': {'Ambiente': 552000, 'Trend': 610000, 'Titanium': 793000}, 'fuels': ['Petrol', 'Diesel']},
        'EcoSport': {'variants': {'Ambiente': 765000, 'Trend': 846000, 'Titanium': 962000, 'Titanium+': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'Endeavour': {'variants': {'Titanium': 2805000, 'Titanium+': 3099000, 'Sport': 3526000}, 'fuels': ['Diesel']},
        'Figo': {'variants': {'Ambiente': 510000, 'Trend': 563000, 'Titanium': 732000}, 'fuels': ['Petrol', 'Diesel']},
        'Freestyle': {'variants': {'Ambiente': 595000, 'Trend': 658000, 'Titanium': 748000, 'Titanium+': 854000}, 'fuels': ['Petrol', 'Diesel']}
    },

    'Honda': {
        'Amaze': {'variants': {'E': 638000, 'S': 705000, 'V': 803000, 'VX': 915000}, 'fuels': ['Petrol', 'Diesel']},
        'BR-V': {'variants': {'S': 850000, 'V': 1000000, 'VX': 1220000}, 'fuels': ['Petrol', 'Diesel']},
        'City': {'variants': {'V': 1105000, 'VX': 1300000, 'ZX': 1586000}, 'fuels': ['Petrol', 'Diesel', 'HEV']},
        'Civic': {'variants': {'V': 1530000, 'VX': 1800000, 'ZX': 2196000}, 'fuels': ['Petrol', 'Diesel']},
        'CR-V': {'variants': {'2WD': 2610000, '4WD': 3190000}, 'fuels': ['Petrol', 'Diesel']},
        'Elevate': {'variants': {'SV': 1062000, 'V': 1172000, 'VX': 1336000, 'ZX': 1525000}, 'fuels': ['Petrol']},
        'Jazz': {'variants': {'V': 722000, 'VX': 850000, 'ZX': 1037000}, 'fuels': ['Petrol', 'Diesel']},
        'Mobilio': {'variants': {'S': 765000, 'V': 900000, 'RS': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'WR-V': {'variants': {'S': 808000, 'SV': 891000, 'VX': 1159000}, 'fuels': ['Petrol', 'Diesel']}
    },

    'Hyundai': {
        'Alcazar': {'variants': {'Prestige': 1445000, 'Signature': 1700000, 'Platinum': 2074000}, 'fuels': ['Petrol', 'Diesel']},
        'Aura': {'variants': {'E': 553000, 'S': 610000, 'SX': 694000, 'SX(O)': 793000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Creta': {'variants': {'E': 935000, 'EX': 1002000, 'S': 1100000, 'SX': 1215000, 'SX(O)': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Elantra': {'variants': {'S': 1530000, 'SX': 1800000, 'SX(O)': 2196000}, 'fuels': ['Petrol', 'Diesel']},
        'Elite i20': {'variants': {'Era': 595000, 'Magna': 657000, 'Sportz': 748000, 'Asta': 854000}, 'fuels': ['Petrol', 'Diesel']},
        'Eon': {'variants': {'Era': 340000, 'Magna': 400000, 'Sportz': 488000}, 'fuels': ['Petrol']},
        'Exter': {'variants': {'EX': 553000, 'S': 610000, 'SX': 694000, 'SX(O)': 793000}, 'fuels': ['Petrol', 'CNG']},
        'Grand i10': {'variants': {'Era': 468000, 'Magna': 517000, 'Sportz': 587000, 'Asta': 671000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Grand i10 Nios': {'variants': {'Era': 510000, 'Magna': 563000, 'Sportz': 640000, 'Asta': 732000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'i10': {'variants': {'Era': 425000, 'Magna': 470000, 'Sportz': 534000, 'Asta': 610000}, 'fuels': ['Petrol']},
        'i20': {'variants': {'Era': 637000, 'Magna': 704000, 'Sportz': 801000, 'Asta': 915000}, 'fuels': ['Petrol', 'Diesel']},
        'i20 Active': {'variants': {'S': 720000, 'SX': 976000}, 'fuels': ['Petrol', 'Diesel']},
        'Ioniq 5': {'variants': {'RWD': 4232000, 'AWD': 5152000}, 'fuels': ['BEV']},
        'Kona Electric': {'variants': {'Premium': 2208000, 'Premium Dual Tone': 2688000}, 'fuels': ['BEV']},
        'Santa Fe': {'variants': {'GLS': 2550000, 'Signature': 3660000}, 'fuels': ['Diesel']},
        'Santro': {'variants': {'D-Lite': 425000, 'Era': 456000, 'Magna': 488000, 'Sportz': 534000, 'Asta': 610000}, 'fuels': ['Petrol', 'CNG']},
        'Tucson': {'variants': {'Platinum': 2550000, 'Signature': 3660000}, 'fuels': ['Petrol', 'Diesel']},
        'Venue': {'variants': {'E': 680000, 'S': 751000, 'SX': 854000, 'SX(O)': 976000}, 'fuels': ['Petrol', 'Diesel']},
        'Verna': {'variants': {'EX': 935000, 'S': 1002000, 'SX': 1215000, 'SX(O)': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Xcent': {'variants': {'E': 468000, 'S': 545000, 'SX': 671000}, 'fuels': ['Petrol', 'Diesel', 'CNG']}
    },

    'Jaguar': {
        'F-Pace': {'variants': {'S': 6560000, 'SE': 8224000, 'HSE': 10660000}, 'fuels': ['Petrol', 'Diesel']},
        'I-Pace': {'variants': {'S': 11592000, 'SE': 12600000, 'HSE': 14112000}, 'fuels': ['BEV']},
        'XE': {'variants': {'S': 4000000, 'SE': 5015000, 'HSE': 6500000}, 'fuels': ['Petrol', 'Diesel']},
        'XF': {'variants': {'Prestige': 6000000, 'Portfolio': 7523000, 'R-Sport': 9750000}, 'fuels': ['Petrol', 'Diesel']},
        'XJ': {'variants': {'L Portfolio': 11000000}, 'fuels': ['Petrol', 'Diesel']}
    },

    'Kia': {
        'Carens': {'variants': {'Premium': 935000, 'Prestige': 1031000, 'Luxury': 1175000, 'Luxury Plus': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Carnival': {'variants': {'Premium': 2975000, 'Prestige': 3508000, 'Limousine': 4270000}, 'fuels': ['Diesel']},
        'EV6': {'variants': {'GT Line': 5520000, 'GT Line AWD': 6720000}, 'fuels': ['BEV']},
        'EV9': {'variants': {'GT Line': 11960000, 'GT Line AWD': 14560000}, 'fuels': ['BEV']},
        'Seltos': {'variants': {'HTE': 935000, 'HTK': 993000, 'HTX': 1079000, 'GTX+': 1185000, 'X-Line': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Sonet': {'variants': {'HTE': 680000, 'HTK': 751000, 'HTX': 854000, 'GTX+': 976000}, 'fuels': ['Petrol', 'Diesel']}
    },

    'Land Rover': {
        'Defender': {'variants': {'S': 7600000, 'SE': 9526000, 'HSE': 11115000, 'X': 12350000}, 'fuels': ['Petrol', 'Diesel']},
        'Discovery': {'variants': {'S': 7200000, 'SE': 9027000, 'HSE': 11700000}, 'fuels': ['Petrol', 'Diesel']},
        'Discovery Sport': {'variants': {'S': 5200000, 'SE': 6519000, 'HSE': 8450000}, 'fuels': ['Petrol', 'Diesel']},
        'Freelander': {'variants': {'SE': 3870000, 'HSE': 5850000}, 'fuels': ['Diesel']},
        'Range Rover': {'variants': {'SE': 18400000, 'HSE': 23061000, 'Autobiography': 29900000}, 'fuels': ['Petrol', 'Diesel']},
        'Range Rover Evoque': {'variants': {'S': 5600000, 'SE': 7021000, 'HSE': 9100000}, 'fuels': ['Petrol', 'Diesel']},
        'Range Rover Sport': {'variants': {'SE': 10800000, 'HSE': 13541000, 'Autobiography': 17550000}, 'fuels': ['Petrol', 'Diesel']},
        'Range Rover Velar': {'variants': {'S': 6800000, 'SE': 8526000, 'HSE': 11050000}, 'fuels': ['Petrol', 'Diesel']}
    },

    'Lexus': {
        'ES': {'variants': {'Luxury': 5200000, 'F-Sport': 8450000}, 'fuels': ['HEV']},
        'LS': {'variants': {'Ultra Luxury': 20000000}, 'fuels': ['HEV']},
        'LX': {'variants': {'VX': 22400000, 'Ultra Luxury': 36400000}, 'fuels': ['Petrol', 'Diesel']},
        'NX': {'variants': {'Luxury': 5760000, 'F-Sport': 9360000}, 'fuels': ['HEV']},
        'RX': {'variants': {'Luxury': 7920000, 'F-Sport': 12870000}, 'fuels': ['HEV']}
    },

    'Mahindra': {
        'Alturas G4': {'variants': {'4x2 AT': 2550000, '4x4 AT': 3660000}, 'fuels': ['Diesel']},
        'BE 6e': {'variants': {'Pack 1': 1748000, 'Pack 2': 1900000, 'Pack 3': 2128000}, 'fuels': ['BEV']},
        'Bolero': {'variants': {'B4': 808000, 'B6': 892000, 'B6(O)': 1159000}, 'fuels': ['Diesel']},
        'KUV100': {'variants': {'K2': 510000, 'K4': 563000, 'K6': 640000, 'K8': 732000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Marazzo': {'variants': {'M2': 935000, 'M4': 1031000, 'M6': 1175000, 'M8': 1342000}, 'fuels': ['Diesel']},
        'Scorpio': {'variants': {'S3': 1020000, 'S5': 1095000, 'S7': 1200000, 'S9': 1335000, 'S11': 1464000}, 'fuels': ['Diesel']},
        'Scorpio-N': {'variants': {'Z2': 1190000, 'Z4': 1278000, 'Z6': 1400000, 'Z8': 1558000, 'Z8 L': 1708000}, 'fuels': ['Petrol', 'Diesel']},
        'Thar': {'variants': {'AX': 1190000, 'AX(O)': 1400000, 'LX': 1708000}, 'fuels': ['Petrol', 'Diesel']},
        'TUV300': {'variants': {'T4': 680000, 'T6': 751000, 'T8': 854000, 'T10': 976000}, 'fuels': ['Diesel']},
        'XEV 9e': {'variants': {'Pack 1': 2024000, 'Pack 2': 2200000, 'Pack 3': 2464000}, 'fuels': ['BEV']},
        'XUV 3XO': {'variants': {'MX1': 680000, 'MX3': 727000, 'AX5': 800000, 'AX7': 888000, 'AX7 L': 976000}, 'fuels': ['Petrol', 'Diesel']},
        'XUV300': {'variants': {'W4': 765000, 'W6': 846000, 'W8': 962000, 'W8(O)': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'XUV400': {'variants': {'EC': 1472000, 'EL': 1792000}, 'fuels': ['BEV']},
        'XUV500': {'variants': {'W5': 1190000, 'W7': 1306000, 'W9': 1446000, 'W11': 1708000}, 'fuels': ['Diesel']},
        'XUV700': {'variants': {'MX': 1190000, 'AX3': 1278000, 'AX5': 1400000, 'AX7': 1558000, 'AX7 L': 1708000}, 'fuels': ['Petrol', 'Diesel']},
        'Xylo': {'variants': {'D2': 723000, 'D4': 772000, 'H4': 835000, 'H8': 925000, 'H9': 1037000}, 'fuels': ['Diesel']}
    },

    'Maruti Suzuki': {
        'A-Star': {'variants': {'LXi': 382000, 'VXi': 450000, 'ZXi': 549000}, 'fuels': ['Petrol']},
        'Alto': {'variants': {'Std': 297000, 'LXi': 350000, 'VXi': 427000}, 'fuels': ['Petrol', 'CNG']},
        'Alto 800': {'variants': {'Std': 340000, 'LXi': 376000, 'VXi': 427000, 'VXi+': 488000}, 'fuels': ['Petrol', 'CNG']},
        'Alto K10': {'variants': {'Std': 382000, 'LXi': 423000, 'VXi': 481000, 'VXi+': 549000}, 'fuels': ['Petrol', 'CNG']},
        'Baleno': {'variants': {'Sigma': 637000, 'Delta': 704000, 'Zeta': 801000, 'Alpha': 915000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Brezza': {'variants': {'LXi': 808000, 'VXi': 892000, 'ZXi': 1015000, 'ZXi+': 1159000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Celerio': {'variants': {'LXi': 468000, 'VXi': 517000, 'ZXi': 587000, 'ZXi+': 671000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Ciaz': {'variants': {'Sigma': 765000, 'Delta': 846000, 'Zeta': 962000, 'Alpha': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'Dzire': {'variants': {'LXi': 595000, 'VXi': 657000, 'ZXi': 748000, 'ZXi+': 854000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Eeco': {'variants': {'5 Seater': 495000, '7 Seater': 605000}, 'fuels': ['Petrol', 'CNG']},
        'Ertiga': {'variants': {'LXi': 808000, 'VXi': 892000, 'ZXi': 1015000, 'ZXi+': 1159000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Fronx': {'variants': {'Sigma': 680000, 'Delta': 751000, 'Zeta': 854000, 'Alpha': 976000}, 'fuels': ['Petrol', 'CNG']},
        'Grand Vitara': {'variants': {'Sigma': 935000, 'Delta': 1031000, 'Zeta': 1175000, 'Alpha': 1342000}, 'fuels': ['Petrol', 'HEV', 'CNG']},
        'Ignis': {'variants': {'Sigma': 527000, 'Delta': 582000, 'Zeta': 663000, 'Alpha': 756000}, 'fuels': ['Petrol', 'Diesel']},
        'Invicto': {'variants': {'Zeta+': 2125000, 'Alpha+': 3050000}, 'fuels': ['HEV']},
        'Jimny': {'variants': {'Zeta': 1105000, 'Alpha': 1586000}, 'fuels': ['Petrol']},
        'Omni': {'variants': {'5 Seater': 270000, '8 Seater': 330000}, 'fuels': ['Petrol', 'CNG']},
        'Ritz': {'variants': {'LXi': 382000, 'VXi': 423000, 'ZXi': 549000}, 'fuels': ['Petrol', 'Diesel']},
        'S-Cross': {'variants': {'Sigma': 722000, 'Delta': 798000, 'Zeta': 908000, 'Alpha': 1037000}, 'fuels': ['Petrol', 'Diesel']},
        'S-Presso': {'variants': {'Std': 425000, 'LXi': 470000, 'VXi': 534000, 'VXi+': 610000}, 'fuels': ['Petrol', 'CNG']},
        'Swift': {'variants': {'LXi': 595000, 'LDi': 643000, 'VXi': 700000, 'VDi': 756000, 'ZXi': 799000, 'ZDi': 863000, 'ZXi+': 854000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'SX4': {'variants': {'VXi': 553000, 'ZXi': 793000}, 'fuels': ['Petrol']},
        'Wagon R': {'variants': {'LXi': 493000, 'VXi': 545000, 'ZXi': 619000, 'ZXi+': 708000}, 'fuels': ['Petrol', 'CNG']},
        'XL6': {'variants': {'Zeta': 1063000, 'Alpha': 1250000, 'Alpha+': 1525000}, 'fuels': ['Petrol', 'CNG']},
        'Zen Estilo': {'variants': {'LXi': 297000, 'VXi': 427000}, 'fuels': ['Petrol']}
    },

    'Mercedes-Benz': {
        'A-Class': {'variants': {'A 200': 3600000, 'A 200 d': 4860000}, 'fuels': ['Petrol', 'Diesel']},
        'B-Class': {'variants': {'B 180': 2800000, 'B 200 CDI': 3780000}, 'fuels': ['Petrol', 'Diesel']},
        'C-Class': {'variants': {'C 200': 4400000, 'C 220 d': 5940000, 'C 300': 7150000}, 'fuels': ['Petrol', 'Diesel']},
        'CLA': {'variants': {'CLA 200': 3600000, 'CLA 220 d': 4860000}, 'fuels': ['Petrol', 'Diesel']},
        'E-Class': {'variants': {'E 200': 6240000, 'E 220 d': 8424000, 'E 350': 10140000}, 'fuels': ['Petrol', 'Diesel']},
        'EQS': {'variants': {'580 4MATIC': 16500000}, 'fuels': ['BEV']},
        'GLA': {'variants': {'GLA 200': 4000000, 'GLA 220 d': 5400000}, 'fuels': ['Petrol', 'Diesel']},
        'GLC': {'variants': {'GLC 220 d': 6480000, 'GLC 300': 9000000}, 'fuels': ['Petrol', 'Diesel']},
        'GLE': {'variants': {'GLE 300 d': 8640000, 'GLE 450': 12000000}, 'fuels': ['Petrol', 'Diesel']},
        'GLS': {'variants': {'GLS 450 4MATIC': 10800000, 'Maybach GLS': 17550000}, 'fuels': ['Petrol', 'Diesel']},
        'M-Class': {'variants': {'ML 250 CDI': 5616000, 'ML 350': 7150000}, 'fuels': ['Petrol', 'Diesel']},
        'S-Class': {'variants': {'S 350d': 15984000, 'S 450': 18538000, 'Maybach S 580': 24050000}, 'fuels': ['Petrol', 'Diesel']}
    },

    'MG': {
        'Astor': {'variants': {'Style': 935000, 'Super': 993000, 'Smart': 1079000, 'Sharp': 1185000, 'Savvy': 1342000}, 'fuels': ['Petrol']},
        'Comet EV': {'variants': {'Executive': 736000, 'Exclusive': 800000, 'Excite': 896000}, 'fuels': ['BEV']},
        'Gloster': {'variants': {'Super': 3315000, 'Smart': 3667000, 'Sharp': 4095000, 'Savvy': 4758000}, 'fuels': ['Diesel']},
        'Hector': {'variants': {'Style': 1233000, 'Super': 1362000, 'Smart': 1549000, 'Sharp': 1769000}, 'fuels': ['Petrol', 'Diesel']},
        'Hector Plus': {'variants': {'Style': 1445000, 'Super': 1598000, 'Smart': 1815000, 'Sharp': 2074000}, 'fuels': ['Petrol', 'Diesel']},
        'Windsor EV': {'variants': {'Excite': 1288000, 'Exclusive': 1568000}, 'fuels': ['BEV']},
        'ZS EV': {'variants': {'Excite': 2116000, 'Exclusive': 2576000}, 'fuels': ['BEV']}
    },

    'Nissan': {
        'GT-R': {'variants': {'Premium Edition': 22000000}, 'fuels': ['Petrol']},
        'Kicks': {'variants': {'XL': 850000, 'XV': 1000000, 'XV Premium': 1220000}, 'fuels': ['Petrol', 'Diesel']},
        'Magnite': {'variants': {'XE': 553000, 'XL': 610000, 'XV': 694000, 'XV Premium': 793000}, 'fuels': ['Petrol']},
        'Micra': {'variants': {'XE': 468000, 'XL': 517000, 'XV': 671000}, 'fuels': ['Petrol', 'Diesel']},
        'Sunny': {'variants': {'XE': 595000, 'XL': 657000, 'XV': 854000}, 'fuels': ['Petrol', 'Diesel']},
        'Terrano': {'variants': {'XE': 850000, 'XL': 939000, 'XV': 1220000}, 'fuels': ['Diesel']}
    },

    'Renault': {
        'Captur': {'variants': {'RXE': 935000, 'RXL': 1031000, 'RXT': 1175000, 'Platine': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Duster': {'variants': {'RXE': 808000, 'RXS': 908000, 'RXZ': 1159000}, 'fuels': ['Petrol', 'Diesel']},
        'Fluence': {'variants': {'E2': 1190000, 'E4': 1708000}, 'fuels': ['Petrol', 'Diesel']},
        'Kiger': {'variants': {'RXE': 553000, 'RXL': 610000, 'RXT': 694000, 'RXZ': 793000}, 'fuels': ['Petrol']},
        'Kwid': {'variants': {'RXE': 425000, 'RXL': 470000, 'RXT': 534000, 'Climber': 610000}, 'fuels': ['Petrol']},
        'Lodgy': {'variants': {'RXE': 808000, 'RXL': 892000, 'RXZ': 1015000, 'Stepway': 1159000}, 'fuels': ['Diesel']},
        'Pulse': {'variants': {'RxE': 425000, 'RxL': 470000, 'RxZ': 610000}, 'fuels': ['Petrol', 'Diesel']},
        'Scala': {'variants': {'RxE': 553000, 'RxL': 610000, 'RxZ': 793000}, 'fuels': ['Petrol', 'Diesel']},
        'Triber': {'variants': {'RXE': 510000, 'RXL': 563000, 'RXT': 640000, 'RXZ': 732000}, 'fuels': ['Petrol']}
    },

    'Skoda': {
        'Fabia': {'variants': {'Active': 510000, 'Ambition': 582000, 'Elegance': 732000}, 'fuels': ['Petrol', 'Diesel']},
        'Kodiaq': {'variants': {'Style': 3400000, 'Sportline': 3760000, 'L&K': 4880000}, 'fuels': ['Petrol', 'Diesel']},
        'Kushaq': {'variants': {'Active': 1020000, 'Ambition': 1125000, 'Style': 1280000, 'Monte Carlo': 1464000}, 'fuels': ['Petrol']},
        'Laura': {'variants': {'Ambiente': 1190000, 'Elegance': 1400000, 'L&K': 1708000}, 'fuels': ['Petrol', 'Diesel']},
        'Octavia': {'variants': {'Style': 2295000, 'L&K': 3294000}, 'fuels': ['Petrol', 'Diesel']},
        'Rapid': {'variants': {'Active': 765000, 'Ambition': 846000, 'Style': 962000, 'Monte Carlo': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'Slavia': {'variants': {'Active': 935000, 'Ambition': 1031000, 'Style': 1175000, 'Monte Carlo': 1342000}, 'fuels': ['Petrol']},
        'Superb': {'variants': {'Sportline': 4845000, 'L&K': 6954000}, 'fuels': ['Petrol', 'Diesel']},
        'Yeti': {'variants': {'Active': 1530000, 'Ambition': 1692000, 'Elegance': 2196000}, 'fuels': ['Diesel']}
    },

    'Tata': {
        'Altroz': {'variants': {'XE': 578000, 'XM': 634000, 'XT': 720000, 'XZ': 823000, 'XZ+': 940000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Bolt': {'variants': {'XE': 425000, 'XM': 470000, 'XT': 534000, 'XMS': 610000}, 'fuels': ['Petrol', 'Diesel']},
        'Curvv': {'variants': {'Smart': 935000, 'Pure': 1031000, 'Creative': 1175000, 'Accomplished': 1342000}, 'fuels': ['Petrol', 'Diesel']},
        'Curvv EV': {'variants': {'Creative': 1656000, 'Accomplished': 1800000, 'Empowered': 2016000}, 'fuels': ['BEV']},
        'Harrier': {'variants': {'Smart': 1318000, 'Pure': 1456000, 'Adventure': 1661000, 'Fearless': 1891000}, 'fuels': ['Diesel']},
        'Hexa': {'variants': {'XE': 1530000, 'XM': 1690000, 'XT': 1923000, 'XT4x4': 2196000}, 'fuels': ['Diesel']},
        'Indica': {'variants': {'LS': 298000, 'LX': 330000, 'LXi': 376000, 'VX': 427000}, 'fuels': ['Petrol', 'Diesel']},
        'Indigo': {'variants': {'LS': 340000, 'LX': 376000, 'VX': 488000}, 'fuels': ['Petrol', 'Diesel']},
        'Manza': {'variants': {'Aqua': 425000, 'Aura': 470000, 'Club Class': 610000}, 'fuels': ['Petrol', 'Diesel']},
        'Nano': {'variants': {'Std': 170000, 'CX': 188000, 'LX': 214000, 'XT': 244000}, 'fuels': ['Petrol']},
        'Nexon': {'variants': {'Smart': 765000, 'Pure': 845000, 'Creative': 962000, 'Fearless': 1098000}, 'fuels': ['Petrol', 'Diesel', 'CNG']},
        'Nexon EV': {'variants': {'Creative': 1380000, 'Fearless': 1500000, 'Empowered': 1680000}, 'fuels': ['BEV']},
        'Punch': {'variants': {'Pure': 553000, 'Adventure': 610000, 'Accomplished': 694000, 'Creative': 793000}, 'fuels': ['Petrol', 'CNG']},
        'Punch EV': {'variants': {'Smart': 1012000, 'Adventure': 1100000, 'Empowered': 1232000}, 'fuels': ['BEV']},
        'Safari': {'variants': {'Smart': 1403000, 'Pure': 1550000, 'Adventure': 1768000, 'Fearless': 2013000}, 'fuels': ['Diesel']},
        'Safari Storme': {'variants': {'LX': 935000, 'EX': 1031000, 'VX': 1342000}, 'fuels': ['Diesel']},
        'Sumo': {'variants': {'CX': 510000, 'EX': 563000, 'GX': 732000}, 'fuels': ['Diesel']},
        'Tiago': {'variants': {'XE': 468000, 'XM': 514000, 'XT': 584000, 'XZ': 667000, 'XZ+': 761000}, 'fuels': ['Petrol', 'CNG']},
        'Tiago EV': {'variants': {'XE': 782000, 'XT': 833000, 'XZ+': 922000, 'XZ+ LUX': 1020000}, 'fuels': ['BEV']},
        'Tigor': {'variants': {'XE': 527000, 'XM': 580000, 'XT': 658000, 'XZ': 752000, 'XZ+': 859000}, 'fuels': ['Petrol', 'CNG']},
        'Zest': {'variants': {'XE': 468000, 'XM': 517000, 'XT': 587000, 'XMA': 671000}, 'fuels': ['Petrol', 'Diesel']}
    },

    'Toyota': {
        'Camry': {'variants': {'Hybrid': 4600000}, 'fuels': ['Petrol', 'HEV']},
        'Corolla Altis': {'variants': {'J': 1530000, 'G': 1693000, 'GL': 1924000, 'VL': 2196000}, 'fuels': ['Petrol', 'Diesel']},
        'Etios': {'variants': {'J': 510000, 'G': 564000, 'V': 641000, 'VX': 732000}, 'fuels': ['Petrol', 'Diesel']},
        'Etios Liva': {'variants': {'J': 468000, 'G': 517000, 'V': 587000, 'VX': 671000}, 'fuels': ['Petrol', 'Diesel']},
        'Fortuner': {'variants': {'4x2 AT': 2890000, '4x4 AT': 3196000, 'Legender': 3640000, 'GR-Sport': 4148000}, 'fuels': ['Petrol', 'Diesel']},
        'Glanza': {'variants': {'E': 595000, 'S': 658000, 'G': 748000, 'V': 854000}, 'fuels': ['Petrol', 'CNG']},
        'Hilux': {'variants': {'Standard': 2635000, 'High': 3782000}, 'fuels': ['Diesel']},
        'Innova': {'variants': {'GX': 1190000, 'VX': 1400000, 'ZX': 1708000}, 'fuels': ['Diesel']},
        'Innova Crysta': {'variants': {'GX': 1700000, 'VX': 2000000, 'ZX': 2440000}, 'fuels': ['Petrol', 'Diesel']},
        'Innova Hycross': {'variants': {'GX': 1615000, 'VX': 1785000, 'ZX': 2029000, 'ZX(O)': 2318000}, 'fuels': ['Petrol', 'HEV']},
        'Land Cruiser': {'variants': {'LC300': 20900000, 'LC200': 26840000}, 'fuels': ['Petrol', 'Diesel']},
        'Prado': {'variants': {'VX': 7650000, 'VX-L': 10980000}, 'fuels': ['Diesel']},
        'Urban Cruiser': {'variants': {'Mid': 723000, 'High': 796000, 'Premium': 1037000}, 'fuels': ['Petrol']},
        'Urban Cruiser Hyryder': {'variants': {'E': 935000, 'S': 1031000, 'G': 1175000, 'V': 1342000}, 'fuels': ['Petrol', 'HEV', 'CNG']},
        'Urban Cruiser Taisor': {'variants': {'E': 637000, 'S': 704000, 'G': 801000, 'V': 915000}, 'fuels': ['Petrol', 'CNG']},
        'Vellfire': {'variants': {'VIP Lounge': 12000000}, 'fuels': ['HEV']},
        'Yaris': {'variants': {'J': 808000, 'G': 892000, 'V': 1015000, 'VX': 1159000}, 'fuels': ['Petrol']}
    },

    'Volkswagen': {
        'Ameo': {'variants': {'Trendline': 510000, 'Comfortline': 563000, 'Highline': 732000}, 'fuels': ['Petrol', 'Diesel']},
        'Beetle': {'variants': {'Standard': 2800000}, 'fuels': ['Petrol']},
        'Jetta': {'variants': {'Trendline': 1275000, 'Comfortline': 1410000, 'Highline': 1830000}, 'fuels': ['Petrol', 'Diesel']},
        'Passat': {'variants': {'Comfortline': 2380000, 'Highline': 3416000}, 'fuels': ['Petrol', 'Diesel']},
        'Polo': {'variants': {'Trendline': 595000, 'Comfortline': 657000, 'Highline': 748000, 'GT TSI': 854000}, 'fuels': ['Petrol', 'Diesel']},
        'Taigun': {'variants': {'Comfortline': 1020000, 'Highline': 1125000, 'Topline': 1280000, 'GT': 1464000}, 'fuels': ['Petrol']},
        'Tiguan': {'variants': {'Elegance': 3145000, 'R-Line': 4810000}, 'fuels': ['Petrol', 'Diesel']},
        'Vento': {'variants': {'Trendline': 765000, 'Comfortline': 846000, 'Highline': 1098000}, 'fuels': ['Petrol', 'Diesel']},
        'Virtus': {'variants': {'Comfortline': 977000, 'Highline': 1077000, 'Topline': 1227000, 'GT': 1403000}, 'fuels': ['Petrol']}
    },

    'Volvo': {
        'C40 Recharge': {'variants': {'Ultimate': 6300000}, 'fuels': ['BEV']},
        'S60': {'variants': {'Momentum': 3600000, 'Inscription': 5850000}, 'fuels': ['Petrol', 'Diesel']},
        'S90': {'variants': {'Ultimate': 6800000}, 'fuels': ['Petrol']},
        'V40': {'variants': {'Kinetic': 2400000, 'Momentum': 3900000}, 'fuels': ['Diesel']},
        'XC40': {'variants': {'Ultimate': 5000000}, 'fuels': ['Petrol']},
        'XC40 Recharge': {'variants': {'Ultimate': 5600000}, 'fuels': ['BEV']},
        'XC60': {'variants': {'Plus': 5360000, 'Ultimate': 8710000}, 'fuels': ['Petrol', 'Diesel']},
        'XC90': {'variants': {'Ultimate': 9800000}, 'fuels': ['Petrol', 'Diesel', 'HEV']}
    },
}

FALLBACK_DEPRECIATION_CURVE = {
    0: 100, 1: 82, 2: 70, 3: 60, 4: 52, 5: 45,
    6: 39, 7: 33, 8: 28, 9: 23, 10: 18,
    11: 14, 12: 11, 13: 8, 14: 6, 15: 4,
}

FALLBACK_MULTIPLIERS = {
    "condition": {"Excellent": 1.05, "Good": 1.00, "Fair": 0.90},
    "owner":     {"1st Owner": 1.00, "2nd Owner": 0.95, "3rd Owner or more": 0.90},
    "fuel_premium": {"Petrol": 1.00, "Diesel": 1.05, "CNG": 0.97,
                     "HEV": 1.05, "PHEV": 1.08, "BEV": 1.10},
}

FALLBACK_META = {
    "data_version": "2026-Q1",
    "last_updated": "2026-01-15",
    "data_source": "Bundled fallback (Google Sheet unreachable or empty)",
    "notes": "Running on bundled data. Check GSHEET_* env vars to enable live sheet.",
}

# Listings has no bundled fallback. If the listings sheet is unreachable or
# empty, get_listings_for_car() returns [] for every query, which causes the
# binary engine switch in app.py to fall back to the depreciation engine. This
# is the locked behavior — the 740Li engine ONLY runs when fresh listings exist.
FALLBACK_LISTINGS: List[Dict] = []


# ============================================================
# IN-MEMORY CACHE (thread-safe)
# ============================================================

_cache = {
    "car_data": None,
    "depreciation": None,
    "multipliers": None,
    "meta": None,
    "listings": None,
    "listings_source": "uninitialized",
    "listings_last_error": None,
    "last_fetch": 0,
    "last_error": None,
    "source": "uninitialized",
}
_cache_lock = threading.Lock()


def _fetch_csv(url: str) -> List[Dict[str, str]]:
    """Fetch one CSV URL and return as list of row dicts. Raises on failure."""
    if not url:
        raise ValueError("Empty URL")
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    return [row for row in reader]


def _parse_car_prices(rows: List[Dict[str, str]]) -> Dict:
    """
    Parse car_prices tab into nested CAR_DATA.

    Columns: make, model, variant, fuel, ex_showroom_price, active, notes

    Output structure per (make, model):
      {
        "variants":            {variant: price}            # FIRST-seen-fuel price per variant
        "variant_fuel_prices": {variant: {fuel: price}}    # NEW: full per-(variant, fuel) prices
        "fuels":               [fuel, fuel, ...]           # canonical-ordered fuel list
      }

    The dual-dict design preserves backward compat:
      - Templates / dropdowns / JSON serialization keep reading `variants` (flat)
      - The pricing engine reads `variant_fuel_prices` via get_variant_base_price()
        when a fuel is provided and per-fuel data exists, falling back to `variants`
        otherwise.

    Why this matters: previously, if the sheet had Honda City V Petrol=11.99L,
    Diesel=11.05L, HEV=11.05L (three rows), the parser kept only the first row's
    price (11.99L) for variant V and dropped the others silently. The engine then
    used 11.99L for ALL three fuels, applied a fuel_premium multiplier on top,
    and produced wrong valuations for Diesel and HEV. Now both prices are kept.
    """
    data = {}
    for row in rows:
        active = str(row.get("active", "TRUE")).strip().upper()
        if active not in ("TRUE", "YES", "1", ""):
            continue
        make    = str(row.get("make", "")).strip()
        model   = str(row.get("model", "")).strip()
        variant = str(row.get("variant", "")).strip()
        fuel    = str(row.get("fuel", "")).strip()
        price_str = str(row.get("ex_showroom_price", "")).replace(",", "").strip()
        if not (make and model and variant and fuel and price_str):
            continue
        try:
            price = int(float(price_str))
        except ValueError:
            continue
        if make not in data:
            data[make] = {}
        if model not in data[make]:
            data[make][model] = {
                "variants": {},
                "variant_fuel_prices": {},  # NEW
                "fuels": [],
            }
        # Flat dict — keeps the FIRST-seen-fuel price for backward compat.
        if variant not in data[make][model]["variants"]:
            data[make][model]["variants"][variant] = price
        # NEW per-(variant, fuel) dict — captures every distinct fuel price.
        # Last write wins if the sheet has duplicate (variant, fuel) rows,
        # which would be a sheet-side data error worth flagging separately.
        if variant not in data[make][model]["variant_fuel_prices"]:
            data[make][model]["variant_fuel_prices"][variant] = {}
        data[make][model]["variant_fuel_prices"][variant][fuel] = price
        # Fuel list — unchanged.
        if fuel not in data[make][model]["fuels"]:
            data[make][model]["fuels"].append(fuel)
    fuel_order = ["Petrol", "Diesel", "CNG", "HEV", "PHEV", "BEV"]
    for make in data:
        for model in data[make]:
            data[make][model]["fuels"] = sorted(
                data[make][model]["fuels"],
                key=lambda f: fuel_order.index(f) if f in fuel_order else 99,
            )
    return data


def _parse_depreciation(rows: List[Dict[str, str]]) -> Dict[int, float]:
    """Columns: year_age, retention_pct"""
    out = {}
    for row in rows:
        try:
            age = int(str(row.get("year_age", "")).strip())
            pct = float(str(row.get("retention_pct", "")).strip())
            out[age] = pct
        except (ValueError, TypeError):
            continue
    return out


def _parse_multipliers(rows: List[Dict[str, str]]) -> Dict:
    """Columns: category, key, multiplier"""
    out = {"condition": {}, "owner": {}, "fuel_premium": {}}
    for row in rows:
        cat = str(row.get("category", "")).strip()
        key = str(row.get("key", "")).strip()
        try:
            val = float(str(row.get("multiplier", "")).strip())
        except (ValueError, TypeError):
            continue
        if cat in out and key:
            out[cat][key] = val
    return out


def _parse_meta(rows: List[Dict[str, str]]) -> Dict[str, str]:
    """Columns: key, value"""
    out = {}
    for row in rows:
        k = str(row.get("key", "")).strip()
        v = str(row.get("value", "")).strip()
        if k:
            out[k] = v
    return out


def _parse_listings(rows: List[Dict[str, str]]) -> List[Dict]:
    """
    Parse listings tab into list of normalized dicts.

    Expected columns (per listings_sheet_spec.md):
      make, model, variant, fuel, year, mileage, condition, owner,
      asking_price, location_city, location_state,
      listed_date, expires_date, active, source_url

    Required: make, model, variant, fuel, year, mileage, asking_price, listed_date, active
    Returns: list of dicts with normalized types (year/mileage/asking_price as ints,
    listed_date/expires_date as date objects, active as bool, others as stripped strings).
    """
    out = []
    for row in rows:
        # Filter inactive
        active_raw = str(row.get("active", "TRUE")).strip().upper()
        if active_raw not in ("TRUE", "YES", "1"):
            continue

        make = str(row.get("make", "")).strip()
        model = str(row.get("model", "")).strip()
        variant = str(row.get("variant", "")).strip()
        fuel = str(row.get("fuel", "")).strip()
        if not (make and model and variant and fuel):
            continue

        # Required numeric fields
        try:
            year = int(str(row.get("year", "")).strip())
            mileage = int(str(row.get("mileage", "")).replace(",", "").strip())
            asking_price = int(str(row.get("asking_price", "")).replace(",", "").replace("₹", "").strip())
        except (ValueError, TypeError):
            continue

        # Sanity bounds (silently drop obviously bad rows)
        if year < 1990 or year > CURRENT_YEAR + 1:
            continue
        if mileage < 0 or mileage > 1_000_000:
            continue
        if asking_price <= 0 or asking_price > 1_000_000_000:  # 100 Cr cap
            continue

        # Optional fields
        condition = str(row.get("condition", "")).strip() or None
        owner = str(row.get("owner", "")).strip() or None
        location_city = str(row.get("location_city", "")).strip() or None
        location_state = str(row.get("location_state", "")).strip() or None
        source_url = str(row.get("source_url", "")).strip() or None

        # Dates — listed_date is required, expires_date optional
        listed_date_raw = str(row.get("listed_date", "")).strip()
        listed_date = None
        if listed_date_raw:
            try:
                listed_date = datetime.strptime(listed_date_raw, "%Y-%m-%d").date()
            except ValueError:
                # Try other common formats as fallback
                for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%m/%d/%Y"):
                    try:
                        listed_date = datetime.strptime(listed_date_raw, fmt).date()
                        break
                    except ValueError:
                        continue
        if not listed_date:
            # Required field — drop row
            continue

        expires_date_raw = str(row.get("expires_date", "")).strip()
        expires_date = None
        if expires_date_raw:
            try:
                expires_date = datetime.strptime(expires_date_raw, "%Y-%m-%d").date()
            except ValueError:
                for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%m/%d/%Y"):
                    try:
                        expires_date = datetime.strptime(expires_date_raw, fmt).date()
                        break
                    except ValueError:
                        continue

        out.append({
            "make": make,
            "model": model,
            "variant": variant,
            "fuel": fuel,
            "year": year,
            "mileage": mileage,
            "condition": condition,
            "owner": owner,
            "asking_price": asking_price,
            "location_city": location_city,
            "location_state": location_state,
            "listed_date": listed_date,
            "expires_date": expires_date,
            "source_url": source_url,
        })

    return out


def _merge_with_fallback(sheet_data: Dict, fallback_data: Dict) -> Dict:
    """
    Merge sheet data on top of fallback. Sheet wins where it has data.
    Variants present only in fallback remain available.

    NOTE: The fallback dict uses the OLD shape (no `variant_fuel_prices`).
    This merge function:
      - Always initializes `variant_fuel_prices` as an empty dict for every
        (make, model) seeded from fallback, so downstream lookups never
        KeyError on the new field.
      - Copies sheet-side `variant_fuel_prices` over the empty placeholder
        when the sheet provides data for that (make, model).
      - Leaves `variant_fuel_prices` empty for fallback-only entries — the
        engine then falls through to the flat `variants` lookup gracefully.
    """
    merged = {}
    for make, models in fallback_data.items():
        merged[make] = {}
        for model, data in models.items():
            merged[make][model] = {
                "variants": dict(data["variants"]),
                "variant_fuel_prices": {},  # NEW: empty for fallback-only
                "fuels": list(data["fuels"]),
            }
    for make, models in sheet_data.items():
        if make not in merged:
            merged[make] = {}
        for model, data in models.items():
            if model not in merged[make]:
                merged[make][model] = {
                    "variants": {},
                    "variant_fuel_prices": {},
                    "fuels": [],
                }
            # Flat variants — sheet wins per-variant.
            for variant, price in data["variants"].items():
                merged[make][model]["variants"][variant] = price
            # Per-fuel prices — sheet wins per-(variant, fuel).
            sheet_vf = data.get("variant_fuel_prices", {})
            for variant, fuel_map in sheet_vf.items():
                if variant not in merged[make][model]["variant_fuel_prices"]:
                    merged[make][model]["variant_fuel_prices"][variant] = {}
                for fuel, price in fuel_map.items():
                    merged[make][model]["variant_fuel_prices"][variant][fuel] = price
            # Fuel list — union with canonical sort.
            fuel_set = set(merged[make][model]["fuels"]) | set(data["fuels"])
            fuel_order = ["Petrol", "Diesel", "CNG", "HEV", "PHEV", "BEV"]
            merged[make][model]["fuels"] = [f for f in fuel_order if f in fuel_set]
    return merged


def _refresh_cache(force: bool = False) -> Dict:
    """Refresh cache from Google Sheets, merging with fallback."""
    with _cache_lock:
        now = time.time()
        age = now - _cache["last_fetch"]
        if not force and _cache["car_data"] is not None and age < PRICE_CACHE_TTL_SECONDS:
            return {
                "status": "cache_hit",
                "age_seconds": int(age),
                "next_refresh_in": int(PRICE_CACHE_TTL_SECONDS - age),
                "source": _cache["source"],
            }

        errors = []
        sheet_car_data = {}
        used_sheet_prices = False
        try:
            rows = _fetch_csv(GSHEET_PRICES_URL)
            sheet_car_data = _parse_car_prices(rows)
            if not sheet_car_data:
                raise ValueError("Parsed car_prices tab is empty")
            used_sheet_prices = True
        except Exception as e:
            errors.append(f"prices: {e}")

        _cache["car_data"] = _merge_with_fallback(sheet_car_data, FALLBACK_CAR_DATA)

        try:
            rows = _fetch_csv(GSHEET_DEPRECIATION_URL)
            curve = _parse_depreciation(rows)
            _cache["depreciation"] = curve if curve else FALLBACK_DEPRECIATION_CURVE
        except Exception as e:
            errors.append(f"depreciation: {e}")
            _cache["depreciation"] = FALLBACK_DEPRECIATION_CURVE

        try:
            rows = _fetch_csv(GSHEET_MULTIPLIERS_URL)
            mults = _parse_multipliers(rows)
            if mults and any(mults.values()):
                for cat in ("condition", "owner", "fuel_premium"):
                    if not mults.get(cat):
                        mults[cat] = FALLBACK_MULTIPLIERS[cat]
                _cache["multipliers"] = mults
            else:
                _cache["multipliers"] = FALLBACK_MULTIPLIERS
        except Exception as e:
            errors.append(f"multipliers: {e}")
            _cache["multipliers"] = FALLBACK_MULTIPLIERS

        try:
            rows = _fetch_csv(GSHEET_META_URL)
            meta = _parse_meta(rows)
            _cache["meta"] = meta if meta else FALLBACK_META
        except Exception as e:
            errors.append(f"meta: {e}")
            _cache["meta"] = FALLBACK_META

        # ============================================================
        # Listings fetch (independent failure mode from prices)
        # If listings fail, the engine simply can't use the 740Li engine
        # for any car, and falls back to depreciation. Prices/etc still work.
        # ============================================================
        try:
            if not GSHEET_LISTINGS_URL:
                # Env var not configured yet — silent skip, no error
                _cache["listings"] = FALLBACK_LISTINGS
                _cache["listings_source"] = "fallback"
                _cache["listings_last_error"] = None
            else:
                rows = _fetch_csv(GSHEET_LISTINGS_URL)
                listings = _parse_listings(rows)
                _cache["listings"] = listings
                _cache["listings_source"] = "sheet" if listings else "fallback"
                _cache["listings_last_error"] = None if listings else "Parsed listings tab is empty"
        except Exception as e:
            _cache["listings"] = FALLBACK_LISTINGS
            _cache["listings_source"] = "fallback"
            _cache["listings_last_error"] = str(e)
            # Don't append to top-level errors — listings failure is informational only
            # (engine routing handles it gracefully via N>=5 check)

        _cache["last_fetch"] = now
        _cache["last_error"] = "; ".join(errors) if errors else None
        if used_sheet_prices and not errors:
            _cache["source"] = "sheet"
        elif used_sheet_prices:
            _cache["source"] = "mixed (sheet prices + fallback for other tabs)"
        else:
            _cache["source"] = "fallback"

        # Compute fuel-distinct price count for diagnostics — how many
        # (make, model, variant, fuel) tuples have a sheet-sourced price.
        fuel_distinct_prices = 0
        for m in _cache["car_data"].values():
            for d in m.values():
                for v_map in d.get("variant_fuel_prices", {}).values():
                    fuel_distinct_prices += len(v_map)

        return {
            "status": "refreshed" if not errors else "partial_error",
            "source": _cache["source"],
            "errors": _cache["last_error"],
            "makes": len(_cache["car_data"]),
            "models": sum(len(m) for m in _cache["car_data"].values()),
            "variants": sum(
                len(d["variants"])
                for m in _cache["car_data"].values()
                for d in m.values()
            ),
            "fuel_distinct_prices": fuel_distinct_prices,  # NEW
            "data_version": _cache["meta"].get("data_version", "?"),
            "sheet_overrides": sum(
                len(d["variants"])
                for m in sheet_car_data.values()
                for d in m.values()
            ),
            "listings_count": len(_cache["listings"]) if _cache["listings"] else 0,
            "listings_source": _cache["listings_source"],
            "listings_error": _cache["listings_last_error"],
        }


def _ensure_loaded():
    """First-access lazy load."""
    if _cache["car_data"] is None:
        _refresh_cache(force=True)


# ============================================================
# PUBLIC API
# ============================================================

def refresh_prices(force: bool = True) -> Dict:
    """Public entrypoint to force-reload from Google Sheets."""
    result = _refresh_cache(force=force)
    # Keep CAR_DATA LazyDict in sync so json.dumps(CAR_DATA) reflects new data
    try:
        CAR_DATA._sync()
    except Exception:
        pass
    return result


def get_cache_status() -> Dict:
    """Diagnostics for admin dashboard."""
    _ensure_loaded()
    # Compute fuel-distinct count on demand
    fuel_distinct_prices = 0
    for m in _cache["car_data"].values():
        for d in m.values():
            for v_map in d.get("variant_fuel_prices", {}).values():
                fuel_distinct_prices += len(v_map)
    return {
        "source": _cache["source"],
        "last_fetch_age_seconds": int(time.time() - _cache["last_fetch"]),
        "last_error": _cache["last_error"],
        "cache_ttl": PRICE_CACHE_TTL_SECONDS,
        "makes": len(_cache["car_data"]),
        "fuel_distinct_prices": fuel_distinct_prices,  # NEW — admin can see how many per-fuel prices loaded
        "data_version": _cache["meta"].get("data_version", "?"),
        "last_updated": _cache["meta"].get("last_updated", "?"),
        "listings_count": len(_cache["listings"]) if _cache["listings"] else 0,
        "listings_source": _cache["listings_source"],
        "listings_last_error": _cache["listings_last_error"],
    }


def get_makes() -> List[str]:
    _ensure_loaded()
    return sorted(_cache["car_data"].keys())


def get_models(make: str) -> List[str]:
    _ensure_loaded()
    if make not in _cache["car_data"]:
        return []
    return sorted(_cache["car_data"][make].keys())


def get_variants(make: str, model: str) -> List[str]:
    """Variants in TRIM order (base → top). Sheet order preserved."""
    _ensure_loaded()
    try:
        return list(_cache["car_data"][make][model]["variants"].keys())
    except KeyError:
        return []


def get_fuels(make: str, model: str) -> List[str]:
    """Fuels in canonical order (Petrol/Diesel/CNG/HEV/PHEV/BEV)."""
    _ensure_loaded()
    try:
        return list(_cache["car_data"][make][model]["fuels"])
    except KeyError:
        return []


def get_variant_base_price(make: str, model: str, variant: str,
                           fuel: Optional[str] = None) -> Optional[int]:
    """
    Returns the ex-showroom base price for a (variant, fuel) pair.

    Lookup order:
      1. If `fuel` is provided AND `variant_fuel_prices[variant][fuel]` exists,
         return that exact per-fuel price.
      2. Otherwise, fall through to the flat `variants[variant]` price (which
         is the FIRST-seen-fuel price from the parser, preserving previous
         behavior for callers that don't pass `fuel`).
      3. If neither lookup hits, return None.

    This means cars where the sheet has distinct per-fuel prices for the same
    variant (e.g. Honda City V Petrol=11.99L vs Diesel=11.05L vs HEV=11.05L)
    now produce correct fuel-specific valuations. Previously, all three fuels
    silently used the Petrol price plus the fuel_premium multiplier on top.

    Backward compat:
      - Callers that don't pass `fuel` get the same answer as before.
      - Callers that pass `fuel` for a (variant, fuel) without per-fuel data
        in the sheet (e.g. fallback-only entries, or models still on the old
        flat shape) gracefully fall through to the flat price. They will not
        get a None where they previously got a number.
    """
    _ensure_loaded()
    try:
        model_data = _cache["car_data"][make][model]
    except KeyError:
        return None

    # Step 1: try per-fuel lookup
    if fuel:
        vfp = model_data.get("variant_fuel_prices", {})
        variant_fuels = vfp.get(variant)
        if variant_fuels and fuel in variant_fuels:
            return variant_fuels[fuel]

    # Step 2: fall through to flat
    return model_data["variants"].get(variant)


def get_base_price(make: str, model: str) -> Optional[int]:
    """
    Returns the LOWEST-priced variant for a model — used as a rough anchor
    in the v2.8 ex-showroom ceiling fallback path and admin guardrail checks.

    Returns the minimum across the flat `variants` dict, which is deterministic
    regardless of dict iteration order.
    """
    _ensure_loaded()
    try:
        variants = _cache["car_data"][make][model]["variants"]
        if not variants:
            return None
        return min(variants.values())
    except KeyError:
        return None


def get_retention_for_age(years_old: int) -> float:
    """Returns depreciation retention multiplier (0.0-1.0)."""
    _ensure_loaded()
    curve = _cache["depreciation"]
    if years_old <= 0:
        return 1.0
    if years_old in curve:
        return curve[years_old] / 100.0
    max_age = max(curve.keys())
    if years_old >= max_age:
        return curve[max_age] / 100.0
    lower = max(k for k in curve if k < years_old)
    upper = min(k for k in curve if k > years_old)
    frac = (years_old - lower) / (upper - lower)
    return (curve[lower] + (curve[upper] - curve[lower]) * frac) / 100.0


def get_multiplier(category: str, key: str) -> float:
    _ensure_loaded()
    return _cache["multipliers"].get(category, {}).get(key, 1.0)


# ============================================================
# LISTINGS API (consumed by 740Li engine in pricing_740Li.py)
# ============================================================

def get_listings_for_car(make: str, model: str, variant: str, fuel: str,
                          year: Optional[int] = None,
                          year_window: int = 2,
                          freshness_days: Optional[int] = None) -> List[Dict]:
    """
    Returns active, fresh listings matching the requested car spec.

    Filters applied (in order):
      1. Match make + model + variant + fuel exactly (case-sensitive — must match
         the prices sheet exactly; mismatches are the founder's responsibility)
      2. If year provided, filter year within +/- year_window (default ±2)
      3. Drop listings older than freshness_days (default LISTINGS_DEFAULT_FRESHNESS_DAYS)
         OR past their explicit expires_date
      4. Already filtered to active=TRUE in _parse_listings()

    Args:
        make, model, variant, fuel: car spec (must match prices sheet)
        year: target year (optional). If None, all years included.
        year_window: tolerance around year (default ±2). Ignored if year is None.
        freshness_days: max listing age in days. Defaults to 60.

    Returns:
        List of listing dicts. Empty if nothing matches.
        Each dict has keys: make, model, variant, fuel, year, mileage, condition,
        owner, asking_price, location_city, location_state, listed_date, expires_date,
        source_url.
    """
    _ensure_loaded()
    listings = _cache.get("listings") or []
    if not listings:
        return []

    if freshness_days is None:
        freshness_days = LISTINGS_DEFAULT_FRESHNESS_DAYS

    today = datetime.now().date()
    from datetime import timedelta
    cutoff_date = today - timedelta(days=freshness_days)

    matched = []
    for L in listings:
        if L["make"] != make: continue
        if L["model"] != model: continue
        if L["variant"] != variant: continue
        if L["fuel"] != fuel: continue

        if year is not None:
            if abs(L["year"] - int(year)) > year_window:
                continue

        # Freshness: explicit expires_date wins; else listed_date + freshness_days
        if L.get("expires_date"):
            if L["expires_date"] < today:
                continue
        else:
            if L.get("listed_date") and L["listed_date"] < cutoff_date:
                continue

        matched.append(L)

    return matched


def get_listings_freshness_status() -> Dict:
    """
    Diagnostic for app.py routing logic — quick check whether listings cache
    is healthy enough to attempt 740Li engine routing for any car.

    Returns dict with:
      - has_data: bool — at least 1 fresh listing in cache
      - total_count: int — total active listings (regardless of car spec)
      - source: 'sheet' | 'fallback'
      - last_error: str | None
    """
    _ensure_loaded()
    listings = _cache.get("listings") or []
    return {
        "has_data": len(listings) > 0,
        "total_count": len(listings),
        "source": _cache.get("listings_source", "uninitialized"),
        "last_error": _cache.get("listings_last_error"),
    }


# ============================================================
# PRICING FORMULA (depreciation engine — unchanged)
# ============================================================

def compute_base_valuation(make, model, variant, fuel, year, mileage, condition, owner):
    """Pure formula-based valuation. Returns int rupees or None.

    Now uses fuel-specific ex-showroom price when the sheet provides one
    (via get_variant_base_price). Falls through to flat price otherwise.
    """
    base = get_variant_base_price(make, model, variant, fuel)
    if base is None:
        return None

    age = max(0, CURRENT_YEAR - int(year))
    age = min(age, 15)

    retention = get_retention_for_age(age)
    price = base * retention

    try:
        mileage = int(mileage or 0)
    except (TypeError, ValueError):
        mileage = 0
    expected_km = age * EXPECTED_KM_PER_YEAR
    excess_km = max(0, mileage - expected_km)
    mileage_penalty = (excess_km / 10000) * 0.02
    mileage_penalty = min(mileage_penalty, 0.25)
    price *= (1 - mileage_penalty)

    price *= get_multiplier("condition", condition or "Good")
    price *= get_multiplier("owner", owner or "1st Owner")
    price *= get_multiplier("fuel_premium", fuel or "Petrol")

    return int(round(price))


def compute_price_range(estimated_price: Optional[int], phase: int = 1) -> Tuple:
    """Range width scales with phase: ±12% at Phase 1 → ±5% at Phase 4."""
    if estimated_price is None:
        return (None, None)
    range_pct = PHASE_BLEND.get(phase, PHASE_BLEND[1])["range_pct"]
    low = int(round(estimated_price * (1 - range_pct)))
    high = int(round(estimated_price * (1 + range_pct)))
    return (low, high)


# ============================================================
# PHASE DETERMINATION
# ============================================================

def determine_phase(deal_count: int, distinct_users: int, previous_phase: int = 1) -> int:
    """Compute phase (1-4) from deal_count + distinct_users in last 180 days."""
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
# BLENDING + GUARDRAILS (unchanged)
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
    """Blend formula_price with deal median per phase weights."""
    if formula_price is None:
        return None, PHASE_BLEND[1]["conf_base"]
    cfg = PHASE_BLEND.get(phase, PHASE_BLEND[1])
    n = len(similar_deals) if similar_deals else 0
    if phase == 1 or n == 0:
        return int(formula_price), cfg["conf_base"]
    sorted_prices = sorted(similar_deals)
    if n >= OUTLIER_TRIM_MIN_DEALS:
        sorted_prices = _trim_outliers(sorted_prices)
    median = _median(sorted_prices)
    if median is None:
        return int(formula_price), cfg["conf_base"]
    fw = cfg["formula_weight"]
    rw = cfg["real_weight"]
    blended = formula_price * fw + median * rw
    upper = formula_price * (1 + GUARDRAIL_MAX_DEVIATION)
    lower = formula_price * (1 - GUARDRAIL_MAX_DEVIATION)
    blended = min(upper, max(lower, blended))
    conf = cfg["conf_base"] + min(8, n // 4)
    conf = min(conf, CONF_CEILING[phase])
    return int(round(blended)), conf


def get_phase_display(phase):
    """Returns {'phase','badge','detail','tooltip'} for template rendering."""
    cfg = PHASE_BLEND.get(phase, PHASE_BLEND[1])
    return {
        "phase": phase,
        "badge": cfg["badge"],
        "detail": cfg["badge_detail"],
        "tooltip": cfg["tooltip"],
    }


# ============================================================
# BACKWARDS-COMPATIBLE MODULE-LEVEL ATTRIBUTES
# ============================================================

class _LazyDict(dict):
    """
    Acts like the CAR_DATA dict, but stays in sync with the live cache.
    Inherits from dict so json.dumps() and other dict-expecting code work
    natively. Every access re-syncs from _cache to reflect latest refreshes.

    NOTE: When this dict is JSON-serialized (e.g. for embedding in the
    seller form template), every (make, model) entry will include the new
    `variant_fuel_prices` key alongside `variants` and `fuels`. Templates
    that previously read `variants` and `fuels` are unaffected — they just
    have an extra key available. If the embedded JSON size matters for page
    weight, consider stripping `variant_fuel_prices` at serialization time
    via the to_dict() helper plus a custom transform.
    """
    def _sync(self):
        _ensure_loaded()
        cached = _cache["car_data"] or {}
        if dict.__ne__(self, cached):
            dict.clear(self)
            dict.update(self, cached)

    def __getitem__(self, key):
        self._sync()
        return dict.__getitem__(self, key)
    def __contains__(self, key):
        self._sync()
        return dict.__contains__(self, key)
    def __iter__(self):
        self._sync()
        return dict.__iter__(self)
    def __len__(self):
        self._sync()
        return dict.__len__(self)
    def keys(self):
        self._sync()
        return dict.keys(self)
    def values(self):
        self._sync()
        return dict.values(self)
    def items(self):
        self._sync()
        return dict.items(self)
    def get(self, key, default=None):
        self._sync()
        return dict.get(self, key, default)
    def to_dict(self):
        """Plain dict snapshot, useful for json.dumps(CAR_DATA) in templates."""
        self._sync()
        return dict(self)


CAR_DATA = _LazyDict()


# Module-level string constants referenced by templates/routes.
BASE_PRICE_DATA_VERSION = FALLBACK_META.get("data_version", "?")
BASE_PRICE_LAST_UPDATED = FALLBACK_META.get("last_updated", "?")

# Module-level flag for app.py routing logic
LISTINGS_DATA_FRESH = False


def refresh_module_constants():
    """Call after a price refresh to sync module-level string constants."""
    global BASE_PRICE_DATA_VERSION, BASE_PRICE_LAST_UPDATED, LISTINGS_DATA_FRESH
    _ensure_loaded()
    BASE_PRICE_DATA_VERSION = _cache["meta"].get("data_version", "?")
    BASE_PRICE_LAST_UPDATED = _cache["meta"].get("last_updated", "?")
    LISTINGS_DATA_FRESH = bool(_cache.get("listings"))


# ============================================================
# STARTUP — attempt first load but don't crash if network is down
# ============================================================
try:
    _refresh_cache(force=True)
    refresh_module_constants()
    # Hydrate the CAR_DATA LazyDict from the cache so json.dumps(CAR_DATA)
    # works immediately, before any dict method is called on it.
    CAR_DATA._sync()
    _src = _cache.get("source", "?")
    _n = len(_cache["car_data"]) if _cache["car_data"] else 0
    _ln = len(_cache["listings"]) if _cache["listings"] else 0
    _lsrc = _cache.get("listings_source", "?")
    # Count fuel-distinct prices for startup log
    _vfp_count = sum(
        len(v_map)
        for m in _cache["car_data"].values()
        for d in m.values()
        for v_map in d.get("variant_fuel_prices", {}).values()
    )
    print(f"[car_data] Loaded {_n} makes. Source: {_src}. "
          f"Per-fuel prices: {_vfp_count}. "
          f"Listings: {_ln} ({_lsrc}).")
except Exception as _e:
    print(f"[car_data] Startup load failed: {_e}. Will retry on first request. "
          f"Fallback data available in FALLBACK_CAR_DATA.")
