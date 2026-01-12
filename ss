import sys
import subprocess
import os
import io
import traceback
import re
import copy

# ==========================================
# 📦 DEPENDENCY CHECK
# ==========================================
try:
    import ipyvuetify as v
    if v.__version__ != '1.9.4':
        print("ipyvuetify version mismatch. Installing 1.9.4...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "ipyvuetify==1.9.4"])
        print("Installation complete. Please restart the kernel.")
except ImportError:
    print("ipyvuetify not found. Installing 1.9.4...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "ipyvuetify==1.9.4"])

import pandas as pd
import numpy as np
import json
import ipyvuetify as v
import ipywidgets as widgets
import traitlets
from datetime import datetime, timedelta, date, time, timezone
from seeq import spy

# ==========================================
# 🔧 SEEQ & GLOBAL CONFIGURATION
# ==========================================

spy.options.compatibility = 196

# Define Mill Timezone (UTC-6)
MILL_TZ = timezone(timedelta(hours=-6))
MILL_START_HOUR = 7

# Mill Options
MILL_OPTIONS = ['PM12', 'PM14', 'PM15', 'PM16', 'PM17', 'PM18']

# --- CHEMICAL CATEGORY MAPPING ---
CHEMICAL_CATEGORY_MAP = {
    'Starch': 'Starch', 
    'Hercobond': 'Strength', 'Fennotech': 'Strength', 'Impress': 'Strength',
    'Fennopol': 'Drainage', 'Axfloc': 'Drainage', 'Bufloc': 'Drainage', 'Fennofloc': 'Drainage', 'Polymer': 'Drainage',
    'Biocide': 'Biocide', 'Busan': 'Biocide', 'Bleach': 'Biocide', 'DBNPA': 'Biocide', 'Hydrogen Peroxide': 'Biocide', 'B-150': 'Biocide',
    'Busperse': 'Cleaner', 'Talc': 'Cleaner', 'Zee': 'Cleaner', 'Presstige': 'Cleaner', 'Superzymer': 'Cleaner', 
    'Carta': 'Color',
    'Defoamer': 'Defoamer',
    'ASA': 'Sizing', 'FennoSize': 'Sizing', 'PAC': 'Sizing', 'Sulfuric': 'Sizing', 'Alum': 'Sizing', 'Bubond': 'Sizing'
}

DEFAULT_CATEGORY = 'Other'
CATEGORY_ORDER = ['Starch', 'Strength', 'Drainage', 'Sizing', 'Biocide', 'Cleaner', 'Color', 'Defoamer', 'Other']

# --- SHARED GRADE MAPPING ---
GLOBAL_GRADE_ALIAS_MAPPING = {
    '23M': '23MED',  '23MED': '23MED',
    '25M': '25MED',  '25MED': '25MED',
    '25P': '25PS',   '25PS': '25PS',
    '30M': '30MED',  '30MED': '30MED',
    '30P': '30PS',   '30PS': '30PS',
    '33M': '33MED',  '33MED': '33MED',
    '33P': '33PS',   '33PS': '33PS',
    '35P': '35PS',   '35PS': '35PS',
    '36M': '36MED',  '36MED': '36MED',
    '40M': '40MEDW', '40MEDW': '40MEDW',
    '40P': '40PS',   '40PS': '40PS', '40': '40PS',
    '56L': '56LIN',  '56LIN': '56LIN', '56': '56LIN',
    '26M': '26MED',  '26MED': '26MED'
}

# Mill Configs
ALL_MILL_CONFIGS = {
    'PM12': {
        'TIMEZONE': 'US/Eastern',
        'GRADE_RUN_CONDITION_ID': "0F0B36D6-E982-FFC0-ADDF-BEE2756E54A0",
        'MILL_CALENDAR_CONDITION_ID': "0F0B36D9-95C1-6000-B2BC-72DEAC0B40FD",
        'SCHEDULE_FILE': 'PM12_Run_Schedule.csv',
        'DOWNTIME_FILE': 'PM12_Known_Downtime.csv',
        'TONS_GRADE_FILE': 'PM12_Tons_Per_Grade.csv',
        'TONS_PER_DAY_BY_GRADE': { '23MED': 839, '25MED': 839, '25PS': 839, '30MED': 900, '30PS': 900, '33MED': 974, '33PS': 974, '35PS': 974, '36MED': 1035, '40MEDW': 1050, '40PS': 1050, '56LIN': 1130, '23MDX': 839 }
    },
    'PM14': {
        'TIMEZONE': 'US/Eastern',
        'GRADE_RUN_CONDITION_ID': "0F0B3739-47AE-FBA0-BA72-DC9D67866EF8",
        'MILL_CALENDAR_CONDITION_ID': "0F0B3736-E4E9-64E0-B86D-FBE63FC762DA",
        'SCHEDULE_FILE': 'PM14_Run_Schedule.csv',
        'DOWNTIME_FILE': 'PM14_Known_Downtime.csv',
        'TONS_GRADE_FILE': 'PM14_Tons_Per_Grade.csv',
        'TONS_PER_DAY_BY_GRADE': { '23MED': 780, '25MED': 780, '25PS': 780, '30MED': 955, '30PS': 955, '33MED': 1055, '33PS': 1055, '35PS': 1055, '36MED': 1150, '40MEDW': 1050, '40PS': 1050, '56LIN': 1130, '23MDX': 780 }
    },
    'PM15': {
        'TIMEZONE': 'US/Eastern',
        'GRADE_RUN_CONDITION_ID': "0F0B017D-1E6B-66E0-956C-4A79D53BC22A",
        'MILL_CALENDAR_CONDITION_ID': "0F0B0182-B2ED-EAC0-8A77-185286B6A3CA",
        'SCHEDULE_FILE': 'PM15_Run_Schedule.csv',
        'DOWNTIME_FILE': 'PM15_Known_Downtime.csv',
        'TONS_GRADE_FILE': 'PM15_Tons_Per_Grade.csv',
        'TONS_PER_DAY_BY_GRADE': { '23MED': 985, '25MED': 985, '25PS': 985, '30MED': 1204, '30PS': 1204, '33MED': 1250, '33PS': 1250, '35PS': 1280, '36MED': 1254, '40MEDW': 1302, '40PS': 1302, '56LIN': 1323, '23MDX': 985 }
    },
    'PM16': {
        'TIMEZONE': 'US/Eastern',
        'GRADE_RUN_CONDITION_ID': "0F0B3769-D1A3-EA60-94A2-D144E7484305",
        'MILL_CALENDAR_CONDITION_ID': "0F0B376B-3E68-FFF0-B3BB-83246EE42DBA",
        'SCHEDULE_FILE': 'PM16_Run_Schedule.csv',
        'DOWNTIME_FILE': 'PM16_Known_Downtime.csv',
        'TONS_GRADE_FILE': 'PM16_Tons_Per_Grade.csv',
        'TONS_PER_DAY_BY_GRADE': { '23MED': 985, '25MED': 985, '25PS': 985, '30MED': 1204, '30PS': 1204, '33MED': 1250, '33PS': 1250, '35PS': 1280, '36MED': 1254, '40MEDW': 1302, '40PS': 1302, '56LIN': 1323, '23MDX': 985 }
    },
    'PM17': {
        'TIMEZONE': 'US/Eastern',
        'GRADE_RUN_CONDITION_ID': "0F0B3775-7859-FDE0-AF1A-223AEE8E3665",
        'MILL_CALENDAR_CONDITION_ID': "0F0B3773-D193-EE30-BF72-5201D2D9891D",
        'SCHEDULE_FILE': 'PM17_Run_Schedule.csv',
        'DOWNTIME_FILE': 'PM17_Known_Downtime.csv',
        'TONS_GRADE_FILE': 'PM17_Tons_Per_Grade.csv',
        'TONS_PER_DAY_BY_GRADE': { '23MED': 985, '25MED': 985, '25PS': 985, '30MED': 1204, '30PS': 1204, '33MED': 1250, '33PS': 1250, '35PS': 1280, '36MED': 1254, '40MEDW': 1302, '40PS': 1302, '56LIN': 1323, '23MDX': 985 }
    },
    'PM18': {
        'TIMEZONE': 'US/Eastern',
        'GRADE_RUN_CONDITION_ID': "0F0ADC22-BA46-64A0-B604-CCCAB3C0B4E1",
        'MILL_CALENDAR_CONDITION_ID': "0F0ADD67-DEEB-FD50-8855-FE740190C30B",
        'SCHEDULE_FILE': 'PM18_Run_Schedule.csv',
        'DOWNTIME_FILE': 'PM18_Known_Downtime.csv',
        'TONS_GRADE_FILE': 'PM18_Tons_Per_Grade.csv',
        'TONS_PER_DAY_BY_GRADE': { '23MED': 1163, '25MED': 1203, '25PS': 1203, '30MED': 1277, '30PS': 1277, '33MED': 1450, '33PS': 1450, '35PS': 1377, '36MED': 1380, '40MEDW': 1525, '40PS': 1525, '56LIN': 1329, '23MDX': 1163 }
    }
}

CHEMICAL_SIGNAL_IDS = {
    'PM12': {"Starch, Pearl": "0F0A3912-735F-FB80-B942-205491349CC3", "Bleach": "0F0C982D-477D-7540-A923-8647AF709DC8", "Busan 1215": "0F0C982D-482F-E8D0-B511-9EF5B4DE0D7C", "Carta Red": "0F0C982D-47F2-E840-A26E-7DB7C48FA268", "Carta Yellow": "0F0C982D-4917-77C0-9FB1-49283ECBF999", "Fennopol K2801": "0F0C982D-497E-6060-95AF-3813083FF354", "Impress SB-973": "0F0C982D-49C9-FB50-B41F-7BF4934BC712"},
    'PM14': {"Starch, Pearl": "0F09EDAA-B6DB-EC00-A342-B4E27D9263DA", "Axfloc AF 9620PG": "0F0C982D-48CB-ECD0-89A6-F511F86FA4AC", "Bleach": "0F0ECC80-C55A-E880-863F-E04F294C3550", "Busan 1215": "0F0C982D-4885-6000-B859-E6BC515F8CEF", "Busperse 2036A": "0F0C982D-4A12-FF30-AE9B-2E1B10A21FE4", "Carta Red": "0F0C982D-4BA3-7570-938D-8FF765B02B92", "Carta Yellow": "0F0C982D-4A63-E840-92F3-34CCCD6A09B6", "Fennotech 1802": "0F0C982D-4B30-F980-8134-A68F9DA5485C", "Hercobond Plus 555": "0F0C982D-4B61-66C0-87F1-06126997289C", "Impress SB973": "0F0C982D-4A94-7580-9118-3075F3F63AA4", "Talc Compacted": "0F0C982D-4AE7-75A0-80EB-3449E77CB4A7"},
    'PM15': {"Starch": "0F0BE550-792B-EC80-91BD-8FF66D11A60C", "ASA 150-D": "0F0C982D-4D4C-6250-B711-9DBFBB55EEC3", "Bleach": "0F0C982D-4C92-F990-92A9-A4CF5E8337E1", 
             "Bubond 650": "0F0C982D-4CEA-77D0-A07C-9D5012356D06", "Bufloc 5031": "0F0C982D-4DAD-ECD0-B458-DF1E3F1FCDAB", "Busan 1215": "0F0C982D-4DFB-EED0-8B80-9DAA57D007F0",
             "Carta Red": "0F0C982D-4C29-F9E0-A15A-826F035FDB0E", "Carta Yellow": "0F0C982D-500B-6450-A375-417F5506553C", "Fennopol K2801": "0F0DC32E-13C5-FB30-9CF9-5C438C885C83", 
             "Hercobond 555": "0F0C982D-4F3E-7310-8BEA-A0DFFDC702BC", "Impress BP 400DS": "0F0C982D-4EF7-6640-B46E-56BCDEF9111F", "Zee 7635": "0F0C982D-4F9A-FF70-B73F-1EAF4FDE8C44"},
    'PM16': {"starch, pearl": "0F097F8A-D57C-7760-AF43-0D42F75A22C8", "Bleach": "0F0C982D-51DB-6230-9039-C39A1305BD1C", "Busan 1215": "0F0C982D-5194-7560-A45B-5DB0A2C72DEB", "Carta Yellow": "0F0C982D-5218-62C0-B112-65DCF6CB2562", "Carta red": "0F0C982D-5106-FBC0-B2A7-FCF33B241EE0", "Fennopol K2801": "0F0C982D-5074-6400-8DD2-1CEA42B9DCFC", "Fennotech 1802": "0F0C982D-514D-E890-9759-E333693AFD53", "PAC": "0F0C982D-50C4-FD10-9997-82858E2ABB76", "Presstige FB9080": "0F0C982D-5252-EC40-BB9E-0F3D40E86D78", "Sulfuric Acid": "0F0C982D-528A-EEB0-8372-B88B6D05139B", "Superzymer 120x": "0F0C982D-5313-EA30-AE97-FC07863F9222"},
    'PM17': {"Starch": "0F0C0B23-B6A7-66B0-8B46-47EA3959E240", "Bleach": "0F0C982D-52D6-F9A0-8B51-074BBE49B384", "Busan 1215": "0F0C982D-5534-7120-96D9-1C93269F5C32", "Busperse 2036A": "0F0C982D-54AD-ECB0-A194-50E371140912", "Carta Red": "0F0C982D-5473-7330-9684-BF7920816387", "Carta Yellow": "0F0C982D-53A3-EAE0-ABD6-1344216293B6", "FennoSize S C32": "0F0C982D-5357-FFF0-B5F2-5842B724BD3A", "Fennofloc ZN29": "0F0C982D-5567-7570-BE96-A43C67A4E8B8", "Fennopol K2801": "0F0C982D-5420-7310-B483-03255E354B45", "Fennotech 1802": "0F0C982D-54F9-77A0-8991-8B38BE6DD1A3"},
    'PM18': {"Starch": "0F0A3AE2-121D-FDF0-8A5D-F7894A0780D9", "ASA 150-D": "0F0C982D-56F7-FBB0-BAEE-9E3C24DCC41A", "B-150": "0F0C982D-55BA-7590-91CF-D33B7F5AB357", 
             "Bleach": "0F0C982D-578A-7370-B881-BCC6B2D999A6", "Bubond 650": "0F0C982D-564C-FD50-A55C-7F0118FCA3B9", "Busan 1215": "0F0C982D-573E-E880-94A0-924F03779610", 
             "Busperse 2036A": "0F0C982D-56A2-6480-BAE7-7C4BDCE9063A", "Carta Red": "0F0C982D-5824-6060-AA3B-E75EDEBBF916", "Carta Yellow": "0F0C982D-57D8-7570-82B8-9E5116F9B750", 
             "DBNPA": "0F0C982D-5950-7510-8303-99D09A3BA520", "Fennopol k2801": "0F0C982D-58B4-7110-9C75-424078452342", "Fennosize S C32": "0F0C982D-5902-7310-ACCF-04A0CFCEB0F9",
             "Fennotech 2089": "0F0C982D-59D4-6270-8016-1E59F3A4D91F", "Hydrogen Peroxide": "0F0C982D-5A24-FB80-AABB-54DF765E9336", "PAC": "0F0C982D-5999-E8F0-92E7-26AAC0B04DA3",
             "Starch Defoamer": "0F0C982D-5A5C-FDF0-B14C-BE8F7065FAB7", "Sulfuric Acid": "0F0C982D-5A8B-6420-B3E7-E35BEBE4235C"}
}

# --- DUMMY BUDGET DATA ($/TON) ---
DUMMY_BUDGETS = {
    'PM12': { "Starch, Pearl": 53.95, "Fennopol K2801": 3.37, "Busan 1215": 3.19, "Bleach": 3.14, "Impress SB-973": 1.93, "Carta Red": 0.28, "Carta Yellow": 0.12 },
    'PM14': { "Starch, Pearl": 49.38, "Busan 1215": 5.14, "Bleach": 4.82, "Impress SB973": 3.48, "Axfloc AF9620": 2.39, "Talc Compacted": 1.16, "Hercobond Plus 555": 0.89, "Fennotech 1802": 0.59, "Busperse 2036A": 0.06 },
    'PM15': { "Starch": 39.75, "Hercobond 555": 8.81, "Bleach": 3.58, "Busan 1215": 3.53, "Fennopol K2801": 2.06, "Bufloc 5031": 1.57, "Zee 7635": 1.08, "ASA 150-D": 0.93, "Impress BP 400DS": 0.36, "Bubond 650": 0.30 },
    'PM16': { "starch, pearl": 35.08, "Fennopol K2801": 3.54, "Bleach": 3.52, "Fennotech 1802": 2.50, "Presstige FB9080": 0.66, "Carta Red": 0.36, "Carta Yellow": 0.28, "Busan 1215": 0.30, "PAC": 0.14, "Superzymer 120x": 0.12 },
    'PM17': { "Starch": 42.47, "Bleach": 3.86, "Fennopol K2801": 3.12, "Busan 1215": 2.73, "Fennotech 1802": 2.35, "FennoSize S C32": 1.50, "Fennofloc ZN29": 0.90, "Busperse 2036A": 0.38, "Carta Yellow": 0.15, "Carta Red": 0.08 },
    'PM18': { "Starch": 39.97, "Busan 1215": 3.56, "Bleach": 3.54, "Fennopol k2801": 3.28, "ASA 150-D": 1.14, "Sulfuric Acid": 0.68, "DBNPA": 0.57, "Bubond 650": 0.34, "Busperse 2036A": 0.27, "Carta Yellow": 0.27, "Carta Red": 0.14 }
}

# --- GROUP BUDGET DATA ($/TON) ---
GROUP_BUDGETS = {
    'PM12': { 'Starch': 0.00, 'Strength': 0.00, 'Drainage': 0.00, 'Biocide': 0.00, 'Color': 0.00 },
    'PM14': { 'Starch': 0.00, 'Strength': 0.00, 'Drainage': 0.00, 'Biocide': 0.00, 'Cleaner': 0.00, 'Color': 0.00 },
    'PM15': { 'Starch': 33.20, 'Strength': 9.66, 'Sizing': 1.82, 'Drainage': 5.83, 'Biocide': 10.05, 'Cleaner': 0.56, 'Color': 0.29, 'Defoamer': 2.16 },
    'PM16': { 'Starch': 0.00, 'Strength': 0.00, 'Sizing': 0.00, 'Drainage': 0.00, 'Biocide': 0.00, 'Cleaner': 0.00, 'Color': 0.00 },
    'PM17': { 'Starch': 0.00, 'Strength': 0.00, 'Sizing': 0.00, 'Drainage': 0.00, 'Biocide': 0.00, 'Cleaner': 0.00, 'Color': 0.00 },
    'PM18': { 'Starch': 0.00, 'Strength': 0.00, 'Sizing': 0.00, 'Drainage': 0.00, 'Biocide': 0.00, 'Cleaner': 0.00, 'Color': 0.00, 'Defoamer': 0.00 }
}

# --- EMBEDDED PM15 BASELINES FROM JSON ---
PM15_BASELINES = {
    "ASA 150-D": {"25MED": 0.1513269436, "25PS": 1.1002565119, "26MDX": 0.4567598077, "26MED": 0.0344939545, "30MED": 0.1168304989, "30PS": 1.4329422779, "33MED": 0.2674962621, "33PS": 1.3356982806, "35PS": 1.4574146741, "36MED": 0.1612681399, "40PS": 1.3070062696, "56LIN": 1.2903775039},
    "Bleach": {"25MED": 13.1281158127, "25PS": 7.0811630785, "26MDX": 13.959015269, "26MED": 7.7820266156, "30MED": 7.8334572117, "30PS": 9.3077584778, "33MED": 13.2461604753, "33PS": 8.6718984407, "35PS": 12.2149526907, "36MED": 9.2584297395, "40PS": 8.701349959, "56LIN": 8.5060393391},
    "Bubond 650": {"25MED": 0.0936971913, "25PS": 0.6792907448, "26MDX": 0.294314284, "26MED": 0.0215246401, "30MED": 0.0716821972, "30PS": 0.9018074305, "33MED": 0.1680287284, "33PS": 0.839828077, "35PS": 0.9539941314, "36MED": 0.1012150012, "40PS": 0.8252019913, "56LIN": 0.8174203001},
    "Bufloc 5031": {"25MED": 1.76299052, "25PS": 0.9152135741, "26MDX": 1.5805208801, "26MED": 1.1021015118, "30MED": 1.1072077171, "30PS": 1.1393894111, "33MED": 1.6602638354, "33PS": 1.1350173878, "35PS": 1.3786166508, "36MED": 1.052127871, "40PS": 1.0185145574, "56LIN": 0.9900623332},
    "Busan 1215": {"25MED": 6.6673265458, "25PS": 3.4999740511, "26MDX": 7.5121374764, "26MED": 4.1268340774, "30MED": 4.0570011376, "30PS": 4.7567572796, "33MED": 6.7233159755, "33PS": 4.3636870204, "35PS": 6.7770857365, "36MED": 4.658860511, "40PS": 4.4501297768, "56LIN": 4.3714512594},
    "Carta Red": {"25MED": 0.0056132031, "25PS": 0.1851511452, "26MDX": 0.0, "26MED": 0.0, "30MED": 0.0013166892, "30PS": 0.0777896959, "33MED": 0.0033617653, "33PS": 0.0508954161, "35PS": 0.0, "36MED": 0.0132778423, "40PS": 0.0631266664, "56LIN": 0.0416013392},
    "Carta Yellow": {"25MED": 0.0044204989, "25PS": 0.1837992474, "26MDX": 0.0, "26MED": 0.0, "30MED": 0.0012422542, "30PS": 0.0608182268, "33MED": 0.0014428354, "33PS": 0.0179525389, "35PS": 0.0, "36MED": 0.0127473639, "40PS": 0.0304908414, "56LIN": 0.0327816371},
    "Fennopol K2801": {"25MED": 1.287508547, "25PS": 0.6744430064, "26MDX": 1.1179794155, "26MED": 0.8007826456, "30MED": 0.778900635, "30PS": 0.8471662309, "33MED": 1.1698744166, "33PS": 0.7585396993, "35PS": 0.9524842198, "36MED": 0.7581771098, "40PS": 0.7346362223, "56LIN": 0.7030090522},
    "Hercobond 555": {"25MED": 170.7148975238, "25PS": 77.4124842968, "26MDX": 243.000635658, "26MED": 27.436775023, "30MED": 158.7861024247, "30PS": 297.7433663159, "33MED": 317.5876819643, "33PS": 282.4773370738, "35PS": 290.4062305195, "36MED": 138.0818792396, "40PS": 190.2465513178, "56LIN": 148.7135695235},
    "Impress BP 400DS": {"25MED": 0.11348768, "25PS": 0.9938036871, "26MDX": 0.0252528721, "26MED": 0.0570736874, "30MED": 0.0958176336, "30PS": 0.9060045108, "33MED": 0.165095595, "33PS": 0.8938300201, "35PS": 1.0112110328, "36MED": 0.1169487167, "40PS": 0.8604506465, "56LIN": 0.7934603527},
    "Starch": {"25MED": 76.6479248631, "25PS": 40.1454702823, "26MDX": 57.4121256714, "26MED": 39.5458622175, "30MED": 45.9181033395, "30PS": 47.7401878733, "33MED": 77.9596121508, "33PS": 97.0266559975, "35PS": 98.4681621189, "36MED": 92.6703605662, "40PS": 80.1534463455, "56LIN": 64.7336744016},
    "Zee 7635": {"25MED": 2.961646483, "25PS": 2.2236659451, "26MDX": 0.9907992617, "26MED": 1.3770559141, "30MED": 0.13799993, "30PS": 0.1928689425, "33MED": 0.2022778802, "33PS": 0.1143313824, "35PS": 0.0, "36MED": 0.0, "40PS": 0.0, "56LIN": 0.0}
}

# ==========================================
# 🛠️ FORECASTING HELPERS
# ==========================================

def load_and_parse_downtime(filepath, timezone_str):
    if not os.path.exists(filepath):
        return pd.DataFrame(columns=['Date', 'Duration', 'Description'])
    try:
        downtime_df = pd.read_csv(filepath)
        downtime_df.rename(columns={'Duration (Hours)': 'Duration', 'Reason': 'Description'}, inplace=True)
        required_cols = ['Date', 'Duration', 'Description']
        if not all(col in downtime_df.columns for col in required_cols):
            return pd.DataFrame(columns=['Date', 'Duration', 'Description'])

        if isinstance(timezone_str, str):
            tz = timezone(timedelta(hours=-5)) if 'Eastern' in timezone_str else timezone.utc 
        else:
            tz = timezone_str

        downtime_df['Date'] = pd.to_datetime(downtime_df['Date'], errors='coerce')
        try:
            downtime_df['Date'] = downtime_df['Date'].dt.tz_localize(tz).dt.normalize()
        except:
            downtime_df['Date'] = downtime_df['Date'].dt.normalize()

        downtime_df['Duration'] = pd.to_numeric(downtime_df['Duration'], errors='coerce')
        downtime_df.dropna(subset=['Date', 'Duration'], inplace=True)
        return downtime_df
    except Exception as e:
        print(f"Downtime error: {e}")
        return pd.DataFrame(columns=['Date', 'Duration', 'Description'])

def get_annual_shut_days(forecast_start_date, forecast_end_date, downtime_df):
    shut_days = set()
    if downtime_df.empty: return shut_days 

    annual_shut_df = downtime_df[
        downtime_df['Description'].str.contains("Annual Shut", case=False, na=False) &
        (downtime_df['Duration'] >= 23.9) 
    ]

    fs_ts = pd.Timestamp(forecast_start_date)
    fe_ts = pd.Timestamp(forecast_end_date)
    fs_norm = fs_ts.normalize()
    fe_norm = fe_ts.normalize()

    if fs_norm.tzinfo is None and not annual_shut_df.empty:
         annual_shut_df = annual_shut_df.copy()
         annual_shut_df['Date'] = annual_shut_df['Date'].dt.tz_localize(None)

    for shut_date in annual_shut_df['Date']:
        if shut_date.normalize() >= fs_norm and shut_date.normalize() <= fe_norm:
            shut_days.add(shut_date.normalize()) 

    return shut_days

def get_downtime_hours_for_day(day, downtime_df):
    if downtime_df.empty: return 0.0 
    
    day_ts = pd.Timestamp(day)
    day_normalized = day_ts.normalize()

    if day_normalized.tzinfo is None and pd.api.types.is_datetime64_any_dtype(downtime_df['Date']):
         if downtime_df['Date'].dt.tz is not None:
             downtime_df = downtime_df.copy()
             downtime_df['Date'] = downtime_df['Date'].dt.tz_localize(None)

    non_annual_dt = downtime_df[
        ~downtime_df['Description'].str.contains("Annual Shut", case=False, na=False) &
        (downtime_df['Duration'] < 23.9) 
    ]
    match = non_annual_dt[non_annual_dt['Date'] == day_normalized]
    if not match.empty:
        return min(match['Duration'].sum(), 24.0) 
    return 0.0 

def standardize_grade(grade_val):
    if pd.isna(grade_val): return ""
    raw_str = str(grade_val).strip().upper().replace(" ", "").replace("-", "")
    if raw_str in ['LEANSPREADS', 'HOLIDAY', 'SHUT', '', 'NAN', 'NONE', 'DATE']: return None
    return GLOBAL_GRADE_ALIAS_MAPPING.get(raw_str, raw_str)

def find_best_sequence_match(target_sequence, calendar_sequence):
    if not target_sequence or not calendar_sequence: return -1
    best_match_score = -1
    best_match_index = -1
    target_len = len(target_sequence)
    for i in range(len(calendar_sequence) - target_len + 1):
        sub_sequence = calendar_sequence[i : i + target_len]
        score = sum(1 for j in range(target_len) if target_sequence[j] == sub_sequence[j])
        if score > best_match_score:
            best_match_score = score
            best_match_index = i
            if best_match_score == target_len: break
    if best_match_score < (target_len / 2) and target_len > 1: return -1
    return best_match_index

def analyze_historical_patterns(start_date, end_date, mill_config):
    try:
        capsules = spy.pull(
            items=spy.search({'ID': mill_config['GRADE_RUN_CONDITION_ID']}, quiet=True),
            start=start_date, end=end_date, shape='capsules', capsule_properties=['Value'], quiet=True, tz_convert=mill_config['TIMEZONE']
        )
        if capsules.empty or 'Value' not in capsules.columns: return None, None
        
        capsules['Grade'] = capsules['Value'].apply(standardize_grade) # Updated
        capsules.dropna(subset=['Grade'], inplace=True)
        
        capsules['Duration'] = (capsules['Capsule End'] - capsules['Capsule Start']).dt.total_seconds() / 3600
        capsules['Tons'] = capsules.apply(lambda row: (row['Duration'] / 24) * mill_config['TONS_PER_DAY_BY_GRADE'].get(row['Grade'], 0), axis=1)
        
        avg_run_tonnage = capsules.groupby('Grade')['Tons'].mean().to_dict()
        
        capsules['Next Grade'] = capsules['Grade'].shift(-1)
        transitions = capsules.dropna(subset=['Next Grade'])
        transitions = transitions[transitions['Grade'] != transitions['Next Grade']]
        transition_matrix = pd.crosstab(transitions['Grade'], transitions['Next Grade'], normalize='index')
        
        return avg_run_tonnage, transition_matrix
    except:
        return None, None

# ----------------------------------------------------------------------
# 🧠 FORECAST ENGINE
# ----------------------------------------------------------------------
class ForecastEngine:
    def __init__(self, baselines_file='PM15_Calculated_Usage_Baselines.json', prices_file='mill_chemical_prices.json'):
        self.historical_metrics = pd.DataFrame()
        self.actual_usage = 0.0
        self.actual_tonnage = 0.0 
        self.current_grade_list = []
        self.baselines_data = {}
        self.chemical_prices = {}

        # 1. Initialize with Embedded Baselines (Primary Source)
        self.baselines_data['PM15'] = PM15_BASELINES

        # 2. Try to load additional/overwrite from file (Secondary)
        try:
            if os.path.exists(baselines_file):
                with open(baselines_file, 'r') as f: 
                    raw_data = json.load(f)
                    
                    first_key = next(iter(raw_data)) if raw_data else ''
                    
                    if first_key in ALL_MILL_CONFIGS:
                        # Original Structure: { "PM15": { "Starch": ... } }
                        for mill, chems in raw_data.items():
                            self.baselines_data[mill] = {}
                            for chem, grades in chems.items():
                                self.baselines_data[mill][chem] = {}
                                for grade, val in grades.items():
                                    norm_grade = standardize_grade(grade)
                                    if norm_grade:
                                        self.baselines_data[mill][chem][norm_grade] = val
                    else:
                        # New Structure (Flat): { "Starch": { ... } } -> Assign to PM15
                        mill = 'PM15'
                        # Use setdefault to preserve embedded if logic overlaps, 
                        # but typically file should update/extend embedded
                        if mill not in self.baselines_data: self.baselines_data[mill] = {}
                        
                        for chem, grades in raw_data.items():
                            self.baselines_data[mill][chem] = {}
                            for grade, val in grades.items():
                                norm_grade = standardize_grade(grade)
                                if norm_grade:
                                    self.baselines_data[mill][chem][norm_grade] = val
            
            if os.path.exists(prices_file):
                with open(prices_file, 'r') as f: self.chemical_prices = json.load(f)
        except: pass

    def get_price(self, mill, chemical):
        mill_prices = self.chemical_prices.get(mill, {})
        if chemical in mill_prices: return mill_prices[chemical]
        chem_lower = chemical.lower()
        for key, price in mill_prices.items():
            if key.lower() in chem_lower or chem_lower in key.lower(): return price
        return 0.0
        
    def get_group_budget_per_ton(self, mill, category):
        mill_budgets = GROUP_BUDGETS.get(mill, {})
        return mill_budgets.get(category, 0.0)
    
    def get_budget_per_ton(self, mill, chemical):
        mill_budgets = DUMMY_BUDGETS.get(mill, {})
        if chemical in mill_budgets: return mill_budgets[chemical]
        chem_lower = chemical.lower()
        for key, val in mill_budgets.items():
            if key.lower() == chem_lower: return val
        return 0.0 

    def load_historical_metrics(self, mill, chemical, use_cost=False):
        return pd.DataFrame()

    def calculate_actual_usage(self, mill, chemical, start_date, end_date, use_cost=False):
        items_to_pull = []
        if chemical == 'All' or chemical is None:
            for name, chem_id in CHEMICAL_SIGNAL_IDS.get(mill, {}).items():
                if chem_id: items_to_pull.append({'ID': chem_id, 'Name': name, 'Type': 'Signal'})
        else:
            chem_id = CHEMICAL_SIGNAL_IDS.get(mill, {}).get(chemical)
            if chem_id: items_to_pull.append({'ID': chem_id, 'Name': chemical, 'Type': 'Signal'})

        if not items_to_pull: return 0.0, {}
        
        try:
            df = spy.pull(items=pd.DataFrame(items_to_pull), start=start_date, end=end_date, grid='1h', header='Name', quiet=True)
            total_val = 0.0
            breakdown = {}
            
            for col in df.columns:
                col_sum_lbs = df[col].sum()
                price = self.get_price(mill, col) if use_cost else 1.0
                val = col_sum_lbs * price
                total_val += val
                breakdown[col] = val
                
            self.actual_usage = total_val
            return self.actual_usage, breakdown
        except: 
            self.actual_usage = 0.0
            return 0.0, {}

    def calculate_actual_usage_by_grade(self, mill, start_date, end_date):
        """
        Calculates the actual usage of all chemicals split by Grade.
        Returns a dictionary: { ChemicalName: { GradeName: TotalUsageLbs } }
        """
        if mill not in ALL_MILL_CONFIGS: return {}
        mill_config = ALL_MILL_CONFIGS[mill]
        cond_id = mill_config.get('GRADE_RUN_CONDITION_ID')
        chem_ids = CHEMICAL_SIGNAL_IDS.get(mill, {})
        
        if not cond_id or not chem_ids: return {}

        # 1. Pull Grade Capsules
        try:
            capsules_df = spy.pull(
                items=spy.search({'ID': cond_id}, quiet=True),
                start=start_date, end=end_date, shape='capsules', 
                capsule_properties=['Value'], quiet=True,
                tz_convert=mill_config.get('TIMEZONE')
            )
        except: return {}

        if capsules_df.empty: return {}
        
        # Standardize Grade Names in Capsules
        capsules_df['Grade'] = capsules_df['Value'].apply(standardize_grade)
        capsules_df.dropna(subset=['Grade'], inplace=True)

        # 2. Pull Chemical Data (Grid 1h to match existing logic assumption)
        items = []
        for name, cid in chem_ids.items():
            items.append({'ID': cid, 'Name': name})
        
        if not items: return {}

        try:
            chem_df = spy.pull(
                items=pd.DataFrame(items),
                start=start_date, end=end_date,
                grid='1h', header='Name', quiet=True,
                tz_convert=mill_config.get('TIMEZONE')
            )
        except: return {}

        if chem_df.empty: return {}

        # 3. Intersect Chemical Data with Grade Capsules
        usage_map = {chem: {} for chem in chem_ids.keys()}
        
        # Iterate capsules to slice chemical dataframe
        for _, cap in capsules_df.iterrows():
            cs = cap['Capsule Start']
            ce = cap['Capsule End']
            grade = cap['Grade']
            
            # Slice the chemical dataframe
            # Assumption: Index is sorted datetime
            slice_df = chem_df[cs:ce]
            
            if not slice_df.empty:
                for chem in chem_ids.keys():
                    if chem in slice_df.columns:
                        total = slice_df[chem].sum()
                        usage_map[chem][grade] = usage_map[chem].get(grade, 0.0) + total
                    
        return usage_map

    def calculate_actual_tonnage(self, mill, start_date, end_date):
        if mill not in ALL_MILL_CONFIGS: return 0.0, {}
        mill_config = ALL_MILL_CONFIGS[mill]
        
        try:
            cond_id = mill_config.get('GRADE_RUN_CONDITION_ID')
            if not cond_id: return 0.0, {}

            capsules = spy.pull(
                items=spy.search({'ID': cond_id}, quiet=True),
                start=start_date, end=end_date, shape='capsules', 
                capsule_properties=['Value'], quiet=True, 
                tz_convert=mill_config.get('TIMEZONE')
            )
            
            total_tons = 0.0
            grade_breakdown = {}
            
            if not capsules.empty:
                capsules['Grade'] = capsules['Value'].apply(standardize_grade)
                capsules = capsules.dropna(subset=['Grade'])
                
                sd = pd.Timestamp(start_date)
                ed = pd.Timestamp(end_date)
                
                for _, cap in capsules.iterrows():
                    cs = cap['Capsule Start']
                    ce = cap['Capsule End']
                    grade = cap['Grade']
                    
                    try:
                        valid_start = max(cs, sd)
                        valid_end = min(ce, ed)
                    except:
                        cs_n = cs.replace(tzinfo=None) if hasattr(cs, 'replace') else cs
                        ce_n = ce.replace(tzinfo=None) if hasattr(ce, 'replace') else ce
                        sd_n = sd.replace(tzinfo=None) if hasattr(sd, 'replace') else sd
                        ed_n = ed.replace(tzinfo=None) if hasattr(ed, 'replace') else ed
                        valid_start = max(cs_n, sd_n)
                        valid_end = min(ce_n, ed_n)

                    if valid_end > valid_start:
                        hours = (valid_end - valid_start).total_seconds() / 3600.0
                        tpd = mill_config['TONS_PER_DAY_BY_GRADE'].get(grade, 0)
                        tons = (hours / 24.0) * tpd
                        total_tons += tons
                        grade_breakdown[grade] = grade_breakdown.get(grade, 0.0) + tons
            
            self.actual_tonnage = total_tons
            # Modified to return Tuple (total, breakdown)
            return total_tons, grade_breakdown
        except Exception as e:
            self.actual_tonnage = 0.0
            return 0.0, {}

    def get_grade_tons_from_csv(self, mill, start_date, end_date):
        if mill not in ALL_MILL_CONFIGS: return {}
        csv_file = ALL_MILL_CONFIGS[mill].get('TONS_GRADE_FILE')
        if not csv_file or not os.path.exists(csv_file): return {}
        
        try:
            df = pd.read_csv(csv_file)
            df.columns = df.columns.str.strip()
            
            if 'Date' not in df.columns: return {}
            
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            df.dropna(subset=['Date'], inplace=True)
            
            sd_ts = pd.Timestamp(start_date).normalize().replace(tzinfo=None)
            ed_ts = pd.Timestamp(end_date).normalize().replace(tzinfo=None)
            
            mask = (df['Date'] >= sd_ts) & (df['Date'] < ed_ts) # Strict less than end date for actuals
            filtered_df = df.loc[mask]
            
            if filtered_df.empty: return {}
            
            grade_tons = {}
            for col in filtered_df.columns:
                if col.lower() == 'date': continue
                
                std_check = standardize_grade(col)
                if not std_check: continue
                
                raw_grade = col.strip().upper()
                
                total = pd.to_numeric(filtered_df[col], errors='coerce').fillna(0).sum()
                
                if total > 0:
                    grade_tons[raw_grade] = grade_tons.get(raw_grade, 0.0) + total
                    
            return grade_tons
        except Exception as e:
            print(f"CSV Grade Load Error: {e}")
            return {}

    def generate_tonnage_forecast(self, mill, start_date, end_date, known_actuals=None):
        if mill not in ALL_MILL_CONFIGS: return {}
        
        # Initialize with known actuals (merged Seeq + CSV)
        grade_tons_map = known_actuals.copy() if known_actuals else {}
        
        mill_config = ALL_MILL_CONFIGS[mill]
        mill_config['GRADE_ALIAS_MAPPING'] = GLOBAL_GRADE_ALIAS_MAPPING
        
        if not os.path.exists(mill_config['SCHEDULE_FILE']): return grade_tons_map
        
        try:
            schedule_df = pd.read_csv(mill_config['SCHEDULE_FILE'])
            schedule_df.rename(columns={'Grade': 'Grade', 'TO MAKE Ton': 'Tons'}, inplace=True)
            schedule_df['Grade'] = schedule_df['Grade'].apply(standardize_grade)
            schedule_df.dropna(subset=['Grade'], inplace=True)
            schedule_df['Tons'] = pd.to_numeric(schedule_df['Tons'], errors='coerce').fillna(0)
        except: return grade_tons_map

        # Logic to determine start of schedule simulation
        # If we have actuals, we assume they cover up to NOW.
        now_local = datetime.now(MILL_TZ).replace(tzinfo=None)
        
        if isinstance(start_date, date) and not isinstance(start_date, datetime):
            start_dt = datetime.combine(start_date, time(7,0))
        else:
            start_dt = start_date.replace(tzinfo=None) if hasattr(start_date, 'replace') else start_date

        if isinstance(end_date, date) and not isinstance(end_date, datetime):
            target_end = datetime.combine(end_date, time(7,0)) + timedelta(days=1)
        else:
            target_end = end_date.replace(tzinfo=None) if hasattr(end_date, 'replace') else end_date

        # If known_actuals is provided (not None), imply start from Now/Max
        if known_actuals is not None:
             current_day = max(start_dt, now_local)
        else:
             # Explicit override: Force simulation from start_date
             current_day = start_dt
            
        downtime_df = load_and_parse_downtime(mill_config['DOWNTIME_FILE'], mill_config.get('TIMEZONE', timezone.utc))
        future_annual_shut_days = get_annual_shut_days(current_day, target_end, downtime_df)
        initial_downtime = get_downtime_hours_for_day(current_day, downtime_df)
        hours_left_in_day = max(0, 24.0 - initial_downtime)

        run_schedule_grades = schedule_df['Grade'].tolist()
        last_grade = run_schedule_grades[-1] if run_schedule_grades else None

        for _, row in schedule_df.iterrows():
            if current_day > target_end: break
            grade = row['Grade']
            tons_to_make = row['Tons']
            tpd = mill_config['TONS_PER_DAY_BY_GRADE'].get(grade, 0)
            if tpd <= 0 or tons_to_make <= 0: continue
            
            hours_needed = (tons_to_make / tpd) * 24.0
            
            while hours_needed > 0.01 and current_day <= target_end:
                if hours_left_in_day < 0.01:
                    current_day += timedelta(days=1)
                    if current_day > target_end: break
                    while current_day in future_annual_shut_days and current_day <= target_end:
                        current_day += timedelta(days=1)
                    dt_hours = get_downtime_hours_for_day(current_day, downtime_df)
                    hours_left_in_day = max(0, 24.0 - dt_hours)
                    continue
                
                hours_to_run = min(hours_needed, hours_left_in_day)
                tons_produced = (hours_to_run / 24.0) * tpd
                grade_tons_map[grade] = grade_tons_map.get(grade, 0) + tons_produced
                hours_needed -= hours_to_run
                hours_left_in_day -= hours_to_run
            last_grade = grade

        if current_day <= target_end:
            try:
                cal_end = target_end + timedelta(days=45)
                cal_caps = spy.pull(
                    items=spy.search({'ID': mill_config['MILL_CALENDAR_CONDITION_ID']}, quiet=True),
                    start=current_day - timedelta(days=30),
                    end=cal_end,
                    shape='capsules',
                    capsule_properties=['Value'],
                    quiet=True,
                    tz_convert=mill_config.get('TIMEZONE')
                )
                if not cal_caps.empty:
                    cal_caps['CleanedGrade'] = cal_caps['Value'].apply(standardize_grade)
                    valid_caps = cal_caps.dropna(subset=['CleanedGrade']).sort_values(by='Capsule Start')
                    full_seq = valid_caps['CleanedGrade'].tolist()
                    match_seq = run_schedule_grades[-3:] if len(run_schedule_grades) >=3 else run_schedule_grades
                    start_idx = find_best_sequence_match(match_seq, full_seq)
                    
                    calendar_grades = []
                    if start_idx != -1:
                        calendar_grades = full_seq[start_idx + len(match_seq):]
                    else:
                        handoff = current_day + timedelta(hours=(24.0-hours_left_in_day))
                        if valid_caps['Capsule End'].dt.tz is not None and handoff.tzinfo is None:
                             handoff = handoff.replace(tzinfo=valid_caps['Capsule End'].dt.tz)
                        idx = valid_caps['Capsule End'].gt(handoff).idxmax()
                        calendar_grades = valid_caps.loc[idx:]['CleanedGrade'].tolist()
                        if calendar_grades and calendar_grades[0] == last_grade:
                            calendar_grades.pop(0)
                            
                    hist_avg, trans_mat = analyze_historical_patterns(
                        start_date - timedelta(days=365), 
                        start_date - timedelta(days=1), 
                        mill_config
                    )
                    
                    if hist_avg:
                        for grade in calendar_grades:
                            if current_day > target_end: break
                            tons_to_make = hist_avg.get(grade, 0)
                            tpd = mill_config['TONS_PER_DAY_BY_GRADE'].get(grade, 0)
                            if tpd <= 0 or tons_to_make <= 0: continue
                            hours_needed = (tons_to_make / tpd) * 24.0
                            
                            while hours_needed > 0.01 and current_day <= target_end:
                                if hours_left_in_day < 0.01:
                                    current_day += timedelta(days=1)
                                    if current_day > target_end: break
                                    while current_day in future_annual_shut_days and current_day <= target_end:
                                        current_day += timedelta(days=1)
                                    dt_hours = get_downtime_hours_for_day(current_day, downtime_df)
                                    hours_left_in_day = max(0, 24.0 - dt_hours)
                                    continue
                                hours_to_run = min(hours_needed, hours_left_in_day)
                                tons_produced = (hours_to_run / 24.0) * tpd
                                grade_tons_map[grade] = grade_tons_map.get(grade, 0) + tons_produced
                                hours_needed -= hours_to_run
                                hours_left_in_day -= hours_to_run
            except Exception as e: pass

        return grade_tons_map

engine = ForecastEngine()

# ==========================================
# 🎨 IPYVUETIFY UI COMPONENTS
# ==========================================

css_style = v.Html(tag='style', children=[
    """
    input[type='date']::-webkit-calendar-picker-indicator {
        cursor: pointer;
    }
    .hover-row:hover { background-color: #f5f5f5; }
    .combined-box-border { border: 2px solid #1e293b; }
    .grade-detail-table { width: 100%; border-collapse: collapse; }
    .grade-detail-table th, .grade-detail-table td { border-bottom: 1px solid #ddd; padding: 8px; text-align: left; }
    .centered-input input { text-align: center; }
    """
])

app_bar = v.AppBar(
    color='#1e293b', 
    dark=True, 
    flat=True,
    children=[
        v.ToolbarTitle(children=['Chemical End of Month Forecast']),
        v.Spacer(),
    ]
)

# --- 2. Config Card ---
mill_selector = v.Select(
    label='Select Mill',
    items=MILL_OPTIONS,
    v_model=MILL_OPTIONS[0],
    prepend_inner_icon='mdi-domain',
    outlined=True, dense=True, class_='pa-1'
)

today_str = date.today().strftime('%Y-%m-%d')
first_day_str = date.today().replace(day=1).strftime('%Y-%m-%d')

date_start = v.TextField(
    label='Start Date', type='date', v_model=first_day_str,
    outlined=True, dense=True, class_='pa-1'
)

date_end = v.TextField(
    label='End Date', type='date', v_model=today_str,
    outlined=True, dense=True, class_='pa-1'
)

btn_prev_month = v.Btn(small=True, color='blue darken-2', class_='white--text mr-2', children=[v.Icon(left=True, small=True, children=['mdi-chevron-left']), 'Last Month'])
btn_mtd = v.Btn(small=True, color='blue darken-2', children=['MTD'], class_='white--text mr-2', elevation=2)
btn_eom = v.Btn(small=True, color='blue darken-2', children=['EOM'], class_='white--text mr-2', elevation=2)
btn_next_month = v.Btn(small=True, color='blue darken-2', class_='white--text', children=['Next Month', v.Icon(right=True, small=True, children=['mdi-chevron-right'])])

switch_cost = v.Switch(label='Calculate Cost ($)', v_model=False, color='green', inset=True, class_='mt-0 pt-0')

btn_init_analysis = v.Btn(
    color='primary', block=True,
    children=[v.Icon(left=True, children=['mdi-refresh']), 'Initialize Baseline & Actuals'],
    class_='mt-2 mb-2'
)

config_card = v.Card(
    class_='mb-4 pa-4', elevation=4,
    children=[
        v.CardTitle(children=['Step 1: Configuration'], class_='text-h3 font-weight-black blue--text mb-4 justify-center'),
        v.Row(children=[v.Col(cols=12, md=12, children=[mill_selector])]),
        v.Row(children=[v.Col(cols=12, md=4, children=[date_start]), v.Col(cols=12, md=4, children=[date_end]), v.Col(cols=12, md=4, style_='display:flex; align-items:center;', children=[switch_cost])]),
        v.Row(justify='center', class_='mb-2', children=[v.Col(cols='auto', children=[btn_prev_month, btn_mtd, btn_eom, btn_next_month])]),
        v.Divider(class_='my-3'),
        btn_init_analysis
    ]
)

# --- 3. Combined Results Card ---

# Actuals Section (Top)
actual_display = v.Html(tag='div', class_='text-h4 blue--text text-center', children=['---'])
actual_cpt_display = v.Html(tag='div', class_='text-h4 blue--text text-center', children=['---']) # New widget
scope_display = v.Html(tag='div', class_='caption grey--text text-center', children=['Waiting for initialization...'])

results_container = v.Container(class_='pa-0', fluid=True, children=[])

# Store references to row widgets for calculation
forecast_rows_data = [] # Stores individual chemical rows
forecast_group_headers = {} # Stores group header widgets

# Footer Components (Defined before layout)
STATIC_BUDGET = 35.00

budget_label = v.Html(tag='div', class_='text-caption grey--text', children=['TOTAL BUDGET:'])
budget_value = v.Html(tag='div', class_='text-h5 font-weight-bold grey--text', children=[f"${STATIC_BUDGET:.2f} / Ton"])

cpt_display = v.Html(tag='div', class_='text-h5 font-weight-bold grey--text', children=['---'])
grand_total_display = v.Html(tag='div', class_='text-h4 success--text font-weight-bold', children=['---'])

# --- MODAL FOR GRADE BREAKDOWN ---
dialog_content = v.CardText(children=[])
dialog_actions = v.CardActions(children=[]) # New: Separate Actions container
close_btn = v.Btn(icon=True, children=[v.Icon(children=['mdi-close'])])

dialog = v.Dialog(
    width='1200', # Increased width for new columns
    children=[
        v.Card(children=[
            v.CardTitle(class_='headline grey lighten-2', children=[
                'Grade Cost Per Ton Breakdown', v.Spacer(), 
                close_btn
            ]),
            v.CardTitle(class_='subtitle-2', children=[v.Html(tag='div', children=['Note: "Rate" shown below is weighted average of Actuals and Forecast. "Budget" is from fixed baselines.'])]),
            dialog_content,
            dialog_actions # Add actions here
        ])
    ]
)

def close_dialog(*args):
    dialog.v_model = False

close_btn.on_event('click', close_dialog)

breakdown_title_display = v.Html(tag='div', class_='subtitle-2 mb-2 black--text', children=['USAGE BREAKDOWN'])

# Reset Button
btn_reset_charts = v.Btn(
    small=True, 
    color='red lighten-2', 
    class_='white--text', 
    children=[v.Icon(left=True, small=True, children=['mdi-restore']), 'Reset Values']
)

def on_reset_click(*args):
    # Reset individual rows
    for row in forecast_rows_data:
        if 'reset_fn' in row:
            row['reset_fn']()
            
    # Reset Group Header Budgets
    mill = mill_selector.v_model
    use_cost = switch_cost.v_model
    
    for group in CATEGORY_ORDER:
        if group in forecast_group_headers:
            header_widgets = forecast_group_headers[group]
            
            # Retrieve original default budget
            default_budget = engine.get_group_budget_per_ton(mill, group)
            if not use_cost: default_budget = 0.0
            
            # Reset widget value
            header_widgets['w_budget'].v_model = float(f"{default_budget:.2f}")

    recalculate_totals()

btn_reset_charts.on_event('click', on_reset_click)

combined_card = v.Card(
    class_='mt-4 pa-4 combined-box-border', elevation=4,
    children=[
        dialog, # Include dialog component
        v.CardTitle(class_='subtitle-1 black--text font-weight-bold', children=[
            'Step 2: Usage & Forecast Report', 
            v.Spacer(), 
            btn_reset_charts
        ]),
        
        # BOTTOM SECTION: FORECAST
        breakdown_title_display,
        results_container,
        
        v.Divider(class_='my-4'),
        
        # TOTAL PROJECTED (Row 1)
        v.Row(
            align='center', justify='end',
            children=[
                v.Col(cols='auto', children=[v.Html(tag='div', class_='text-h6 grey--text mr-2', children=['TOTAL PROJECTED:'])]),
                v.Col(cols='auto', children=[grand_total_display])
            ]
        ),

        # METRICS (Row 2)
        v.Row(
            align='center', justify='end', class_='mt-0 pt-0',
            children=[
                v.Col(cols='auto', children=[budget_label]),
                v.Col(cols='auto', children=[budget_value]),
                v.Col(cols='auto', class_='ml-6', children=[v.Html(tag='div', class_='text-caption grey--text', children=['ACTUAL METRIC/TON:'])]),
                v.Col(cols='auto', children=[cpt_display]),
            ]
        )
    ]
)

# ----------------------------------------------------------------------
# ⚡ LOGIC & INTERACTIVITY
# ----------------------------------------------------------------------

# NOTE: on_mill_change for chemicals not needed as we show all chemicals now

def on_mtd_click(widget, event, data):
    now_mill_tz = datetime.now(MILL_TZ)
    if now_mill_tz.day == 1 and now_mill_tz.hour < MILL_START_HOUR:
        prev = now_mill_tz - timedelta(days=1)
        start_d = date(prev.year, prev.month, 1)
    else:
        start_d = date(now_mill_tz.year, now_mill_tz.month, 1)
    date_start.v_model = start_d.strftime('%Y-%m-%d')
    date_end.v_model = now_mill_tz.strftime('%Y-%m-%d')

btn_mtd.on_event('click', on_mtd_click)

def on_eom_click(widget, event, data):
    now_mill_tz = datetime.now(MILL_TZ)
    start_d = date(now_mill_tz.year, now_mill_tz.month, 1)
    next_month = now_mill_tz.replace(day=28) + timedelta(days=4)
    end_d = next_month - timedelta(days=next_month.day)
    
    date_start.v_model = start_d.strftime('%Y-%m-%d')
    date_end.v_model = end_d.strftime('%Y-%m-%d')

btn_eom.on_event('click', on_eom_click)

def on_prev_month_click(widget, event, data):
    try:
        curr_start = datetime.strptime(date_start.v_model, '%Y-%m-%d').date()
        first_curr = curr_start.replace(day=1)
        last_prev = first_curr - timedelta(days=1)
        first_prev = last_prev.replace(day=1)
        date_start.v_model = first_prev.strftime('%Y-%m-%d')
        date_end.v_model = last_prev.strftime('%Y-%m-%d')
    except: pass

btn_prev_month.on_event('click', on_prev_month_click)

def on_next_month_click(widget, event, data):
    try:
        curr_start = datetime.strptime(date_start.v_model, '%Y-%m-%d').date()
        first_next = (curr_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        next_plus_one = (first_next.replace(day=28) + timedelta(days=4)).replace(day=1)
        last_next = next_plus_one - timedelta(days=1)
        date_start.v_model = first_next.strftime('%Y-%m-%d')
        date_end.v_model = last_next.strftime('%Y-%m-%d')
    except: pass

# --- RECALCULATION LOGIC ---
def recalculate_totals(*args):
    total_forecast_cost = 0.0
    total_budget_sum = 0.0
    use_cost = switch_cost.v_model
    mill = mill_selector.v_model  # Get current mill
    
    # Track totals per group to update headers
    group_totals = {cat: 0.0 for cat in CATEGORY_ORDER}
    
    for row in forecast_rows_data:
        try:
            # Check row state for updated values
            row_state = row.get('state', {})
            val = row_state.get('numeric_total', 0.0)
            group = row_state.get('group', 'Other')
            
            total_forecast_cost += val
            
            # Accumulate for group
            if group in group_totals:
                group_totals[group] += val
            else:
                group_totals.setdefault(group, 0.0)
                group_totals[group] += val
                
        except: pass
    
    # Update Group Headers Variance AND Row Percentages
    # We need the current production tons to convert total cost back to $/Ton for comparison
    mode = getattr(engine, 'last_mode', 'Forecast')
    forecast_prod_tons = getattr(engine, 'last_forecast_tons', 0.0)
    
    if mode == 'Actuals':
        total_period_tons = engine.actual_tonnage
    else:
        # forecast_prod_tons already includes Actuals + Forecast, so we use it directly
        total_period_tons = forecast_prod_tons
    
    # 1. Update individual row percentages based on group totals
    for row in forecast_rows_data:
        try:
            row_state = row.get('state', {})
            val = row_state.get('numeric_total', 0.0)
            group = row_state.get('group', 'Other')
            w_pct_widget = row.get('w_group_pct')
            show_pct = row_state.get('show_group_pct', True) # Check flag
            
            if w_pct_widget:
                if not show_pct:
                    w_pct_widget.children = [""] # Hide if only one item in group
                else:
                    g_total = group_totals.get(group, 0.0)
                    if g_total > 0:
                        pct_val = (val / g_total) * 100
                        w_pct_widget.children = [f"{pct_val:.1f}%"]
                    else:
                        w_pct_widget.children = ["0.0%"]
        except: pass

    # 2. Update Group Headers
    for group in CATEGORY_ORDER: # Iterate through all groups
        if group in forecast_group_headers:
            header_widgets = forecast_group_headers[group]
            
            # Retrieve currently set budget from the widget (allows manual override)
            try:
                budget_val = float(header_widgets['w_budget'].v_model)
            except:
                budget_val = 0.0
            
            # Update stored value
            header_widgets['budget_val'] = budget_val
            
            # Calculate Actual/Forecast Rate ($/Ton) for the group
            total_val = group_totals.get(group, 0.0)
            current_rate = 0.0
            if total_period_tons > 0:
                current_rate = total_val / total_period_tons
                
            # Update Rate Display (Update the model to reflect calculation)
            # Only update if the difference is significant to avoid fighting user input cursor
            try:
                current_display = float(header_widgets['w_rate'].v_model)
            except: current_display = -1.0
            
            if abs(current_display - current_rate) > 0.001:
                header_widgets['w_rate'].v_model = float(f"{current_rate:.2f}")
            
            # Update Variance
            variance = current_rate - budget_val
            pct_variance = (variance / budget_val * 100) if budget_val > 0 else 0.0
            
            lbl_unit = "$" if use_cost else "Lbs"
            var_color = "green--text" if variance <= 0 else "red--text"
            fmt_var = f"{lbl_unit}{variance:,.2f}"
            if variance > 0: fmt_var = f"+{fmt_var}"
            
            pct_color = "green--text" if pct_variance <= 0 else "red--text"
            fmt_pct = f"{pct_variance:+.1f}%"
            
            header_widgets['w_var'].class_ = f'text-center {var_color}'
            header_widgets['w_var'].children = [fmt_var]
            
            header_widgets['w_pct'].class_ = f'text-center {pct_color}'
            header_widgets['w_pct'].children = [fmt_pct]
            
            # Add to total budget sum
            if use_cost and (total_val > 0 or budget_val > 0):
                 total_budget_sum += budget_val

    # Grand Totals
    if mode == 'Actuals':
        grand_total = total_forecast_cost
    else:
        grand_total = total_forecast_cost 
        
    gt_fmt = f"${grand_total:,.2f}" if use_cost else f"{grand_total:,.2f} Lbs"
    grand_total_display.children = [gt_fmt]

    if total_period_tons > 0:
        cpt = grand_total / total_period_tons
    else:
        cpt = 0.0
        
    cpt_fmt = f"${cpt:,.2f}" if use_cost else f"{cpt:,.2f}"
    
    # Update Total Budget Display (Sum of group budgets)
    budget = total_budget_sum
    budget_fmt = f"${budget:,.2f} / Ton" if use_cost else f"{budget:,.2f} / Ton"
    budget_value.children = [budget_fmt]
    
    color_class = "green--text" if cpt <= budget else "red--text"
    cpt_display.children = [v.Html(tag='span', class_=f"text-h5 font-weight-bold {color_class}", children=[f"{cpt_fmt} / Ton"])]

def show_breakdown_modal(widget, event, data):
    try:
        chem_name = data.get('chem_name')
        breakdown_data = data.get('breakdown', [])
        context_label = data.get('context_label', 'Theoretical')
        on_save_callback = data.get('on_save') # Retrieve callback

        use_cost = switch_cost.v_model
        
        # Dynamic Headers based on mode
        unit_rate = "$/T" if use_cost else "Lbs/T"
        
        dialog_content.children = [] # Clear content
        dialog_actions.children = [] # Clear actions
        
        # List to hold references to widgets for calculation on save
        grade_inputs = []
        
        # Header Row
        header = v.Row(
            class_='font-weight-bold grey lighten-3 pa-2 mb-2',
            children=[
                v.Col(cols=4, children=[v.Html(tag='div', children=['Grade'])]), 
                v.Col(cols=2, class_='text-right', children=[v.Html(tag='div', children=[f'Rate ({unit_rate})'])]),
                v.Col(cols=2, class_='text-right', children=[v.Html(tag='div', children=[f'Budget ({unit_rate})'])]),
                v.Col(cols=2, class_='text-right', children=[v.Html(tag='div', children=['Var'])]),
                v.Col(cols=2, class_='text-right', children=[v.Html(tag='div', children=['% Var'])]),
            ]
        )
        
        rows_list = [
            v.Html(tag='div', class_='subtitle-1 mb-2', children=[f'Breakdown for {chem_name} ({context_label})']),
            header
        ]

        # Footer Display Widget - Shows Sum of Costs (Budget is implicit in row variances)
        w_footer_sum_label = v.Html(tag='div', class_='text-right font-weight-bold', children=['TOTAL COST:'])
        w_footer_sum_value = v.Html(tag='div', class_='text-right font-weight-bold', children=['---'])

        # Function to update the footer sum (Logic kept but not displayed in footer row)
        def update_footer_sum():
            total_dollars = 0.0
            for g_item in grade_inputs:
                try:
                    # Calculate current total from inputs
                    r = float(g_item['widget_rate'].v_model)
                    t = g_item['tons']
                    total_dollars += (r * t)
                except: pass
            
            fmt_sum = f"${total_dollars:,.2f}" if use_cost else f"{total_dollars:,.2f}"
            w_footer_sum_value.children = [fmt_sum]

        for item in breakdown_data:
            grade = item['grade']
            
            # MODIFIED LOGIC:
            # We assume 'rate' is the Actual/Calculated Rate passed in.
            # We assume 'budget_rate' is the baseline/budget rate passed in.
            
            initial_rate = item.get('rate', 0.0) # This is the Actual Rate
            grade_budget_rate = item.get('budget_rate', initial_rate) # Budget rate from baselines
            
            grade_tons = item.get('tons', 0.0)
            
            # Widgets for row
            w_grade = v.Html(tag='div', class_='pt-2', children=[str(grade)])
            
            # Budget Display (Static, from Baseline)
            fmt_budget = f"${grade_budget_rate:,.2f}" if use_cost else f"{grade_budget_rate:,.2f}"
            w_budget_display = v.Html(tag='div', class_='pt-2 text-right grey--text', children=[fmt_budget])

            # Variance Displays
            w_var = v.Html(tag='div', class_='pt-2 text-right', children=['---'])
            w_pct = v.Html(tag='div', class_='pt-2 text-right', children=['---'])

            # Helper to safely get float
            def safe_float(val):
                try:
                    return float(f"{val:.4f}")
                except:
                    return 0.0

            # Editable Rate Input - Pre-populated with ACTUAL Rate
            w_rate_input = v.TextField(
                v_model=safe_float(initial_rate),
                type='number',
                step=0.01,
                dense=True,
                hide_details=True,
                solo=True, flat=True,
                class_='ma-0 pa-0 centered-input',
                style_='text-align: right; max-width: 100%;'
            )
            
            # Store for save calculation
            grade_inputs.append({
                'grade': grade, 
                'widget_rate': w_rate_input,
                'tons': grade_tons,
                'w_var': w_var,
                'w_pct': w_pct,
                'budget_rate': grade_budget_rate,
                'rate': initial_rate,
            })
            
            # Logic to update line calculations
            def update_row_calculations(widget, event, data, 
                                        w_r=w_rate_input, 
                                        w_v=w_var, w_p=w_pct,
                                        b_rate=grade_budget_rate):
                try:
                    current_rate = float(w_r.v_model)
                    
                    # Update Variance (Actual vs Budget)
                    variance = current_rate - b_rate
                    pct_variance = (variance / b_rate * 100) if b_rate > 0 else 0.0
                    
                    var_color = "green--text" if variance <= 0 else "red--text"
                    fmt_var = f"${variance:,.2f}" if use_cost else f"{variance:,.2f}"
                    if variance > 0: fmt_var = f"+{fmt_var}"
                    
                    pct_color = "green--text" if pct_variance <= 0 else "red--text"
                    fmt_pct = f"{pct_variance:+.1f}%"
                    
                    w_v.class_ = f'pt-2 text-right {var_color}'
                    w_v.children = [fmt_var]
                    
                    w_p.class_ = f'pt-2 text-right {pct_color}'
                    w_p.children = [fmt_pct]

                    # Trigger footer update
                    update_footer_sum()

                except: pass
                
            # Bind change events
            w_rate_input.on_event('change', update_row_calculations)
            
            # Initialize variance
            update_row_calculations(None, None, None)
            
            row_ui = v.Row(
                class_='mb-1 hover-row pa-1 border-bottom',
                align='center',
                children=[
                    v.Col(cols=4, children=[w_grade]),
                    v.Col(cols=2, class_='d-flex justify-end', children=[w_rate_input]),
                    v.Col(cols=2, class_='d-flex justify-end', children=[w_budget_display]),
                    v.Col(cols=2, class_='d-flex justify-end', children=[w_var]),
                    v.Col(cols=2, class_='d-flex justify-end', children=[w_pct]),
                ]
            )
            rows_list.append(row_ui)

        # Append Footer Row
        footer_row = v.Row(
            class_='grey lighten-4 pa-2 mt-2',
            align='center',
            children=[
                v.Col(cols=7, children=[]),
                v.Col(cols=2, class_='text-right', children=[w_footer_sum_label]),
                v.Col(cols=3, class_='text-right', children=[w_footer_sum_value])
            ]
        )
        rows_list.append(footer_row)
        
        # Initial Footer Calculation
        update_footer_sum()

        # Add Rows to Content
        dialog_content.children = rows_list
        
        # Create Save Action
        def on_save_click(*args):
            # Calculate new totals for the chemical
            new_total_metric = 0.0
            new_total_budget_metric = 0.0 
            updated_details = [] 
            
            for g_data in grade_inputs:
                try:
                    r = float(g_data['widget_rate'].v_model)
                except:
                    r = g_data.get('rate', 0.0)
                    
                t = g_data['tons']
                new_total_metric += (r * t)
                
                # Persist updated values
                updated_details.append({
                    'grade': g_data['grade'],
                    'rate': r,
                    'budget_rate': g_data['budget_rate'],
                    'tons': t,
                    'cost': r * t
                })
                
            # Call Parent Callback
            if on_save_callback:
                on_save_callback(new_total_metric, new_total_budget_metric, updated_details)
                
            dialog.v_model = False 

        btn_save = v.Btn(color='primary', children=['Save Changes'], class_='white--text')
        btn_save.on_event('click', on_save_click)
        
        dialog_actions.children = [v.Spacer(), btn_save]
        
        dialog.v_model = True
    except Exception as e:
        print(f"Error in show_breakdown_modal: {e}")
        traceback.print_exc()

def create_chemical_row(chem, total_qty, details, use_cost, group_name, tons_basis=0.0, context_label="Theoretical", show_group_pct=True, row_budget=0.0):
    # Widgets
    w_chem = v.Html(tag='div', class_='pt-2 pl-4', children=[str(chem)]) # Indented
    
    # Price Per Ton Calculation
    val_per_ton = 0.0
    if tons_basis > 0:
        val_per_ton = total_qty / tons_basis
        
    # Store initial state for reset functionality
    initial_val_per_ton = val_per_ton
    initial_details = copy.deepcopy(details)

    # State object to track current total
    row_state = {
        'numeric_total': total_qty, 
        'current_rate': val_per_ton, 
        'details': details, # Store details in state for persistence
        'group': group_name,
        'show_group_pct': show_group_pct, # Store flag
        'row_budget': row_budget # Store specific budget
    }
        
    # Editable Input for Main Table (Rate)
    w_per_ton = v.TextField(
        v_model=float(f"{val_per_ton:.2f}"),
        type='number',
        step=0.01,
        dense=True,
        hide_details=True,
        solo=True, flat=True,
        class_='pt-0 mt-0 centered-input',
        style_='width: 90%; min-width: 60px;'
    )
    
    # New: % of Group Widget
    initial_pct_text = '0%' if show_group_pct else ''
    w_group_pct = v.Html(tag='div', class_='text-center grey--text body-2 pt-2', children=[initial_pct_text])
    
    # Budget Input (Editable)
    w_budget_input = v.TextField(
        v_model=float(f"{row_budget:.2f}"),
        type='number',
        step=0.01,
        dense=True,
        hide_details=True,
        solo=True, flat=True,
        class_='pt-0 mt-0 centered-input grey--text',
        style_='width: 90%; min-width: 60px;'
    )

    # Variance Widgets (Active if budget exists)
    w_var_placeholder = v.Html(tag='div', class_='text-center grey--text lighten-2', children=['-'])
    w_pct_placeholder = v.Html(tag='div', class_='text-center grey--text lighten-2', children=['-'])

    # Reusable logic to update row from any source (Manual input or Details Save)
    def update_row_logic(new_rate):
        row_state['current_rate'] = new_rate
        
        # 1. Update State Total
        new_total = new_rate * tons_basis
        row_state['numeric_total'] = new_total
        
        # 2. Update UI Model
        w_per_ton.v_model = float(f"{new_rate:.2f}")
        
        # 3. Update Variance (Using current budget from input)
        try:
            current_budget = float(w_budget_input.v_model)
        except: current_budget = 0.0
        
        if current_budget > 0 and use_cost:
            variance = new_rate - current_budget
            pct_variance = (variance / current_budget * 100)
            
            var_color = "green--text" if variance <= 0 else "red--text"
            fmt_var = f"${variance:,.2f}"
            if variance > 0: fmt_var = f"+{fmt_var}"
            
            pct_color = "green--text" if pct_variance <= 0 else "red--text"
            fmt_pct = f"{pct_variance:+.1f}%"
            
            w_var_placeholder.class_ = f'text-center {var_color} pt-2'
            w_var_placeholder.children = [fmt_var]
            
            w_pct_placeholder.class_ = f'text-center {pct_color} pt-2'
            w_pct_placeholder.children = [fmt_pct]
        else:
            w_var_placeholder.children = ['-']
            w_pct_placeholder.children = ['-']

        # 4. Recalculate Grand Total & Group Header
        recalculate_totals()

    # Function to Reset Row to Initial Values
    def reset_row_values():
        # Restore values
        row_state['current_rate'] = initial_val_per_ton
        row_state['details'] = copy.deepcopy(initial_details)
        row_state['numeric_total'] = initial_val_per_ton * tons_basis
        
        # Update Budget Widget first
        w_budget_input.v_model = float(f"{row_state['row_budget']:.2f}")
        
        # Trigger update logic to refresh Rate widget, Variance, and Totals
        update_row_logic(initial_val_per_ton)
        
    # Handler for Main Input Change (Rate)
    def on_main_rate_change(widget, event, data):
        try:
            new_rate = float(widget.v_model)
            update_row_logic(new_rate)
        except: return

    # Handler for Budget Input Change
    def on_budget_change(widget, event, data):
        # Just trigger update logic to refresh variance
        try:
            current_rate = float(w_per_ton.v_model)
            update_row_logic(current_rate)
        except: return

    w_per_ton.on_event('change', on_main_rate_change)
    w_budget_input.on_event('change', on_budget_change)
    
    # Initialize variance
    update_row_logic(val_per_ton)
    
    # Callback for Details Modal Save
    def on_details_save(new_total_metric, new_total_budget_metric, updated_details):
        # Update stored details with new values from modal
        row_state['details'] = updated_details
        
        # Calculate new average rate
        if tons_basis > 0:
            new_avg_rate = new_total_metric / tons_basis
            update_row_logic(new_avg_rate)
    
    # Details Button
    btn_details = v.Btn(icon=True, small=True, color='blue', children=[v.Icon(children=['mdi-format-list-bulleted'])])
    
    # Named function for click handler to avoid closure issues
    def on_details_click(widget, event, data):
        data_packet = {
            'chem_name': chem, 
            'breakdown': row_state['details'], 
            'context_label': context_label,
            'chem_budget': 0.0, # No individual budget
            'on_save': on_details_save 
        }
        show_breakdown_modal(widget, event, data_packet)

    btn_details.on_event('click', on_details_click)
    
    # Layout - Updated for 12 column grid with new fields (Added % Grp col)
    row_layout = v.Row(
        align='center', class_='mb-1 hover-row pa-1 flex-nowrap', 
        children=[
            v.Col(cols=3, children=[w_chem]),
            v.Col(cols=2, class_='d-flex justify-center', children=[w_per_ton]),
            v.Col(cols=1, class_='d-flex justify-center', children=[w_group_pct]), # New Column
            v.Col(cols=2, class_='d-flex justify-center', children=[w_budget_input]), # Now Editable
            v.Col(cols=2, class_='d-flex justify-center', children=[w_var_placeholder]),
            v.Col(cols=1, class_='d-flex justify-center', children=[w_pct_placeholder]), 
            v.Col(cols=1, class_='d-flex justify-center', children=[btn_details]), # Reduced to 1 col
        ]
    )
    
    return {
        'layout': row_layout,
        'state': row_state, 
        'reset_fn': reset_row_values,
        'w_group_pct': w_group_pct,
        'w_per_ton': w_per_ton # Return ref to rate widget for external updates
    }

def create_group_header_row(category, budget_per_ton, use_cost):
    lbl_unit = "$" if use_cost else "Lbs"
    
    w_title = v.Html(tag='div', class_='text-uppercase font-weight-black grey--text text--darken-2 pl-2', children=[category])
    
    # Editable Group Rate
    w_rate = v.TextField(
        v_model=0.0, # Will be set by recalculate_totals
        type='number', step=0.01, dense=True, hide_details=True, solo=True, flat=True,
        class_='pt-0 mt-0 centered-input font-weight-bold',
        style_='width: 90%; min-width: 60px;'
    )
    
    # Editable Group Budget
    w_budget = v.TextField(
        v_model=float(f"{budget_per_ton:.2f}"),
        type='number', step=0.01, dense=True, hide_details=True, solo=True, flat=True,
        class_='pt-0 mt-0 centered-input font-weight-bold grey--text',
        style_='width: 90%; min-width: 60px;'
    )
    
    w_var = v.Html(tag='div', class_='text-center', children=['---'])
    w_pct = v.Html(tag='div', class_='text-center', children=['---'])
    
    row = v.Row(
        class_='grey lighten-4 mt-2 mb-1 pa-2 font-weight-bold',
        align='center',
        children=[
            v.Col(cols=3, children=[w_title]),
            v.Col(cols=2, class_='d-flex justify-center', children=[w_rate]),
            v.Col(cols=1, children=[]), # Spacer for % Grp
            v.Col(cols=2, class_='d-flex justify-center', children=[w_budget]),
            v.Col(cols=2, children=[w_var]),
            v.Col(cols=1, children=[w_pct]),
            v.Col(cols=1, children=[]), # Spacer for details btn col
        ]
    )
    
    return row, {
        'w_rate': w_rate,
        'w_var': w_var,
        'w_pct': w_pct,
        'w_budget': w_budget, 
        'budget_val': budget_per_ton
    }

def get_chemical_category(chem_name):
    # Try exact match first
    if chem_name in CHEMICAL_CATEGORY_MAP:
        return CHEMICAL_CATEGORY_MAP[chem_name]
    
    # Try substring match
    for key, category in CHEMICAL_CATEGORY_MAP.items():
        if key.lower() in chem_name.lower():
            return category
            
    return DEFAULT_CATEGORY

# --- MAIN LOGIC FUNCTION ---
def on_load_baseline(widget, event, data):
    btn_init_analysis.loading = True
    results_container.children = [] # Clear table
    
    try:
        mill = mill_selector.v_model
        use_cost = switch_cost.v_model
        
        d_start = datetime.strptime(date_start.v_model, '%Y-%m-%d').date()
        d_end = datetime.strptime(date_end.v_model, '%Y-%m-%d').date()
        
        now_mill_tz = datetime.now(MILL_TZ)
        today_date = now_mill_tz.date()

        # -------------------------------------------------------------
        # SPLIT TIME RANGE: ACTUALS vs FORECAST
        # Actuals: From Start Date up to (End Date OR Yesterday, whichever is earlier)
        # Forecast: From Today (07:00) onwards, if End Date >= Today
        # -------------------------------------------------------------

        # Actuals Start Time
        dt_start_actuals = datetime.combine(d_start, time(MILL_START_HOUR, 0), tzinfo=MILL_TZ)
        
        # Determine Cutoff for Actuals vs Forecast
        if d_end < today_date:
            # Selected range is entirely in the past
            # Actuals run until end of selection
            dt_end_actuals = datetime.combine(d_end + timedelta(days=1), time(MILL_START_HOUR, 0), tzinfo=MILL_TZ)
            dt_start_forecast = None
            dt_end_forecast = None
        else:
            # Selected range includes Today or Future
            # Actuals stop at Today 07:00 (Start of current shift)
            dt_end_actuals = datetime.combine(today_date, time(MILL_START_HOUR, 0), tzinfo=MILL_TZ)
            # Forecast starts at Today 07:00
            dt_start_forecast = datetime.combine(today_date, time(MILL_START_HOUR, 0), tzinfo=MILL_TZ)
            dt_end_forecast = datetime.combine(d_end + timedelta(days=1), time(MILL_START_HOUR, 0), tzinfo=MILL_TZ)

        # -------------------------------------------------------------
        # 1. GET ACTUALS (From Seeq + CSV)
        # -------------------------------------------------------------
        actual_tons_map = {}
        actual_usage_map = {} # {Chem: {Grade: Lbs}}
        actual_tons_total = 0.0
        
        if dt_end_actuals > dt_start_actuals:
            # Get Tonnage from CSV (Actual Tons)
            actual_tons_map = engine.get_grade_tons_from_csv(mill, dt_start_actuals, dt_end_actuals)
            actual_tons_total = sum(actual_tons_map.values())
            
            # Get Chemical Usage from Seeq (Actual Usage)
            actual_usage_map = engine.calculate_actual_usage_by_grade(mill, dt_start_actuals, dt_end_actuals)

        # -------------------------------------------------------------
        # 2. GET FORECAST (From Schedule Simulation)
        # -------------------------------------------------------------
        forecast_tons_map = {}
        forecast_tons_total = 0.0
        
        if dt_start_forecast and dt_end_forecast > dt_start_forecast:
             # Pass known_actuals={} to force simulation from start date (Today)
             forecast_tons_map = engine.generate_tonnage_forecast(mill, dt_start_forecast, dt_end_forecast, known_actuals={})
             forecast_tons_total = sum(forecast_tons_map.values())

        # Combined Total Tons for Rate Calculation
        total_production_tons = actual_tons_total + forecast_tons_total
        engine.last_forecast_tons = total_production_tons

        # Determine Mode for Display Label
        is_forecast_mode = (forecast_tons_total > 0)
        engine.last_mode = 'Forecast' if is_forecast_mode else 'Actuals'
        
        # -------------------------------------------------------------
        # 3. MERGE & CALCULATE WEIGHTED RATES
        # -------------------------------------------------------------
        
        # Load Baselines (Budget Rates) for Forecast Calculations
        mill_baselines = engine.baselines_data.get(mill, {})
        mill_chemicals = list(CHEMICAL_SIGNAL_IDS.get(mill, {}).keys())

        # Structure: { Category: [ {name: Chem, data: {total: X, details: [...]}} ] }
        all_chem_data = {cat: [] for cat in CATEGORY_ORDER}

        for chem_name in mill_chemicals:
            chem_key = next((k for k in mill_baselines.keys() if chem_name.lower() in k.lower() or k.lower() in chem_name.lower()), None)
            
            chem_total_metric = 0.0 # Total Cost or Lbs
            chem_total_budget = 0.0 # Total Budget (Cost or Lbs)
            chem_total_tons = 0.0   # Total Tons
            chem_details = []
            
            # Identify all unique grades involved for this chemical (Actuals U Forecast)
            # Actual grades for this chem are keys in actual_usage_map[chem_name] OR keys in actual_tons_map
            # Forecast grades are keys in forecast_tons_map
            
            grades_in_actuals = set()
            if chem_name in actual_usage_map:
                grades_in_actuals.update(actual_usage_map[chem_name].keys())
            grades_in_actuals.update(actual_tons_map.keys())
            
            all_grades = grades_in_actuals.union(set(forecast_tons_map.keys()))
            
            price = engine.get_price(mill, chem_name) if use_cost else 1.0

            for grade in all_grades:
                std_grade = standardize_grade(grade)
                if not std_grade: continue
                
                # --- ACTUALS COMPONENT ---
                act_tons = actual_tons_map.get(std_grade, 0.0)
                # Usage from Seeq (Lbs)
                act_usage_lbs = actual_usage_map.get(chem_name, {}).get(std_grade, 0.0)
                
                # If we have tons but no usage recorded, or usage but no tons, data might be messy.
                # Rate = Usage / Tons
                
                # --- FORECAST COMPONENT ---
                fcst_tons = forecast_tons_map.get(std_grade, 0.0)
                
                # Get Baseline Rate (Budget)
                baseline_rate = 0.0
                if chem_key: baseline_rate = mill_baselines[chem_key].get(std_grade, 0.0)
                
                # Calculate Forecast Usage based on Budget
                # If use_cost is True, baseline_rate is Lbs/Ton? 
                # Usually Baselines are in Usage/Ton (Lbs/Ton). 
                # DUMMY_BUDGETS variable has $/Ton, but baselines_file usually has Usage.
                # Assuming baselines_data is Usage/Ton (Lbs/Ton) based on logic structure.
                
                fcst_usage_lbs = fcst_tons * baseline_rate
                
                # --- COMBINE ---
                total_grade_tons = act_tons + fcst_tons
                total_grade_usage_lbs = act_usage_lbs + fcst_usage_lbs
                
                if total_grade_tons <= 0 and total_grade_usage_lbs <= 0: continue

                # Calculate Composite Rate & Cost
                if use_cost:
                    # Metric = Dollars
                    total_grade_metric = total_grade_usage_lbs * price
                    composite_rate = total_grade_metric / total_grade_tons if total_grade_tons > 0 else 0.0
                    budget_rate_metric = baseline_rate * price # Budget $/Ton
                else:
                    # Metric = Lbs
                    total_grade_metric = total_grade_usage_lbs
                    composite_rate = total_grade_metric / total_grade_tons if total_grade_tons > 0 else 0.0
                    budget_rate_metric = baseline_rate # Budget Lbs/Ton

                chem_total_metric += total_grade_metric
                
                # Calculate Budget Totals for Main Row
                grade_budget_total = budget_rate_metric * total_grade_tons
                chem_total_budget += grade_budget_total
                chem_total_tons += total_grade_tons
                
                chem_details.append({
                    'grade': std_grade,
                    'rate': composite_rate,       # Weighted Average Actual + Forecast
                    'budget_rate': budget_rate_metric, # Fixed Baseline
                    'cost': total_grade_metric,
                    'tons': total_grade_tons
                })

            # Add to Category List
            if chem_total_metric > 0 or len(chem_details) > 0 or chem_total_budget > 0:
                cat = get_chemical_category(chem_name)
                all_chem_data[cat].append({
                    'name': chem_name, 
                    'data': {
                        'total': chem_total_metric, 
                        'budget_total': chem_total_budget,
                        'tons': chem_total_tons,
                        'details': chem_details
                    }
                })

        # -------------------------------------------------------------
        # 4. RENDER UI
        # -------------------------------------------------------------
        
        # Update Top Actuals Display (Keep strictly actuals for top metric)
        # Calculate strict actuals total for display
        strict_actual_val = 0.0
        for chem, grade_map in actual_usage_map.items():
            price = engine.get_price(mill, chem) if use_cost else 1.0
            total_lbs = sum(grade_map.values())
            strict_actual_val += (total_lbs * price)
            
        val_str = f"${strict_actual_val:,.2f}" if use_cost else f"{strict_actual_val:,.2f} Lbs"
        actual_display.children = [val_str]

        if actual_tons_total > 0:
            act_cpt = strict_actual_val / actual_tons_total
        else:
            act_cpt = 0.0
        
        unit_label = "$/Ton" if use_cost else "Lbs/Ton"
        cpt_str = f"{act_cpt:,.2f} {unit_label}"
        actual_cpt_display.children = [cpt_str]

        scope_msg = f"Actuals: {dt_start_actuals.strftime('%b %d')} - {dt_end_actuals.strftime('%b %d %H:%M')}"
        if dt_start_forecast:
             scope_msg += f" | Forecast: {dt_start_forecast.strftime('%b %d')} - {dt_end_forecast.strftime('%b %d')}"
        scope_display.children = [scope_msg]

        # Prepare Table Headers
        lbl_prefix = "Fcst" if is_forecast_mode else "Actual"
        lbl_unit = "$/T" if use_cost else "Lbs/T"
        header_label = f"{lbl_prefix} {lbl_unit}"
        modal_title_context = "Weighted Avg" if is_forecast_mode else "Actual"
        breakdown_title_text = "ACTUAL + FORECAST" if is_forecast_mode else "ACTUAL"
        
        breakdown_title_display.children = [f"USAGE BREAKDOWN - {breakdown_title_text}"]

        header_row = v.Row(
            class_='mb-2 font-weight-bold grey lighten-3 pa-2 rounded flex-nowrap',
            children=[
                v.Col(cols=3, children=[v.Html(tag='div', children=['Chemical'])]),
                v.Col(cols=2, class_='d-flex justify-center', children=[v.Html(tag='div', children=[header_label])]),
                v.Col(cols=1, class_='d-flex justify-center', children=[v.Html(tag='div', children=['% Grp'])]),
                v.Col(cols=2, class_='d-flex justify-center', children=[v.Html(tag='div', children=[f'Budget {lbl_unit}'])]),
                v.Col(cols=2, class_='d-flex justify-center', children=[v.Html(tag='div', children=[f'{lbl_unit} Var'])]),
                v.Col(cols=1, class_='d-flex justify-center', children=[v.Html(tag='div', children=['% Var'])]), 
                v.Col(cols=1, class_='d-flex justify-center', children=[v.Html(tag='div', children=['Details'])]),
            ]
        )
        
        rows_views = [header_row]
        forecast_rows_data.clear()
        forecast_group_headers.clear()

        # Render Rows
        for category in CATEGORY_ORDER:
            chem_list = all_chem_data.get(category, [])
            
            budget_val = engine.get_group_budget_per_ton(mill, category)
            if not use_cost: budget_val = 0.0 
            
            if len(chem_list) > 0 or budget_val > 0:
                # Group Header
                header_ui, header_widgets = create_group_header_row(category, budget_val, use_cost)
                rows_views.append(header_ui)
                forecast_group_headers[category] = header_widgets
                
                # Logic to handle Group Rate Change (Distribution)
                def make_group_rate_handler(cat, widgets, items):
                    def on_group_rate_change(widget, event, data):
                        try:
                            new_group_total = float(widget.v_model)
                            current_sum = 0.0
                            valid_rows = []
                            
                            for r in forecast_rows_data:
                                if r['state']['group'] == cat:
                                    try:
                                        val = float(r['w_per_ton'].v_model)
                                        current_sum += val
                                        valid_rows.append(r)
                                    except: pass
                            
                            if current_sum > 0:
                                ratio = new_group_total / current_sum
                                for r in valid_rows:
                                    old_val = float(r['w_per_ton'].v_model)
                                    new_val = old_val * ratio
                                    r['w_per_ton'].v_model = float(f"{new_val:.4f}")
                                    r['w_per_ton'].fire_event('change', None) 
                        except Exception as e: print(e)
                    return on_group_rate_change

                header_widgets['w_rate'].on_event('change', make_group_rate_handler(category, header_widgets, chem_list))
                
                # Group Budget Handler
                def make_group_budget_handler(cat, widgets):
                    def on_group_budget_change(widget, event, data):
                        recalculate_totals()
                    return on_group_budget_change

                header_widgets['w_budget'].on_event('change', make_group_budget_handler(category, header_widgets))

                # Individual Rows
                chem_list_len = len(chem_list) 
                for item in chem_list:
                    # Calculate Weighted Budget Per Ton (from Baselines)
                    chem_budget_val = 0.0
                    c_budget_total = item['data'].get('budget_total', 0.0)
                    c_tons = item['data'].get('tons', 0.0)
                    
                    if c_tons > 0:
                        chem_budget_val = c_budget_total / c_tons
                    
                    row_obj = create_chemical_row(
                        item['name'], 
                        item['data']['total'], 
                        item['data']['details'], 
                        use_cost, 
                        group_name=category, 
                        tons_basis=total_production_tons, 
                        context_label=modal_title_context,
                        show_group_pct=(chem_list_len > 1),
                        row_budget=chem_budget_val
                    )
                    forecast_rows_data.append(row_obj)
                    rows_views.append(row_obj['layout'])

        results_container.children = rows_views
        recalculate_totals()
        
    except Exception as e:
        scope_display.children = [f"Error: {str(e)}"]
        traceback.print_exc()
    finally:
        btn_init_analysis.loading = False

btn_init_analysis.on_event('click', on_load_baseline)

# ==========================================
# 🚀 INITIALIZATION & LAYOUT
# ==========================================

app = v.App(
    children=[
        css_style,
        v.Container(
            fluid=True, # Allow full width
            class_='mx-auto pa-4', # Remove max-width restriction
            style_='max-width: 65%;', # Added constraint
            children=[
                app_bar,
                v.Row(children=[
                    v.Col(cols=12, children=[config_card]),
                    v.Col(cols=12, children=[combined_card])
                ])
            ]
        )
    ]
)

if __name__ == "__main__":
    try: spy.login(url='http://localhost:34216', force=False, quiet=True)
    except: pass
    display(app)
