import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import uuid
from database import get_db_connection

def insert_or_get_payer(payer_name):
    """Deterministic Payer ID generation"""
    if not payer_name: return None
    return str(uuid.uuid5(uuid.NAMESPACE_OID, str(payer_name).strip()))

def insert_or_get_plan(plan_name, payer_id, state_name, payer_name_raw):
    """Deterministic Plan ID generation"""
    # Create a unique key for the plan
    unique_key = f"{payer_name_raw}|{plan_name}|{state_name}"
    return str(uuid.uuid5(uuid.NAMESPACE_OID, unique_key))

# From file.env - PASSWORD URL-ENCODED
DB_NAME = 'ebv'
DB_USER = 'postgres'
DB_PASSWORD = 'Jg@281105'
DB_HOST = 'localhost'
DB_PORT = '5432'

# URL-encode password (fixes @ issue)
encoded_password = quote_plus(DB_PASSWORD)

conn_str = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(f"Connecting to: {DB_HOST}:{DB_PORT}/{DB_NAME}")

csv_path = '2026 Plan Master(January).csv'
df = pd.read_csv(csv_path)
df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('Unnamed:', 'drop_')
df = df.loc[:, ~df.columns.str.contains('^drop_')]

print("Data shape:", df.shape)
print("Columns:", df.columns.tolist())

# --- UUID Generation Start ---
print("Generating/Retrieving Payer and Plan UUIDs...")

# 1. Fetch existing payers from payer_master table for cross-referencing
existing_payers = set()
try:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT "Payer_Name" FROM payer_master')
            existing_payers = {row[0] for row in cur.fetchall() if row[0]}
except Exception as e:
    print(f"Warning: Could not fetch existing payers from database: {e}")

notified_new_payers = set()

# Iterate over DataFrame
for index, row in df.iterrows():
    # The CSV has Payer Name in 'PAYER_ID' column usually, let's check
    # Based on inspection: 'PAYER_ID' col has "Cigna Healthcare" etc.
    payer_name_raw = row.get('PAYER_ID')
    plan_name = row.get('PLAN_NAME')
    state_name = row.get('STATE_NAME')
    
    if pd.notna(payer_name_raw):
        # Check if payer is in the master list
        clean_name = str(payer_name_raw).strip()
        if clean_name not in existing_payers and clean_name not in notified_new_payers:
            print(f"Payer {clean_name} is not present in the payer_master, created a new UUID")
            notified_new_payers.add(clean_name)

        # 1. Get Payer UUID
        payer_uuid = insert_or_get_payer(clean_name)
        
        # Update the DataFrame to have the actual UUID in PAYER_ID
        df.at[index, 'PAYER_ID'] = payer_uuid
        
        # 2. Get Plan UUID (PP_ID)
        if pd.notna(plan_name):
            if pd.isna(state_name):
                 state_name = ""
            
            plan_uuid = insert_or_get_plan(str(plan_name), payer_uuid, str(state_name), str(payer_name_raw))
            df.at[index, 'PP_ID'] = plan_uuid
        else:
            print(f"Warning: Missing PLAN_NAME at row {index}")
    else:
        print(f"Warning: Missing PAYER_ID (Name) at row {index}")
    
    if index % 100 == 0:
        print(f"Processed row {index} / {len(df)}")

# Verify IDs are populated
print("Sample PAYER_IDs (UUIDs):", df['PAYER_ID'].head(3).tolist())
print("Sample PP_IDs (UUIDs):", df['PP_ID'].head(3).tolist())
# --- UUID Generation End ---

# Create engine for bulk insert
engine = create_engine(conn_str)

# Create table
df.to_sql('payer_plan_master', engine, if_exists='replace', index=False, method='multi')
print("✅ SUCCESS: 'plan_master' table created/replaced with UUIDs!")