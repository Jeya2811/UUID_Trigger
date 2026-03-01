import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import uuid
from database import get_db_connection, ensure_database_schema

def insert_or_get_payer(payer_name):
    """Generate a deterministic UUID based on the payer name. No database interaction."""
    if not payer_name:
        return None
    # Use namespace OID to generate a consistent UUID for the same name
    return str(uuid.uuid5(uuid.NAMESPACE_OID, str(payer_name).strip()))

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

csv_path = 'Payer_Master Table(Payer Master).csv'
df = pd.read_csv(csv_path)
df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('Unnamed:', 'drop_')
df = df.loc[:, ~df.columns.str.contains('^drop_')]

print("Data shape:", df.shape)
print("Columns:", df.columns.tolist())

# --- UUID Generation Start ---
print("Generating/Retrieving Payer UUIDs...")
# Iterate over DataFrame and update Payer_ID
for index, row in df.iterrows():
    payer_name = row.get('Payer_name') or row.get('Payer_Name')
    if pd.notna(payer_name):
        # Generate deterministic UUID
        payer_id = insert_or_get_payer(str(payer_name))
        df.at[index, 'Payer_ID'] = payer_id
    else:
        print(f"Warning: Missing Payer_name at row {index}")

# Verify IDs are populated
print("Sample Payer IDs:", df['Payer_ID'].head().tolist())
# --- UUID Generation End ---

# Create engine for bulk insert
engine = create_engine(conn_str)

# Create table
# Note: 'payer_master' schema should ideally match the DataFrame.
# If Payer_ID is now a UUID string, it will be TEXT/VARCHAR in Postgres.
df.to_sql('payer_master', engine, if_exists='replace', index=False, method='multi')
print("✅ SUCCESS: 'payer_master' table created/replaced with UUIDs!")
