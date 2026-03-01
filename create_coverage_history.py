import psycopg2
import uuid
import logging
from datetime import datetime
from config import DB_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_and_populate_coverage_history():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1. Create the coverage_history table
        logger.info("Creating coverage_history table...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS coverage_history (
                coverage_history_id UUID PRIMARY KEY,
                payer_id VARCHAR(36) NOT NULL,
                plan_id VARCHAR(36) NOT NULL,
                coverage_data_available BOOLEAN DEFAULT TRUE,
                source_type VARCHAR(50),
                source_link TEXT,
                source_document_version VARCHAR(50),
                last_verified_date TIMESTAMP,
                active_flag BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # 1b. Clean existing records (since we are refactoring the source)
        logger.info("Cleaning existing coverage_history records...")
        cur.execute("TRUNCATE TABLE coverage_history;")
        
        # 2. Fetch data from payer_plan_master ONLY
        logger.info("Fetching data strictly from Master tables...")
        cur.execute("""
            SELECT 
                "PAYER_ID" as payer_id,
                "PP_ID" as plan_id
            FROM payer_plan_master;
        """)
        
        rows = cur.fetchall()
        logger.info(f"Found {len(rows)} master plans to populate coverage_history.")

        if not rows:
            logger.info("No data in payer_plan_master to insert.")
            conn.commit()
            return

        # 3. Prepare data for insertion (BPO fallbacks)
        insert_data = []
        for row in rows:
            payer_id, plan_id = row
            
            # Transformation rules for Master-Only source
            ch_id = str(uuid.uuid4())
            coverage_data_available = False # Set to FALSE as we are referencing master only (BPO)
            source_type = "BPO"             # Default type for master-only sourcing
            source_link = None              # No link in master table
            source_document_version = "January" 
            last_verified_date = None       # No date in master table
            active_flag = True
            now = datetime.now()

            insert_data.append((
                ch_id, payer_id, plan_id, coverage_data_available,
                source_type, source_link, source_document_version,
                last_verified_date, active_flag, now, now
            ))

        # 4. Bulk Insert
        logger.info("Inserting records into coverage_history...")
        insert_query = """
            INSERT INTO coverage_history (
                coverage_history_id, payer_id, plan_id, coverage_data_available,
                source_type, source_link, source_document_version,
                last_verified_date, active_flag, created_at, updated_at
            ) VALUES %s
            ON CONFLICT (coverage_history_id) DO NOTHING;
        """
        
        from psycopg2.extras import execute_values
        execute_values(cur, insert_query, insert_data)

        conn.commit()
        logger.info(f"Successfully populated coverage_history with {len(insert_data)} records.")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Error in create_and_populate_coverage_history: {e}")
        raise
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    create_and_populate_coverage_history()
