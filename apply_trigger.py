import psycopg2
from config import DB_CONFIG
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_and_apply_trigger():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # 1. Check if table exists
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'coverage_history');")
        exists = cur.fetchone()[0]
        
        if not exists:
            logger.error("Table 'coverage_history' DOES NOT EXIST in the database!")
            # List all tables to help debug
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = [row[0] for row in cur.fetchall()]
            logger.info(f"Available tables: {tables}")
            return

        logger.info("Table 'coverage_history' found. Proceeding with trigger application...")

        sql_script = """
CREATE OR REPLACE FUNCTION sync_coverage_to_plan_status()
RETURNS TRIGGER AS $$
DECLARE
    new_status VARCHAR(20);
BEGIN
    -- Determine the new status text based on the active_flag
    IF (NEW.active_flag IS TRUE) THEN
        new_status := 'active';
    ELSE
        new_status := 'inactive';
    END IF;

    -- 1. Sync status to plan_details
    UPDATE plan_details
    SET status = new_status,
        last_updated_date = CURRENT_TIMESTAMP
    WHERE plan_id = NEW.plan_id
    AND status != new_status;

    -- 2. Sync status to drug_formulary_details (new column)
    UPDATE drug_formulary_details
    SET plan_status = new_status,
        last_updated_date = CURRENT_TIMESTAMP
    WHERE plan_id = NEW.plan_id
    AND plan_status != new_status;
    
    RAISE NOTICE 'Trigger: Plan % status synced to % in all related tables.', NEW.plan_id, new_status;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_plan_inactivity ON coverage_history;
CREATE TRIGGER trg_sync_plan_inactivity
AFTER INSERT OR UPDATE ON coverage_history
FOR EACH ROW
EXECUTE FUNCTION sync_coverage_to_plan_status();
"""
        cur.execute(sql_script)
        conn.commit()
        logger.info("SQL Trigger applied successfully.")
        
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to apply trigger: {e}")
        raise
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    verify_and_apply_trigger()
