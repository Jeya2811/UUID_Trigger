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
CREATE OR REPLACE FUNCTION sync_plan_inactivity()
RETURNS TRIGGER AS $$
BEGIN
    -- Fire if the plan is being marked as inactive (active_flag = FALSE)
    -- This handles both new INSERTS and UPDATES from TRUE to FALSE
    IF (NEW.active_flag IS FALSE) THEN
        UPDATE plan_details
        SET status = 'inactive',
            last_updated_date = CURRENT_TIMESTAMP
        WHERE plan_id = NEW.plan_id
        AND status != 'inactive'; -- Only update if not already inactive

        UPDATE drug_formulary_details
        SET coverage_status = 'Inactive',
            last_updated_date = CURRENT_TIMESTAMP
        WHERE plan_id = NEW.plan_id
        AND coverage_status != 'Inactive';
        
        RAISE NOTICE 'Trigger: Plan % and its formulary records marked as inactive.', NEW.plan_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_sync_plan_inactivity ON coverage_history;
CREATE TRIGGER trg_sync_plan_inactivity
AFTER INSERT OR UPDATE ON coverage_history
FOR EACH ROW
EXECUTE FUNCTION sync_plan_inactivity();
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
