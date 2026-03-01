import psycopg2
from config import DB_CONFIG
import uuid

def verify_full_automation():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        print("--- VERIFICATION START ---")

        # 1. Test INSERT Automation
        print("\n[Test 1] Testing INSERT automation...")
        # We need a payer and plan that exist to satisfy FKs (if any) or just a valid test case
        cur.execute("SELECT payer_id, plan_id FROM plan_details LIMIT 1")
        base_row = cur.fetchone()
        if not base_row:
            print("No data available to test.")
            return
        
        test_payer_id, base_plan_id = base_row
        
        # We'll create a dummy plan entry for a fresh insert test if possible,
        # or just insert into coverage_history for an existing plan that doesn't have an entry yet.
        # Let's find a plan that is NOT in coverage_history.
        cur.execute("""
            SELECT pd.plan_id, pd.payer_id 
            FROM plan_details pd 
            LEFT JOIN coverage_history ch ON pd.plan_id = ch.plan_id 
            WHERE ch.plan_id IS NULL 
            LIMIT 1
        """)
        insert_row = cur.fetchone()
        
        if insert_row:
            test_plan_id, test_payer_id = insert_row
            print(f"Inserting new coverage record for Plan: {test_plan_id} with active_flag = FALSE")
            cur.execute("""
                INSERT INTO coverage_history (coverage_history_id, payer_id, plan_id, active_flag)
                VALUES (%s, %s, %s, %s)
            """, (str(uuid.uuid4()), test_payer_id, test_plan_id, False))
            
            # Check if synced
            cur.execute("SELECT status FROM plan_details WHERE plan_id = %s", (test_plan_id,))
            status = cur.fetchone()[0]
            print(f"Resulting plan_details status: {status}")
            if status == 'inactive':
                print("PASSED: INSERT automation works!")
            else:
                print("FAILED: INSERT automation failed.")
        else:
            print("Skipping INSERT test (no plans found without coverage records).")

        # 2. Test UPDATE Automation
        print("\n[Test 2] Testing UPDATE automation...")
        cur.execute("""
            SELECT pd.plan_id 
            FROM plan_details pd 
            JOIN coverage_history ch ON pd.plan_id = ch.plan_id 
            WHERE pd.status = 'active' AND ch.active_flag = TRUE 
            LIMIT 1
        """)
        update_row = cur.fetchone()
        if update_row:
            up_plan_id = update_row[0]
            print(f"Updating coverage record for Plan: {up_plan_id} to active_flag = FALSE")
            cur.execute("UPDATE coverage_history SET active_flag = FALSE WHERE plan_id = %s", (up_plan_id,))
            
            # Check if synced
            cur.execute("SELECT status FROM plan_details WHERE plan_id = %s", (up_plan_id,))
            status = cur.fetchone()[0]
            print(f"Resulting plan_details status: {status}")
            if status == 'inactive':
                print("PASSED: UPDATE automation works!")
            else:
                print("FAILED: UPDATE automation failed.")
        else:
            print("Skipping UPDATE test (no active plans found with coverage records).")

        # Rollback all test data
        conn.rollback()
        print("\n--- VERIFICATION COMPLETE (All changes rolled back) ---")

    except Exception as e:
        print(f"Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    verify_full_automation()
