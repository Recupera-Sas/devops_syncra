import time
from google.api_core import exceptions as gcp_exceptions
import firebase_admin
from firebase_admin import credentials, firestore
import polars as pl
import re
from typing import List, Dict, Any

# --- CONFIGURATION (MUST BE UPDATED) ---
CREDENTIALS_FILE = "cloud/datalake-7a937-firebase-adminsdk-fbsvc-a5778267ab.json"
PROJECT_ID = "datalake-7a937"
COLLECTION_NAME = "demographics_cruice" 
    
# --- AUXILIARY FUNCTIONS ---

def _get_existing_keys(db: firestore.client) -> set:
    """
    Fetches the combined keys (account_value:demographic_value) from Firestore
    to efficiently check for existing records.
    """
    if not db:
        print("Skipping Firestore read due to initialization error.")
        return set()

    print(f"\n🔍 Querying existing keys from Firestore collection: '{COLLECTION_NAME}'...")
    existing_keys = set()
    
    # Fetch 'account' and 'demographic' fields for optimization
    try:
        docs = db.collection(COLLECTION_NAME).select(['account', 'demographic']).stream()
        for doc in docs:
            data = doc.to_dict()
            if 'account' in data and 'demographic' in data:
                # Create the unique key: 'account:demographic'
                key = f"{data['account']}:{data['demographic']}"
                existing_keys.add(key)
        print(f"✅ Found {len(existing_keys)} existing unique records in Firestore.")
    except Exception as e:
        print(f"❌ Error fetching data from Firestore: {e}")

    return existing_keys

# --- MAIN PROCESSOR FUNCTION ---

def process_and_validate_demographics(csv_path: str, db: firestore.client) -> List[Dict[str, Any]]:
    """
    Reads a CSV, transforms the data, validates against Firestore, and returns 
    a list of new records ready for insertion.
    """
    
    # 1. Polars Lazy Execution: Read, Rename, and Transform
    print(f"\n⚙️ Starting Polars processing for file: {csv_path}...")
    
    # IMPORTANT: The separator is set to ';' as previously defined
    df_transformed = pl.scan_csv(
        csv_path, 
        separator=';',
        schema={"identificacion": pl.Utf8, "cuenta": pl.Utf8, "ciudad": pl.Utf8, "depto": pl.Utf8, "dato": pl.Utf8, "tipodato": pl.Utf8, "Marca": pl.Utf8}
    ).with_columns(
        # Standardize and rename columns immediately
        pl.col("cuenta").alias("account"),
        pl.col("identificacion").alias("document"),
        pl.col("dato").alias("demographic")
   ).with_columns(
        pl.col("demographic")
            .str.replace_all(r"[^0-9]", "") 
            .str.replace_all(r"^0+", "")    
            .alias("cleaned_numeric_dato")
    ).with_columns(
    pl.when(pl.col("demographic").str.contains("@"))
            .then(pl.col("demographic")) 
            .otherwise(pl.col("cleaned_numeric_dato")) # Use cleaned numeric if not email
            .alias("demographic")
    ).with_columns(
        # Determine the 'type' (email, movil, fijo)
        pl.when(pl.col("demographic").str.contains("@"))
          .then(pl.lit("email"))
          .when(
              (pl.col("cleaned_numeric_dato").str.starts_with("3")) & 
              (pl.col("cleaned_numeric_dato").str.len_chars() == 10)
          )
          .then(pl.lit("movil"))
          .when(
              (pl.col("cleaned_numeric_dato").str.starts_with("6")) & 
              (pl.col("cleaned_numeric_dato").str.len_chars() == 10)
          )
          .then(pl.lit("fijo"))
          .otherwise(pl.lit("errado"))
          .alias("type")
    ).with_columns(
        # Create a unique key for validation against Firestore using 'account:demographic'
        (pl.col("account").cast(pl.Utf8) + ":" + pl.col("demographic").cast(pl.Utf8))
        .alias("firestore_key")
    ).select(
        "account",
        "document",
        "demographic",
        "type",
        "firestore_key"
    ).collect() # Execute Polars pipeline

    # Handle case where DataFrame is empty
    if df_transformed.is_empty():
        print("⚠️ CSV is empty or reading failed. Returning empty list.")
        return []

    # 2. Firestore Validation
    existing_keys = _get_existing_keys(db)

    if not existing_keys:
        print("➡️ No existing keys to check. All records will be considered new.")
        df_transformed = df_transformed.filter(
            pl.col("type") != "errado"
        )
    else:
        # 3. Filter Records (Polars Filtering using a Set)
        print("➡️ Filtering out records that already exist in Firestore...")
        
        # Convert existing_keys set to a Polars Literal for filtering
        existing_keys_lit = pl.Series("firestore_key", list(existing_keys))
        
        # First, filter out rows with 'type' as 'errado'
        df_transformed = df_transformed.filter(
            pl.col("type") != "errado"
        )
        
        # Use a filter expression: keep rows where 'firestore_key' is NOT in 'existing_keys_lit'
        df_transformed = df_transformed.filter(~pl.col("firestore_key").is_in(existing_keys_lit))
    
    # 4. Final Structure Preparation
    # Select the required final columns and convert to a list of Python dictionaries
    final_output = df_transformed.select(
        "account",
        "document",
        "demographic",
        "type"
    ).to_dicts()

    print(f"✅ Polars processing complete. {len(final_output)} new records found.")
    return final_output

# --- INITIAL PROCESS ---
def insert_demographics_to_firebase(csv_file_path, gui_process_data):
    """
    Main function to insert data from a CSV into Firestore.
    """
    # Initialize Firebase (assuming successful credential loading)
    try:
        cred = credentials.Certificate(CREDENTIALS_FILE)
        firebase_admin.initialize_app(cred, {"projectId": PROJECT_ID})
        db = firestore.client()
        print("✅ Firestore connection established.")
    except Exception as e:
        print(f"❌ Error initializing Firebase. Please ensure '{CREDENTIALS_FILE}' is correct.")
        db = None # Set to None to prevent subsequent errors
    
    # Run the processor
    if db:
        new_records = process_and_validate_demographics(csv_file_path, db)
    
        print("\n--- NEW RECORDS TO INSERT ---")
        if new_records:
            for record in new_records:
                db.collection(COLLECTION_NAME).add(record)
            print(f"\n✔️ Successfully inserted {len(new_records)} new documents.")
        else:
            print("⚠️ No new records found after validation.")
    else:
        print("\nProcess aborted due to Firebase connection error.")