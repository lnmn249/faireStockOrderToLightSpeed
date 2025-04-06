import pandas as pd
import requests
import os
from dotenv import load_dotenv

# Load environment variables
DRY_RUN = False
# DRY_RUN = True
load_dotenv()
API_KEY = os.getenv("LS_API_KEY")
DOMAIN_PREFIX = os.getenv("LS_DOMAIN_PREFIX")  # e.g., 'yourstore'
OUTLET_ID = os.getenv("OUTLET_ID")
BASE_URL = f"https://{DOMAIN_PREFIX}.retail.lightspeed.app/api/2.0"
HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json"
}
def get_all_products():
    url = f"{BASE_URL}/products"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }

    products = []
    while url:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break

        data = response.json()

        if not data.get("data"):
            break

        products.extend(data["data"])

        # Get the next version-based page
        version = data.get("version", {})
        if "max" in version:
            url = f"{BASE_URL}/products?after={version['max']}"
        else:
            break

    return products
def save_all_products_CSV(product_list, filename):
    import pandas as pd
    products_df = pd.DataFrame(product_list)
    products_df.to_csv(filename)
def get_all_inventory():
    url = f"{BASE_URL}/inventory"
    inventory = []
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/json"
    }

    while True:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break

        r = response.json()
        batch = r.get('data', [])
        if not batch:
            break

        inventory.extend(batch)

        # Use version-based pagination
        version = r.get("version", {}).get("max")
        if version:
            url = f"{BASE_URL}/inventory?after={version}"
        else:
            break  # No next page available

    return inventory
def save_inventory_CSV(inventory_list, filename):
    import pandas as pd
    inventory_df = pd.DataFrame(inventory_list)
    inventory_df.to_csv(filename)
def read_faire_order(file_path: str) -> pd.DataFrame:
    import pandas as pd
    """
    Reads a Faire order CSV file and returns a DataFrame.
    
    Parameters:
        file_path (str): Path to the Faire order CSV file.
        
    Returns:
        pd.DataFrame: The loaded order data.
    """
    try:
        df = pd.read_csv(file_path)
        # print(f"Successfully loaded {len(df)} rows from '{file_path}'.")
        return df
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
    except pd.errors.ParserError:
        print("Error: Could not parse the file.")
    return pd.DataFrame()  # Return empty DataFrame on error

def read_products_csv(csv_filepath):
    import pandas as pd
    import ast

    """
    Reads a Lightspeed products CSV file and returns a DataFrame,
    filtering out rows with missing supplier_code. Also extracts
    brand names from the 'brand' column into a new 'brand_name' column.

    Parameters:
        csv_filepath (str): Path to the products CSV file.

    Returns:
        pd.DataFrame: The loaded product data with valid supplier codes.
    """
    try:
        df = pd.read_csv(csv_filepath)
        df = df[df['supplier_code'].notna()]

        if 'brand' in df.columns:
            def extract_brand_name(brand_str):
                try:
                    brand_info = ast.literal_eval(brand_str)
                    return brand_info.get('name')
                except (ValueError, SyntaxError, TypeError):
                    return None

            df['brand_name'] = df['brand'].apply(extract_brand_name)

        return df

    except FileNotFoundError:
        print(f"Error: File '{csv_filepath}' not found.")
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
    except pd.errors.ParserError:
        print("Error: Could not parse the file.")
    except KeyError:
        print("Error: The 'supplier_code' column is missing.")
    
    return pd.DataFrame()  # Return empty DataFrame on error
def get_all_brands():
    """
    Retrieves all brands from Lightspeed X-Series using paginated API calls.
    """
    url = f"{BASE_URL}/brands"
    brands = []

    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break

        data = response.json()
        brands.extend(data.get('data', []))

        # Handle pagination
        version = data.get("version", {})
        max_version = version.get("max")
        if max_version:
            url = f"{BASE_URL}/brands?after={max_version}"
        else:
            break

    return brands
def match_products_and_find_missing(products_df, faire_df):
    import pandas as pd
    """
    Matches Lightspeed products (by supplier_code and brand_name) with Faire order SKUs and Brand Name.
    Returns two DataFrames:
      - existing (matched products with Lightspeed IDs)
      - missing (products not found in Lightspeed)
    Both are guaranteed to be valid DataFrames (even if empty).
    """
    if 'SKU' not in faire_df.columns or 'supplier_code' not in products_df.columns:
        raise ValueError("Faire data must contain 'SKU', and Lightspeed data must contain 'supplier_code'.")

    if 'brand_name' not in products_df.columns:
        raise ValueError("Lightspeed data must include 'brand_name'.")
    if 'Brand Name' not in faire_df.columns:
        raise ValueError("Faire data must include 'Brand Name'.")

    # Create slimmed down, temporary copies for merging without modifying originals
    products_slim = products_df[['id', 'supplier_code', 'brand_name', 'name']].dropna(subset=['supplier_code', 'brand_name'])
    faire_slim = faire_df[['SKU', 'Brand Name']].copy()

    # Merge on both SKU + brand
    merged_df = pd.merge(
        faire_df,
        products_slim,
        how='left',
        left_on=['SKU', 'Brand Name'],
        right_on=['supplier_code', 'brand_name'],
        suffixes=('', '_lightspeed')
    )

    # Separate matched and unmatched
    existing = merged_df[merged_df['id'].notna()].copy()
    missing = merged_df[merged_df['id'].isna()].copy()

    # Ensure consistent structure
    if existing.empty:
        existing = pd.DataFrame(columns=merged_df.columns)
    if missing.empty:
        missing = pd.DataFrame(columns=merged_df.columns)

    return existing, missing
def get_first_brand_name(faire_df):
    """
    Returns the brand name from the first row of the Faire order.
    """
    if 'Brand Name' in faire_df.columns and not faire_df.empty:
        return str(faire_df.iloc[0]['Brand Name']).strip()
    return "Unknown Supplier"
def ensure_supplier_and_brand(brand_name: str, dry_run: bool = False) -> dict:
    """
    Ensures a supplier and brand with the given name exist in Lightspeed X-Series.
    Creates them if they do not exist.

    Args:
        brand_name (str): The name to use for both supplier and brand.
        dry_run (bool): Simulate the process without real API calls.

    Returns:
        dict: {'supplier_id': str, 'brand_id': str}
    """
    supplier_id = None
    brand_id = None

    # Load current data
    suppliers = get_all_suppliers()
    brands = get_all_brands()

    # Check for existing supplier
    supplier = find_supplier_by_name(suppliers, brand_name)
    if supplier:
        supplier_id = supplier['id']
    else:
        result = create_supplier(name=brand_name, dry_run=dry_run)
        if result and 'data' in result:
            supplier_id = result['data']

    # Check for existing brand
    brand = find_supplier_by_name(brands, brand_name)  # same match logic
    if brand:
        brand_id = brand['id']
    else:
        result = create_brand(name=brand_name, dry_run=dry_run)
        if result and 'data' in result:
            brand_id = result['data']

    return {
        "supplier_id": supplier_id,
        "brand_id": brand_id
    }
def get_all_suppliers():
    url = f"{BASE_URL}/suppliers"
    suppliers = []

    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break

        data = response.json()
        suppliers.extend(data.get('data', []))

        # Pagination fix: stop if version max is None
        version = data.get("version", {})
        max_version = version.get("max")
        if max_version:
            url = f"{BASE_URL}/suppliers?after={max_version}"
        else:
            break

    return suppliers

def get_all_brands():
    """
    Retrieves all brands from Lightspeed X-Series using paginated API calls.
    """
    url = f"{BASE_URL}/brands"
    brands = []

    while url:
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error {response.status_code}: {response.text}")
            break

        data = response.json()
        brands.extend(data.get('data', []))

        # Handle pagination
        version = data.get("version", {})
        max_version = version.get("max")
        if max_version:
            url = f"{BASE_URL}/brands?after={max_version}"
        else:
            break

    return brands
def find_supplier_by_name(suppliers: list, target_name: str) -> dict:
    """
    Looks for a supplier in the list that matches the target name (case-insensitive).
    Returns the full supplier dict if found, otherwise None.
    """
    target = target_name.strip().lower()
    for supplier in suppliers:
        if supplier.get("name", "").strip().lower() == target:
            return supplier
    return None
def create_product(payload: dict):
    if DRY_RUN:
        print(f"[DRY RUN] Would create product: {payload['name']} (SKU: {payload['supplier_code']})")
        return {"id": f"dry_{payload['supplier_code']}", "name": payload["name"]}  # fake ID
    else:
        url = f"{BASE_URL}/products"
        response = requests.post(url, headers=HEADERS, json=payload)
        # print(response.status_code)
        if response.status_code == 200 or response.status_code == 201:
            # print(response.json())
            print(f"Created Product {payload['name']}, SKU {payload['supplier_code']}")
            return response.json().get('data')
        else:
            print(f"Failed to create product: {response.text}")
            return None
def create_missing_products(missing_df, dry_run: bool = False):
    """
    Create new products in Lightspeed for each missing SKU.
    Returns a list of product records with 'id', 'supplier_code', and 'name'.
    """
    if missing_df.empty:
        print("No missing products to create.")
        return []

    # Extract brand name from the first row
    brand_name = get_first_brand_name(missing_df)

    # Ensure supplier and brand exist
    ids = ensure_supplier_and_brand(brand_name, dry_run=dry_run)
    supplier_id = ids.get("supplier_id")
    brand_id = ids.get("brand_id")

    created = []

    for _, row in missing_df.iterrows():
        sku = row['SKU']
        name = f"{row['Product Name']}"
        supply_price = float(str(row['Wholesale Price']).replace('$', '').strip())
        retail_price = float(str(row['Retail Price']).replace('$', '').strip())
        quantity = f"{row['Quantity']}"
        brand = f"{row['Brand Name']}"

        payload = {
            "name": name,
            "supplier_code": sku,
            "supply_price": supply_price,
            "price_excluding_tax": retail_price,
            "customSku": True,
            "type": "standard",
            "supplier_id": supplier_id,
            "brand_id": brand_id,
            "inventory": [{ "current_amount": 0,
                           "outlet_id": OUTLET_ID
                            }]       
        }
        # print(payload)

        result = create_product(payload)
        # print(result[0])
        if result:
            created.append({
                "id": result[0],
                "supplier_code": sku,
                "name": name,
                "Quantity": quantity,
                "Wholesale Price": supply_price,
                "Retail Price": retail_price,
                "Brand Name": brand
            })

    return created
def create_supplier(name: str, description: str = "", dry_run: bool = False) -> dict:
    """
    Creates a new supplier in Lightspeed X-Series.

    Args:
        name (str): Supplier name.
        description (str): Optional description.
        dry_run (bool): If True, simulate without making the API call.

    Returns:
        dict: Created supplier data or simulated response.
    """
    if dry_run:
        print(f"[DRY RUN] Would create supplier: {name}")
        return {"name": name, "description": description, "id": "simulated-supplier-id"}

    url = f"{BASE_URL}/suppliers"
    payload = {
        "name": name,
        "description": description or name
    }

    response = requests.post(url, headers=HEADERS, json=payload)
    if response.status_code == 201 or response.status_code == 200:
        # print(response)
        print(f"Supplier '{name}' created successfully.")
        result = response.json()
        return result
    else:
        print(f"Error creating supplier '{name}': {response.text}")
        return None
def create_brand(name: str, dry_run: bool = False) -> dict:
    """
    Creates a new brand in Lightspeed X-Series.

    Args:
        name (str): Brand name.
        dry_run (bool): If True, simulate without making the API call.

    Returns:
        dict: Created brand data or simulated response.
    """
    if dry_run:
        print(f"[DRY RUN] Would create brand: {name}")
        return {"name": name, "id": "simulated-brand-id"}

    url = f"{BASE_URL}/brands"
    payload = {
        "name": name
    }

    response = requests.post(url, headers=HEADERS, json=payload)
    if response.status_code == 201 or response.status_code == 200:
        print(f"Brand '{name}' created successfully.")
        result = response.json()
        return result
    else:
        print(f"Error creating brand '{name}': {response.text}")
        return None
def clean_price(price_val):
    """
    Converts a price string like '$14.70' to float 14.70.
    Returns 0.0 if the value cannot be parsed.
    """
    if pd.isna(price_val):
        return 0.0

    try:
        if isinstance(price_val, (float, int)):
            return float(price_val)

        # Clean and convert
        cleaned = (
            str(price_val)
            .replace('$', '')
            .replace(',', '')
            .replace('\xa0', '')  # non-breaking space
            .strip()
        )
        return float(cleaned)
    except Exception as e:
        print(f"Warning: couldn't convert '{price_val}' to float. Error: {e}")
        return 0.0

def combine_product_ids(existing_df: pd.DataFrame, created_products: list) -> pd.DataFrame:
    """
    Combines existing and newly created products into one DataFrame with quantities,
    wholesale price, and brand name. Handles empty inputs gracefully.
    """
    # Define the desired output columns
    output_columns = ['id', 'supplier_code', 'Quantity', 'Wholesale Price', 'Brand Name']

    # Prepare existing products DataFrame
    if not existing_df.empty:
        existing_slim = existing_df.copy()
        # Try to access columns safely
        for col in ['id', 'SKU', 'Quantity', 'Wholesale Price', 'Brand Name']:
            if col not in existing_slim.columns:
                existing_slim[col] = None
        # existing_slim = existing_slim.rename(columns={'SKU': 'supplier_code'})
        existing_slim = existing_slim[output_columns]
    else:
        existing_slim = pd.DataFrame(columns=output_columns)

    # If no created products, return existing only
    if not created_products:
        return existing_slim

    # Create DataFrame from created products
    created_df = pd.DataFrame(created_products)

    # Ensure required columns exist
    for col in ['id', 'supplier_code', 'Quantity', 'Wholesale Price', 'Brand Name']:
        if col not in created_df.columns:
            created_df[col] = None

    # Attempt to map Quantity from existing_df if possible
    if not existing_df.empty and 'supplier_code' in existing_df.columns and 'Quantity' in existing_df.columns:
        quantity_map = existing_df.set_index('supplier_code')['Quantity'].to_dict()
        created_df['Quantity'] = created_df['supplier_code'].map(quantity_map).fillna(created_df['Quantity']).fillna(1).astype(int)
    else:
        # Default to 1 if Quantity not available
        created_df['Quantity'] = created_df['Quantity'].fillna(1).astype(int)

    # Reorder and trim created_df to match output columns
    created_slim = created_df[output_columns]

    # Combine and drop any rows with no ID (just in case)
    combined = pd.concat([existing_slim, created_slim], ignore_index=True)
    combined = combined.dropna(subset=['id'])

    return combined

def create_stock_order_shell(location_id: int, faire_df: pd.DataFrame, dry_run: bool = False) -> dict:
    """
    Creates a stock order (consignment) in Lightspeed X-Series using supplier from Faire order.
    """
    # Extract brand/supplier name from order
    brand_name = get_first_brand_name(faire_df)

    # Ensure supplier/brand exist
    ids = ensure_supplier_and_brand(brand_name, dry_run=dry_run)
    supplier_id = ids.get("supplier_id")

    if dry_run:
        print(f"[DRY RUN] Would create stock order for supplier: {brand_name} at location {location_id}")
        return {"id": "simulated-stock-order-id"}

    url = f"{BASE_URL}/consignments"
    payload = {
        "name": f"Faire Stock Order - {brand_name}",
        "outlet_id": location_id,
        "type": "SUPPLIER",
        "status": "OPEN",
        "supplier_id": supplier_id
    }

    response = requests.post(url, headers=HEADERS, json=payload)
    # print(response.status_code)
    if response.status_code == 201:
        data = response.json().get("data", {})
        stock_order_id = data.get("id")
        print(f"Created stock order with ID: {stock_order_id}")
        return data
    else:
        print(f"Error creating stock order shell: {response.text}")
        return None
def build_stock_order_lines(product_df: pd.DataFrame) -> list:
    return [
        {
            "product_id": row["id"],
            "quantity": int(row["Quantity"]),
            "cost": float(clean_price(row['Wholesale Price']))
        }
        for _, row in product_df.iterrows()
    ]
def add_products_to_stock_order(stock_order_id: str, line_items: list):
    """
    Adds products one-by-one to an existing stock order (consignment) in Lightspeed X-Series.

    """
    # print(line_items)
    if DRY_RUN:
        print(f"[DRY RUN] Would add {len(line_items)} items to stock order {stock_order_id}")
        for line in line_items:
            print(f"  - Product ID: {line['product_id']}, Quantity: {line['quantity']}")
        return {"status": "simulated"}

    url_base = f"{BASE_URL}/consignments/{stock_order_id}/products"
    results = []
    
    for line in line_items:
        payload = {
            "product_id": line["product_id"],
            "count": line["quantity"],
            "cost": line["cost"]
        }

        response = requests.post(url_base, headers=HEADERS, json=payload)
        if response.status_code == 200 or response.status_code == 201:
            results.append(response.json())
        else:
            # print(response.status_code)
            print(f"Error adding product {line['product_id']} to stock order: {response.text}")

    print(f"Added {len(results)} products to stock order {stock_order_id}")
    return results
