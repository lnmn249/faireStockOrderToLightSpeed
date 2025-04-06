import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
import pandas as pd
import os
import threading
import time
from dotenv import load_dotenv
load_dotenv()
from faireOrderFuncs import (
    save_all_products_CSV, get_all_products, build_stock_order_lines,
    add_products_to_stock_order, create_missing_products, combine_product_ids,
    create_stock_order_shell, read_faire_order, read_products_csv,
    match_products_and_find_missing
)

TEMP_PRODUCTS_FILE = "tempProductsFile.csv"

class FaireStockOrderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Faire to Lightspeed Stock Order")
        self.root.geometry("600x450")

        self.label = tk.Label(root, text="Upload Faire Order CSV:")
        self.label.pack(pady=10)

        self.upload_button = tk.Button(root, text="Choose File", command=self.choose_file)
        self.upload_button.pack(pady=5)

        self.run_button = tk.Button(root, text="Run Stock Order Process", command=self.start_process_thread, state=tk.DISABLED)
        self.run_button.pack(pady=10)

        self.progress = ttk.Progressbar(root, mode="indeterminate")
        self.progress.pack(fill=tk.X, padx=10, pady=5)

        self.log_output = scrolledtext.ScrolledText(root, wrap=tk.WORD, height=15)
        self.log_output.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

        self.csv_path = None

    def log(self, message):
        self.log_output.insert(tk.END, f"{message}\n")
        self.log_output.see(tk.END)

    def choose_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if file_path:
            self.csv_path = file_path
            self.log(f"Selected file: {file_path}")
            self.run_button.config(state=tk.NORMAL)

    def start_process_thread(self):
        thread = threading.Thread(target=self.run_process)
        thread.start()

    def run_process(self):
        self.progress.start()
        try:
            OUTLET_ID = os.getenv("OUTLET_ID")
            if not OUTLET_ID:
                raise ValueError("OUTLET_ID environment variable not set.")

            self.log("Saving current Lightspeed products...")

            # Progress bar is running while downloading
            save_all_products_CSV(get_all_products(), TEMP_PRODUCTS_FILE)

            self.log("Reading Faire and Lightspeed product data...")
            faireDF = read_faire_order(self.csv_path)
            productsDF = read_products_csv(TEMP_PRODUCTS_FILE)

            self.log("Matching products...")
            existing_products_df, missing_products_df = match_products_and_find_missing(productsDF, faireDF)

            self.log(f"Found {len(missing_products_df)} missing products. Creating them...")
            created_products = create_missing_products(missing_products_df)

            self.log("Combining matched and newly created product data...")
            combined_df = combine_product_ids(existing_products_df, created_products)

            self.log("Creating stock order shell...")
            stock_order = create_stock_order_shell(location_id=OUTLET_ID, faire_df=faireDF)

            if stock_order and "id" in stock_order:
                stock_order_id = stock_order["id"]
                self.log(f"Stock order created with ID: {stock_order_id}")

                self.log("Building line items...")
                line_items = build_stock_order_lines(combined_df)

                self.log("Adding products to stock order...")
                add_products_to_stock_order(stock_order_id, line_items)

                self.log("✅ Stock order completed successfully.")
            else:
                self.log("❌ Failed to create stock order.")

        except Exception as e:
            self.log(f"❌ Error: {e}")
            messagebox.showerror("Error", str(e))
        finally:
            self.progress.stop()
            # Clean up temp file
            try:
                if os.path.exists(TEMP_PRODUCTS_FILE):
                    os.remove(TEMP_PRODUCTS_FILE)
                    self.log("🧹 Temporary products file deleted.")
            except Exception as cleanup_error:
                self.log(f"⚠️ Could not delete temp file: {cleanup_error}")

if __name__ == "__main__":
    root = tk.Tk()
    app = FaireStockOrderApp(root)
    root.mainloop()
