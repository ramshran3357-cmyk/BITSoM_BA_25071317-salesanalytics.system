import requests
import json
import logging
from urllib.parse import urlencode, urlunparse, urlparse

BASE_URL = "https://dummyjson.com/"

# helper function
def build_url(base_url, path, args_dict):
    url_parts = list(urlparse(base_url))
    url_parts[2] = path
    url_parts[4] = urlencode(args_dict)
    return urlunparse(url_parts)

def save_enriched_data(enriched_transactions, filename="data/enriched_sales_data.txt"):
    if not enriched_transactions:
        return

    headers = [
        "TransactionID", "Date", "ProductID", "ProductName",
        "Quantity", "UnitPrice", "CustomerID", "Region",
        "API_Category", "API_Brand", "API_Rating", "API_Match"
    ]

    with open(filename, "w") as f:
        f.write("|".join(headers) + "\n")

        for tx in enriched_transactions:
            row = [
                str(tx.get("TransactionID", "")),
                str(tx.get("Date", "")),
                str(tx.get("ProductID", "")),
                str(tx.get("ProductName", "")),
                str(tx.get("Quantity", "")),
                str(tx.get("UnitPrice", "")),
                str(tx.get("CustomerID", "")),
                str(tx.get("Region", "")),
                str(tx.get("API_Category", "")),
                str(tx.get("API_Brand", "")),
                str(tx.get("API_Rating", "")),
                str(tx.get("API_Match", "")),
            ]
            f.write("|".join(row) + "\n")


def fetch_all_products(n=100):
    try:
        url = build_url(BASE_URL, "products", {"limit": n})
        response = requests.get(url, timeout=5)
        response.raise_for_status()

        data = response.json()
        products = data.get("products", [])
        result = []
        for p in products:
            result.append({
                "id": p.get("id"),
                "title": p.get("title"),
                "category": p.get("category"),
                "brand": p.get("brand"),
                "price": p.get("price"),
                "rating": p.get("rating"),
            })
        print("Successfully Fetched products!")
        return json.dumps(result, indent=4)
    except Exception as e:
        logging.error("Error while fetching all products:", exc_info=e)
        return []


def create_product_mapping(api_products):
    api_products = json.loads(api_products)
    result = {}
    for p in api_products:
        result[int(p.get("id"))] = {
            "id": p.get("id"),
            "title": p.get("title"),
            "category": p.get("category"),
            "brand": p.get("brand"),
            "price": p.get("price"),
            "rating": p.get("rating"),
        }

    return result



def enrich_sales_data(transactions, product_mapping):
    transactions = json.loads(transactions)
    enriched_transactions = []

    for tx in transactions:
        enriched = tx.copy()

        try:
            product_id_str = tx.get("ProductID", "")
            numeric_id = int("".join(filter(str.isdigit, product_id_str)))

            api_product = product_mapping.get(numeric_id)

            if api_product:
                enriched["API_Category"] = api_product.get("category")
                enriched["API_Brand"] = api_product.get("brand")
                enriched["API_Rating"] = api_product.get("rating")
                enriched["API_Match"] = True
            else:
                enriched["API_Category"] = None
                enriched["API_Brand"] = None
                enriched["API_Rating"] = None
                enriched["API_Match"] = False

        except Exception:
            enriched["API_Category"] = None
            enriched["API_Brand"] = None
            enriched["API_Rating"] = None
            enriched["API_Match"] = False

        enriched_transactions.append(enriched)

    # Save to file as required
    save_enriched_data(enriched_transactions)
    return json.dumps(enriched_transactions, indent=4)
