import json
import logging
from collections import defaultdict


def calculate_total_revenue(transactions):
    transactions = json.loads(transactions)
    return sum([txn["Quantity"] * txn["UnitPrice"] for txn in transactions])

def region_wise_sale(transactions):
    out = {
        "North": {"transaction_count": 0, "total_sales": 0.0},
        "South": {"transaction_count": 0, "total_sales": 0.0},
        "West": {"transaction_count": 0, "total_sales": 0.0},
        "East": {"transaction_count": 0, "total_sales": 0.0},
    }
    try:
        total_sales = calculate_total_revenue(transactions)
        transactions = json.loads(transactions)
        for txn in transactions:
            if not txn["Region"]:
                continue
            sale_amount = txn["Quantity"] * txn["UnitPrice"]
            out[txn["Region"]]["transaction_count"] += 1
            out[txn["Region"]]["total_sales"] += sale_amount

        # to calculate percentage
        for region in list(out):
            out[region]["percentage"] = round(
                out[region]["total_sales"] / total_sales * 100, 2
            )

        # to sort
        sorted_regions = dict(
            sorted(out.items(), key=lambda item: item[1]["total_sales"], reverse=True)
        )

        return json.dumps(sorted_regions, indent=4)
    except Exception as e:
        logging.error('error', exc_info=e)
        return out


def top_selling_products(transactions, n=5):
    """
    Finds top n products by total quantity sold

    Returns: list of tuples
    """
    try:
        transactions = json.loads(transactions)
        # return json.dumps(transactions,indent=4)

        product_data = defaultdict(lambda: {"total_quantity": 0, "total_revenue": 0.0})

        for txn in transactions:
            product = txn.get("ProductName", "").strip()
            quantity = txn.get("Quantity", 0)
            unit_price = txn.get("UnitPrice", 0)

            if not product:
                continue

            product_data[product]["total_quantity"] += quantity
            product_data[product]["total_revenue"] += quantity * unit_price

        result = [
            (product,
             data["total_quantity"],
             data["total_revenue"])
            for product, data in product_data.items()
        ]

        # Sort by total quantity sold (descending) and return top n
        result.sort(key=lambda x: x[1], reverse=True)
        return result[:n]

    except Exception as e:
        logging.error("error", exc_info=e)
        return []

def customer_analysis(transactions):
    try:
        transactions = json.loads(transactions)

        customers = {}

        for txn in transactions:
            customer_id = txn.get("CustomerID", "").strip()
            product = txn.get("ProductName", "").strip()
            quantity = txn.get("Quantity", 0)
            unit_price = txn.get("UnitPrice", 0)

            if not customer_id:
                continue

            order_value = quantity * unit_price

            if customer_id not in customers:
                customers[customer_id] = {
                    "total_spent": 0.0,
                    "purchase_count": 0,
                    "products_bought": set()
                }

            customers[customer_id]["total_spent"] += order_value
            customers[customer_id]["purchase_count"] += 1

            if product:
                customers[customer_id]["products_bought"].add(product)

        for customer_id, data in customers.items():
            purchase_count = data["purchase_count"]

            data["avg_order_value"] = round(
                data["total_spent"] / purchase_count, 2
            ) if purchase_count else 0.0

            data["products_bought"] = list(set(data["products_bought"]))

        sorted_customers = dict(
            sorted(
                customers.items(), key=lambda item: item[1]["total_spent"], reverse=True
            )
        )

        return json.dumps(sorted_customers, indent=4)

    except Exception as e:
        logging.error("error", exc_info=e)
        return {}

def daily_sales_trend(transactions):
    try:
        transactions = json.loads(transactions)
        daily_data = {}

        for txn in transactions:
            date = txn.get("Date", "").strip()
            customer_id = txn.get("CustomerID", "").strip()
            quantity = txn.get("Quantity", 0)
            unit_price = txn.get("UnitPrice", 0)

            if not date:
                continue

            revenue = quantity * unit_price

            if date not in daily_data:
                daily_data[date] = {
                    "revenue": 0.0,
                    "transaction_count": 0,
                    "unique_customers": set()
                }

            daily_data[date]["revenue"] += revenue
            daily_data[date]["transaction_count"] += 1

            if customer_id:
                daily_data[date]["unique_customers"].add(customer_id)

        for date, data in daily_data.items():
            data["unique_customers"] = len(data["unique_customers"])

        sorted_daily_data = dict(
            sorted(daily_data.items(), key=lambda item: item[0])
        )

        return json.dumps(sorted_daily_data, indent=4)

    except Exception as e:
        logging.error("error", exc_info=e)
        return {}

def find_peak_sales_day(transactions):
    transactions = json.loads(transactions)
    revenue_by_date = defaultdict(float)
    count_by_date = defaultdict(int)

    for txn in transactions:
        date = txn["Date"]
        amount = float(txn["Quantity"] * txn["UnitPrice"])
        revenue_by_date[date] += amount
        count_by_date[date] += 1

    peak_date = max(revenue_by_date, key=lambda d: revenue_by_date[d])
    return (peak_date, revenue_by_date[peak_date], count_by_date[peak_date])

def low_performing_products(transactions, threshold=10):
    transactions = json.loads(transactions)
    product_stats = defaultdict(lambda: {'quantity': 0, 'revenue': 0.0})
    for t in transactions:
        name = t["ProductName"]
        qty = t["Quantity"]
        revenue = float(t["Quantity"] * t["UnitPrice"])
        product_stats[name]["quantity"] += qty
        product_stats[name]["revenue"] += revenue

    low_products = [
        (name, stats["quantity"], stats["revenue"])
        for name, stats in product_stats.items()
        if stats["quantity"] < threshold
    ]
    low_products.sort(key=lambda x: x[1])
    return low_products
