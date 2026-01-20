import logging
import json
from datetime import datetime

from utils.data_processor import (
    calculate_total_revenue,
    region_wise_sale,
    top_selling_products,
    customer_analysis,
    daily_sales_trend,
    find_peak_sales_day,
    low_performing_products,
)

from utils.api_handler import (
    fetch_all_products,
    create_product_mapping,
    enrich_sales_data,
)

def open_with_fallback_encodings(path, encodings=("utf-8", "latin-1", "cp1252")):
    for enc in encodings:
        try:
            return open(path, "r", encoding=enc)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("utf-8", b"", 0, 1, "Unable to decode file")

def safe_to_int(val):
    try:
        return int(val.replace(",", ""))
    except (ValueError, AttributeError):
        return 0

def handleQuestionOne():
    try:
        total_records = 0
        invalid_records = 0
        INPUT_FILE = './data/sales_data.txt'
        OUTPUT_FILE = './output/first_question.txt'
        with open_with_fallback_encodings(INPUT_FILE) as infile, open(OUTPUT_FILE, 'w') as outfile:
            header = infile.readline().strip()
            outfile.write(header + "\n")
            for line in infile:
                row = line.strip()

                if not row:
                    continue
                total_records +=1

                row = row.split('|')
                if not row[6].startswith('C'):
                    invalid_records +=1
                    continue
                if int(row[4]) < 0 or safe_to_int(row[5]) < 0:
                    invalid_records +=1
                    continue
                if not row[0].startswith('T'):
                    invalid_records +=1
                    continue
                outfile.write("|".join(row) + "\n")
            print(f'Total records passed: {total_records}')
            print(f'Invalid records removed: {invalid_records}')
            print(f'Valid records after cleaning: {total_records - invalid_records}')

    except FileNotFoundError as e:
        logging.error(f"Input file not found: {e.filename}")
        print(f"Error: Input file not found -> {e.filename}")

    except PermissionError as e:
        logging.error("Permission denied", exc_info=e)
        print("Error: Permission denied while accessing files.")

    except Exception as e:
        logging.error("First Question error", exc_info=e)

    return

# Q2 TASK 1.1
def read_sale_data():
    try:
        out = []
        INPUT_FILE = './output/first_question.txt'
        with open_with_fallback_encodings(INPUT_FILE) as infile:
            _ = infile.readline().strip()
            for line in infile:
                row = line.strip()
                if not row:
                    continue
                out.append(row)

        print(json.dumps(out, indent=4))

    except FileNotFoundError as e:
        logging.error(f"Input file not found: {e.filename}")
        print(f"Error: Input file not found -> {e.filename}")

    except PermissionError as e:
        logging.error("Permission denied", exc_info=e)
        print("Error: Permission denied while accessing files.")

    except Exception as e:
        logging.error("First Question error", exc_info=e)
    return

# Q2 TASK 1.2
def parse_transactions():
    try:
        out = []
        INPUT_FILE = "./output/first_question.txt"
        with open_with_fallback_encodings(INPUT_FILE) as infile:
            headings = infile.readline().strip().split("|")
            for line in infile:
                row = line.strip()
                if not row:
                    continue
                row_data = row.split("|")
                json_data = {}
                for index, r in enumerate(row_data):
                    if headings[index] == "Quantity":
                        json_data[headings[index]] = int(r)
                    elif headings[index] == "Date":
                        json_data[headings[index]] = datetime.strptime(
                            r, "%Y-%m-%d"
                        ).date().isoformat()
                    elif headings[index] == "ProductName":
                        json_data[headings[index]] = r.replace(",", " ")
                    elif headings[index] == "UnitPrice":
                        json_data[headings[index]] = float(safe_to_int(r))
                    else:
                        json_data[headings[index]] = r

                out.append(json_data)

        # print(json.dumps(out, indent=4))
        return json.dumps(out, indent=4)

    except FileNotFoundError as e:
        logging.error(f"Input file not found: {e.filename}")
        print(f"Error: Input file not found -> {e.filename}")

    except PermissionError as e:
        logging.error("Permission denied", exc_info=e)
        print("Error: Permission denied while accessing files.")

    except Exception as e:
        logging.error("First Question error", exc_info=e)
    return

def validate_and_filter(transactions, region=None, min_amount=None, max_amount=None):
    transactions = json.loads(transactions)
    filtered_by_region = 0
    filtered_by_amount = 0
    valid_transaction, invalid_count, filter_summary = (
        [],
        0,
        {
            "total_input": len(transactions),
            "invalid": 0,
            "filtered_by_region": 0,
            "filtered_by_amount": 0,
            "final_count": 0,
        },
    )
    for transaction in transactions:
        if transaction.get("Quantity", 0) <= 0 or transaction.get("UnitPrice", 0) <= 0:
            invalid_count += 1
            continue

        amount = transaction.get("Quantity", 0) * transaction.get("UnitPrice", 0)

        if region and region != transaction.get("Region"):
            filtered_by_region += 1
            continue

        if min_amount is not None and amount < min_amount:
            filtered_by_amount += 1
            continue

        if max_amount is not None and amount > max_amount:
            filtered_by_amount += 1
            continue

        valid_transaction.append(transaction)

    filter_summary["invalid"] = invalid_count
    filter_summary["filtered_by_region"] = filtered_by_region
    filter_summary["filtered_by_amount"] = filtered_by_amount
    filter_summary["final_count"] = len(valid_transaction)

    return (valid_transaction, invalid_count, filter_summary)

def format_currency(value):
    return f"₹{value:,.2f}"

def truncate(text, width):
    return text if len(text) <= width else text[:width-3] + "..."


def generate_sales_report(transactions, enriched_transactions, output_file="output/sales_report.txt"):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    transactions = json.loads(transactions)
    total_records = len(transactions)

    dates = sorted(t["Date"] for t in transactions)
    date_range = f"{dates[0]} to {dates[-1]}"

    transactions = json.dumps(transactions)

    total_revenue = calculate_total_revenue(transactions)
    total_transactions = len(transactions)
    avg_order_value = total_revenue / total_transactions if total_transactions else 0

    region_sale = json.loads(region_wise_sale(transactions))
    top_5_prods = top_selling_products(transactions)
    top_5_customers = json.loads(customer_analysis(transactions))
    daily_sales = json.loads(daily_sales_trend(parse_transactions())) #Q3 TASK 2.2 (a)
    peak_sale_days = find_peak_sales_day(parse_transactions()) #Q3 TASK 2.2 (b)
    low_performing_prods = low_performing_products(parse_transactions(), 6) #Q3 TASK 2.3 (a)

    enriched = [t for t in enriched_transactions if t.get("API_Match")]
    not_enriched = [t["ProductID"] for t in enriched_transactions if not t.get("API_Match")]

    success_rate = (len(enriched) / len(enriched_transactions) * 100) if enriched_transactions else 0


    with open(output_file, "w") as f:
        f.write("=" * 60 + "\n")
        f.write("SALES ANALYTICS REPORT\n")
        f.write(f"Generated: {now}\n")
        f.write(f"Records Processed: {total_records}\n")
        f.write("=" * 60 + "\n\n")

        f.write("OVERALL SUMMARY\n")
        f.write("-" * 60 + "\n")
        f.write(f"Total Revenue: {format_currency(total_revenue)}\n")
        f.write(f"Total Transactions: {total_transactions}\n")
        f.write(f"Average Order Value: {format_currency(avg_order_value)}\n")
        f.write(f"Date Range: {date_range}\n\n")

        f.write("REGION-WISE PERFORMANCE\n")
        f.write("-" * 60 + "\n")
        f.write(f"{'Region':<10}{'Sales':>15}{'% of Total':>14}{'Txns':>8}\n")
        for key, val in region_sale.items():
            f.write(
            f"{key:<10}"
            f"{format_currency(val['total_sales']):>15}"
            f"{val['percentage']:>11.2f}%"
            f"{val['transaction_count']:>8}\n"
        )
        f.write("\n")

        f.write("TOP 5 PRODUCTS\n")
        f.write("-" * 60 + "\n")
        f.write(f"{'Rank':<6}{'Product':<20}{'Qty Sold':<12}{'Revenue'}\n")
        for i, (prod_name, qty, amount) in enumerate(top_5_prods, 1):
            f.write(f"{i:<6}{prod_name:<20}{qty:<12}{format_currency(amount)}\n")
        f.write("\n")

        f.write("TOP 5 CUSTOMERS\n")
        f.write("-" * 60 + "\n")
        f.write(
            f"{'CustomerID':<12}"
            f"{'Products Bought':<35}"
            f"{'Total Spent':>15}"
            f"{'Orders':>10}"
            f"{'Avg Order':>15}\n"
        )
        for key, value in top_5_customers.items():
            products = ", ".join(value.get("products_bought"))
            products = truncate(products, 35)
            # f.write(
            #     f"{key:<6}{(', ').join(value.get('products_bought')):<15}{format_currency(value.get('total_spent')):<15}{value.get('purchase_count')}{value.get('avg_order_value')}\n"
            # )
            f.write(
            f"{key:<12}"
            f"{products:<35}"
            f"{format_currency(value['total_spent']):>15}"
            f"{value['purchase_count']:>8}"
            f"{format_currency(value['avg_order_value']):>18}\n"
        )
        f.write("\n")

        f.write("DAILY SALES TREND\n")
        f.write("-" * 60 + "\n")
        f.write(f"{'Date':<12}{'Revenue':>15}{'Txns':>10}{'Customers':>13}\n")
        for key, value in daily_sales.items():
            f.write(
                f"{key:<12}{format_currency(value.get('revenue')):>15}"
                f"{value.get('transaction_count'):>8}{value.get('unique_customers'):>10}\n"
            )
        f.write("\n")

        f.write("PEAK SALES DAYS \n")
        f.write("-" * 60 + "\n")
        f.write(f"Best Selling Day: {peak_sale_days[0]}\n")
        # f.write(f"Low Performing Products: {(', ').join([ p[0] for p in low_performing_prods]) if low_performing_prods else 'None'}\n")
        low_names = [p[0] for p in low_performing_prods]
        f.write(
            "Low Performing Products: " +
            (", ".join(low_names) if low_names else "None") +
            "\n"
        )
        for r, v in region_sale.items():
            f.write(
                f"Average Transaction Value ({r}):"
                f"{format_currency((v.get('total_sales') / v.get('transaction_count')) / 100 if v.get('transaction_count') > 0 else 0)}\n"
            )
        f.write("\n")

        f.write("API ENRICHMENT SUMMARY\n")
        f.write("-" * 60 + "\n")
        f.write(f"Total Products Enriched: {len(enriched)}\n")
        f.write(f"Success Rate: {success_rate:.2f}%\n")
        # f.write("Products Not Enriched: " + (", ".join(not_enriched) if not_enriched else "None") + "\n")
        unique_not_enriched = sorted(set(not_enriched))
        f.write(
            "Products Not Enriched: " +
            (", ".join(unique_not_enriched) if unique_not_enriched else "None") +
            "\n"
        )

    return


def main():
    # uncomment each line to run required questions

    # handleQuestionOne() #Q1
    # read_sale_data() #Q2 TASK 1.1
    # print(parse_transactions()) #Q2 TASK 1.2
    # print(validate_and_filter(parse_transactions(), "North", 0, 500))  # Q2 TASK 1.3


    # print(calculate_total_revenue(parse_transactions())) #Q3 TASK 2.1 (a)
    # print(region_wise_sale(parse_transactions())) #Q3 TASK 2.1 (b)
    # print(top_selling_products(parse_transactions(), 10)) #Q3 TASK 2.1 (c)
    # print(customer_analysis(parse_transactions())) #Q3 TASK 2.1 (d)

    # print(daily_sales_trend(parse_transactions())) #Q3 TASK 2.2 (a)
    # print(find_peak_sales_day(parse_transactions())) #Q3 TASK 2.2 (b)

    # print(low_performing_products(parse_transactions(), 6)) #Q3 TASK 2.3 (a)

    # print(fetch_all_products(1)) #Q3 TASK 3.1
    # print(create_product_mapping(fetch_all_products(1)))
    # print(enrich_sales_data(parse_transactions(), create_product_mapping(fetch_all_products(100))))
    generate_sales_report(
        parse_transactions(),
        json.loads(enrich_sales_data(
            parse_transactions(), create_product_mapping(fetch_all_products(100))
        )),
    )

if __name__ == "__main__":
    main()

