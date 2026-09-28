"""Expected raw schemas (minimal contract for Bronze validation).

Bronze does NOT clean data — it only checks that required columns exist
before copying to the lake. Cleaning/dedup/typing belongs to Silver (future).
"""

EXPECTED_COLUMNS: dict[str, list[str]] = {
    "branches": ["branch_id", "branch_name", "city", "state"],
    "products": ["product_id", "product_name", "product_type"],
    "customers": ["customer_id", "full_name", "birth_date", "branch_id", "is_active"],
    "accounts": ["account_id", "customer_id", "branch_id", "product_id", "open_date", "status"],
    "transactions": [
        "transaction_id",
        "account_id",
        "transaction_type",
        "amount",
        "timestamp",
        "status",
    ],
}

# Maps bronze table -> raw source filename (simulated API/CSV/JSON landing).
SOURCE_FILES: dict[str, str] = {
    "branches": "branches.csv",
    "products": "products.csv",
    "customers": "customers.csv",
    "accounts": "accounts.json",
    "transactions": "transactions.json",
}
