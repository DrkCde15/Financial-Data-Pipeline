"""Generate small synthetic datasets (CLEARLY fictitious data).

Tables:
  - branches.csv    (10 rows)
  - products.csv    (6 rows)
  - customers.csv   (50 rows)
  - accounts.json   (60 rows)
  - transactions.json (200 rows + 2 intentional outliers for future analysis)

Reproducible via SYNTHETIC_SEED. Uses only the standard library.
Idempotent: overwrites files in <project>/data/raw.
"""

from __future__ import annotations

import csv
import json
import random
import sys
from pathlib import Path

# Allow `python scripts/generate_synthetic_data.py` without install.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from financial_pipeline.config import load_settings, setup_logging

logger = setup_logging()

FIRST_NAMES = [
    "Ana", "Bruno", "Carla", "Diego", "Elisa", "Fabio", "Gabi", "Heitor",
    "Igor", "Julia", "Kaique", "Larissa", "Marcos", "Nadia", "Otavio", "Paula",
]
LAST_NAMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Costa", "Pereira", "Almeida",
    "Carvalho", "Ferreira", "Rodrigues", "Gomes", "Martins",
]
CITIES = [
    ("Sao Paulo", "SP"), ("Campinas", "SP"), ("Rio de Janeiro", "RJ"),
    ("Belo Horizonte", "MG"), ("Curitiba", "PR"), ("Porto Alegre", "RS"),
    ("Salvador", "BA"), ("Recife", "PE"), ("Fortaleza", "CE"), ("Goiania", "GO"),
]


def _rng(seed: int) -> random.Random:
    return random.Random(seed)


def generate_branches() -> list[dict]:
    """Return 10 fictitious branches."""
    return [
        {
            "branch_id": f"B{str(i + 1).zfill(3)}",
            "branch_name": f"Agencia Ficticia {city}",
            "city": city,
            "state": state,
        }
        for i, (city, state) in enumerate(CITIES)
    ]


def generate_products() -> list[dict]:
    """Return 6 fictitious financial products."""
    return [
        {"product_id": "P001", "product_name": "Conta Corrente Simples", "product_type": "account"},
        {"product_id": "P002", "product_name": "Conta Poupanca", "product_type": "savings"},
        {"product_id": "P003", "product_name": "Cartao de Credito Basico", "product_type": "card"},
        {"product_id": "P004", "product_name": "Emprestimo Pessoal", "product_type": "loan"},
        {"product_id": "P005", "product_name": "Investimento CDB", "product_type": "investment"},
        {"product_id": "P006", "product_name": "Seguro de Vida", "product_type": "insurance"},
    ]


def generate_customers(rng: random.Random, branches: list[dict], n: int = 50) -> list[dict]:
    """Return n fictitious customers distributed across branches."""
    customers = []
    for i in range(1, n + 1):
        branch = rng.choice(branches)
        customers.append(
            {
                "customer_id": f"C{str(i).zfill(4)}",
                "full_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
                "birth_date": f"{rng.randint(1960, 2003)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}",
                "branch_id": branch["branch_id"],
                "is_active": rng.choices([True, False], weights=[85, 15])[0],
            }
        )
    return customers


def generate_accounts(
    rng: random.Random,
    customers: list[dict],
    branches: dict[str, dict],
    products: list[dict],
) -> list[dict]:
    """Return ~60 fictitious accounts (at least one per first 50 customers)."""
    account_products = [p for p in products if p["product_type"] in ("account", "savings")]
    accounts = []
    for i in range(1, 61):
        customer = customers[(i - 1) % len(customers)]
        product = rng.choice(account_products)
        accounts.append(
            {
                "account_id": f"A{str(i).zfill(5)}",
                "customer_id": customer["customer_id"],
                "branch_id": customer["branch_id"],
                "product_id": product["product_id"],
                "open_date": f"2023-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}",
                "status": rng.choices(["active", "inactive"], weights=[90, 10])[0],
            }
        )
    return accounts


def generate_transactions(rng: random.Random, accounts: list[dict], n: int = 200) -> list[dict]:
    """Return n fictitious transactions + 2 obvious outliers for future study."""
    types = ["deposit", "withdrawal", "transfer_in", "transfer_out", "payment", "fee"]
    active_accounts = [a["account_id"] for a in accounts if a["status"] == "active"]
    txs: list[dict] = []
    for i in range(1, n + 1):
        txs.append(
            {
                "transaction_id": f"T{str(i).zfill(6)}",
                "account_id": rng.choice(active_accounts),
                "transaction_type": rng.choice(types),
                "amount": round(rng.uniform(10.0, 5000.0), 2),
                "timestamp": f"2024-06-{rng.randint(1, 28):02d}T{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00",
                "status": rng.choices(
                    ["completed", "pending", "failed"], weights=[92, 5, 3]
                )[0],
            }
        )
    # Intentional outliers (clearly anomalous, for Silver/Gold outlier exercises later)
    txs.append(
        {
            "transaction_id": "T999991",
            "account_id": active_accounts[0],
            "transaction_type": "deposit",
            "amount": 999999.99,
            "timestamp": "2024-06-15T10:00:00",
            "status": "completed",
        }
    )
    txs.append(
        {
            "transaction_id": "T999992",
            "account_id": active_accounts[1],
            "transaction_type": "withdrawal",
            "amount": -50.00,
            "timestamp": "2024-06-15T11:00:00",
            "status": "failed",
        }
    )
    return txs


def write_csv(path: Path, rows: list[dict]) -> None:
    """Write rows to CSV (creates parent dirs)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list[dict]) -> None:
    """Write rows to pretty JSON array (creates parent dirs)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, ensure_ascii=False)


def main() -> None:
    """Generate all synthetic raw files."""
    settings = load_settings()
    rng = _rng(settings.synthetic_seed)
    raw_dir = settings.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    branches = generate_branches()
    products = generate_products()
    customers = generate_customers(rng, branches)
    branch_by_id = {b["branch_id"]: b for b in branches}
    accounts = generate_accounts(rng, customers, branch_by_id, products)
    transactions = generate_transactions(rng, accounts)

    write_csv(raw_dir / "branches.csv", branches)
    write_csv(raw_dir / "products.csv", products)
    write_csv(raw_dir / "customers.csv", customers)
    write_json(raw_dir / "accounts.json", accounts)
    write_json(raw_dir / "transactions.json", transactions)

    logger.info(
        "Synthetic data written to %s "
        "(branches=%d, products=%d, customers=%d, accounts=%d, transactions=%d)",
        raw_dir, len(branches), len(products), len(customers),
        len(accounts), len(transactions),
    )


if __name__ == "__main__":
    main()
