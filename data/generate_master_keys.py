import json
import random
from faker import Faker
from pathlib import Path

fake = Faker()

# Define number of master records
NUM_CUSTOMERS = 10000
NUM_PRODUCTS = 5000
NUM_SUPPLIERS = 500

# Define output path
BASE_DIR = Path("./master_keys")
BASE_DIR.mkdir(parents=True, exist_ok=True)

# 1. Generate Suppliers
suppliers = []
for i in range(1, NUM_SUPPLIERS + 1):
    supplier = {
        "supplier_id": f"SUPP{i:04d}",
        "name": fake.company(),
        "contact_email": fake.company_email(),
        "country": fake.country_code()
    }
    suppliers.append(supplier)

with open(BASE_DIR / "suppliers.json", "w") as f:
    json.dump(suppliers, f, indent=2)

# 2. Generate Products
products = []
for i in range(1, NUM_PRODUCTS + 1):
    product = {
        "product_id": f"PROD{i:05d}",
        "name": fake.word().title(),
        "category": fake.random_element(["Electronics", "Clothing", "Home", "Grocery", "Books"]),
        "price": round(random.uniform(5, 5000), 2),
        "supplier_id": random.choice(suppliers)["supplier_id"]
    }
    products.append(product)

with open(BASE_DIR / "products.json", "w") as f:
    json.dump(products, f, indent=2)

# 3. Generate Customers
customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    customer = {
        "customer_id": f"CUST{i:05d}",
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "country": fake.country_code(),
        "signup_date": fake.date_between(start_date="-3y", end_date="today").isoformat()
    }
    customers.append(customer)

with open(BASE_DIR / "customers.json", "w") as f:
    json.dump(customers, f, indent=2)

print("✅ Master key files generated:")
print("- suppliers.json")
print("- products.json")
print("- customers.json")
