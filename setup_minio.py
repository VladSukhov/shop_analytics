from minio import Minio
import json
import os
import pandas as pd
from io import BytesIO

# Настройка клиента MinIO
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Создание бакетов
buckets = ["analytics", "iceberg"]
for bucket in buckets:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' created")
    else:
        print(f"Bucket '{bucket}' already exists")

# Создание тестовых данных для продуктов
products_data = [
    {"product_id": "p1", "product_name": "Laptop", "product_category": "Electronics", "product_price": 1200.00},
    {"product_id": "p2", "product_name": "Smartphone", "product_category": "Electronics", "product_price": 800.00},
    {"product_id": "p3", "product_name": "Headphones", "product_category": "Accessories", "product_price": 150.00},
    {"product_id": "p4", "product_name": "T-shirt", "product_category": "Clothing", "product_price": 25.00},
    {"product_id": "p5", "product_name": "Jeans", "product_category": "Clothing", "product_price": 60.00},
    {"product_id": "p6", "product_name": "Book", "product_category": "Books", "product_price": 15.00},
    {"product_id": "p7", "product_name": "Coffee", "product_category": "Groceries", "product_price": 10.00},
    {"product_id": "p8", "product_name": "Bread", "product_category": "Groceries", "product_price": 3.00}
]

# Создание тестовых данных для клиентов
customers_data = [
    {"customer_id": "c1", "customer_name": "John Doe", "customer_region": "North", "customer_segment": "Premium"},
    {"customer_id": "c2", "customer_name": "Jane Smith", "customer_region": "South", "customer_segment": "Standard"},
    {"customer_id": "c3", "customer_name": "Bob Johnson", "customer_region": "East", "customer_segment": "Premium"},
    {"customer_id": "c4", "customer_name": "Alice Brown", "customer_region": "West", "customer_segment": "Standard"},
    {"customer_id": "c5", "customer_name": "Charlie Davis", "customer_region": "North", "customer_segment": "Standard"}
]

# Сохранение данных в формате Parquet
products_df = pd.DataFrame(products_data)
customers_df = pd.DataFrame(customers_data)

# Преобразование в Parquet и загрузка в MinIO
products_parquet = BytesIO()
products_df.to_parquet(products_parquet)
products_parquet.seek(0)
client.put_object("iceberg", "products.parquet", products_parquet, length=products_parquet.getbuffer().nbytes)
print("Products data uploaded")

customers_parquet = BytesIO()
customers_df.to_parquet(customers_parquet)
customers_parquet.seek(0)
client.put_object("iceberg", "customers.parquet", customers_parquet, length=customers_parquet.getbuffer().nbytes)
print("Customers data uploaded")

# Создание директории для чекпоинтов
checkpoint_dir = "analytics/checkpoints/purchase_processor"
client.put_object("analytics", "checkpoints/purchase_processor/.keep", BytesIO(b""), 0)
print(f"Checkpoint directory created: {checkpoint_dir}")

print("MinIO setup completed")