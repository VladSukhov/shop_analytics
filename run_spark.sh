spark-submit \
  --master local[*] \
  --conf "spark.driver.extraClassPath=jars/*" \
  --conf "spark.executor.extraClassPath=jars/*" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg.type=hadoop" \
  --conf "spark.sql.catalog.iceberg.warehouse=s3a://iceberg/warehouse" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://localhost:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.secret.key=minioadmin" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  streaming/purchase_processor.py