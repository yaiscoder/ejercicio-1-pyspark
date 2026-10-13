from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# 1. Crear la sesión de Spark
spark = (
    SparkSession.builder
    .appName("AnalisisVentas")
    .getOrCreate()
)

# 2. Leer el archivo CSV
ventas = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/app/sales.csv")
)

ventas.show(5, truncate=False)
ventas.printSchema()

# 3. Validar los campos obligatorios
campos_obligatorios = [
    "order_id",
    "order_date",
    "customer_id",
    "city",
    "product",
    "quantity",
    "unit_price"
]

campos_completos = F.lit(True)

for campo in campos_obligatorios:
    campos_completos = campos_completos & (
        F.col(campo).isNotNull()
        & (F.trim(F.col(campo).cast("string")) != "")
    )

cantidad_valida = F.col("quantity") > 0
precio_valido = F.col("unit_price") > 0

condicion_valida = (campos_completos & cantidad_valida & precio_valido)

# Separar las ventas válidas y rechazadas
ventas_validas = ventas.filter(condicion_valida)

ventas_rechazadas = (
    ventas.filter(~condicion_valida)
    .withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(
                ~campos_completos,
                F.lit("Campos obligatorios vacíos o nulos")
            ),
            F.when(
                campos_completos & (F.col("quantity") <= 0),
                F.lit("Cantidad menor o igual a cero")
            ),
            F.when(
                campos_completos & (F.col("unit_price") <= 0),
                F.lit("Precio unitario menor o igual a cero")
            )
        )
    )
)


# 4. Calcular el ingreso de cada venta válida
ventas_con_ingresos = ventas_validas.withColumn(
    "revenue",
    F.col("quantity") * F.col("unit_price")
)

ventas_con_ingresos.show(5, truncate=False)


# 5. Calcular las métricas generales
total_originales = ventas.count()
total_validas = ventas_validas.count()
total_rechazadas = ventas_rechazadas.count()

ingresos_totales = (
    ventas_con_ingresos
    .agg(F.sum("revenue").alias("total"))
    .first()["total"]
)


print(f"Total de registros: {total_originales}")
print(f"Ventas válidas: {total_validas}")
print(f"Ventas rechazadas: {total_rechazadas}")
print(f"Ingresos totales: {ingresos_totales}")


# 6. Resumen por producto
resumen_productos = (
    ventas_con_ingresos
    .groupBy("product")
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("revenue").alias("total_revenue")
    )
    .orderBy(F.desc("total_revenue"))
)

resumen_productos.show(truncate=False)

# Producto con mayores ingresos
producto_top = resumen_productos.first()

print(
    f"Producto con mayores ingresos: {producto_top['product']} "
    f"({producto_top['total_revenue']})"
)


# 7. Resumen por ciudad
resumen_ciudades = (
    ventas_con_ingresos
    .groupBy("city")
    .agg(
        F.sum("revenue").alias("total_revenue")
    )
    .orderBy(F.desc("total_revenue"))
)


resumen_ciudades.show(truncate=False)

# Ciudad con mayores ingresos
ciudad_top = resumen_ciudades.first()

print(
    f"Ciudad con mayores ingresos: {ciudad_top['city']} "
    f"({ciudad_top['total_revenue']})"
)


# 8. Crear la tabla de métricas generales
metricas_generales = spark.createDataFrame(
    [
        (
            total_originales,
            total_validas,
            total_rechazadas,
            float(ingresos_totales or 0),
            str(producto_top["product"]),
            float(producto_top["total_revenue"]),
            str(ciudad_top["city"]),
            float(ciudad_top["total_revenue"])
        )
    ],
    [
        "total_records",
        "accepted_records",
        "rejected_records",
        "total_revenue",
        "top_product",
        "top_product_revenue",
        "top_city",
        "top_city_revenue"
    ]
)

metricas_generales.show(truncate=False)


# 9. Guardar los resultados como archivos CSV
ruta_salida = "/app/output"

(
    ventas_con_ingresos.write
    .mode("overwrite")
    .option("header", True)
    .csv(f"{ruta_salida}/ventas_validas")
)

(
    ventas_rechazadas.write
    .mode("overwrite")
    .option("header", True)
    .csv(f"{ruta_salida}/ventas_rechazadas")
)

(
    resumen_productos.write
    .mode("overwrite")
    .option("header", True)
    .csv(f"{ruta_salida}/resumen_productos")
)

(
    resumen_ciudades.write
    .mode("overwrite")
    .option("header", True)
    .csv(f"{ruta_salida}/resumen_ciudades")
)

(
    metricas_generales.write
    .mode("overwrite")
    .option("header", True)
    .csv(f"{ruta_salida}/metricas_generales")
)

print("\nResultados guardados en /app/output")


# 10. Cerrar la sesión de Spark
spark.stop()
