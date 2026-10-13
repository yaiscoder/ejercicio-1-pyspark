from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, sum


spark = SparkSession.builder.appName("AnalisisSpark").getOrCreate()

ventas = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("/app/sales.csv")
)

ventas.show(5)
ventas.printSchema()

ventas_validas = ventas.filter(
    col("order_id").isNotNull() &
    col("order_date").isNotNull() &
    col("customer_id").isNotNull() &
    col("city").isNotNull() &
    col("product").isNotNull() &
    col("quantity").isNotNull() &
    col("unit_price").isNotNull() &

    (trim(col("order_date")) != "") &
    (trim(col("customer_id")) != "") &
    (trim(col("city")) != "") &
    (trim(col("product")) != "") &

    (col("quantity") > 0) &
    (col("unit_price") > 0)
)

ventas_invalidas = ventas.filter(
    col("order_id").isNull() |
    col("order_date").isNull() |
    col("customer_id").isNull() |
    col("city").isNull() |
    col("product").isNull() |
    col("quantity").isNull() |
    col("unit_price").isNull() |

    (trim(col("order_date")) == "") |
    (trim(col("customer_id")) == "") |
    (trim(col("city")) == "") |
    (trim(col("product")) == "") |

    (col("quantity") <= 0) |
    (col("unit_price") <= 0)
)

print("Total de ventas:", ventas.count())
print("Ventas válidas:", ventas_validas.count())
print("Ventas inválidas:", ventas_invalidas.count())


ventas_con_ingreso = ventas_validas.withColumn("ingreso", col("quantity") * col("unit_price"))
ventas_con_ingreso.show(5)

# ingreso total de todas las ventas
ventas_con_ingreso.agg(sum("ingreso").alias("ingreso_total")).show()

unidades_totales = ventas_con_ingreso.agg(sum("quantity").alias("unidades_vendidas"))

unidades_totales.show()

#ingreso total POR cada venta
resumen_productos = ventas_con_ingreso.groupBy("product").agg(sum("quantity").alias("unidades_vendidas"),sum("ingreso").alias("ingreso_total"))
resumen_productos.show()

productos_ordenados = resumen_productos.orderBy(col("ingreso_total").desc())
productos_ordenados.show()


resumen_ciudades = ventas_con_ingreso.groupBy("city").agg(sum("ingreso").alias("ingreso_total"))
resumen_ciudades.show()

ciudades_ordenadas = resumen_ciudades.orderBy(col("ingreso_total").desc())
ciudades_ordenadas.show(1)


# Guardar resultados

ventas_validas.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/app/output/ventas_validas")

ventas_invalidas.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("/app/output/ventas_invalidas")


spark.stop()































