import os
import sys
os.environ["SPARK_HOME"] = "/home/talentum/spark"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3.6" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.7-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.functions import col,when,create_map, lit,regexp_replace
from itertools import chain
import pyspark.sql.functions as F

# Extraction of data from csv files and from mysql RDBMS

class Ingest:

    url = "jdbc:mysql://127.0.0.1:3306/test?useSSL=false&allowPublicKeyRetrieval=true"
    driver = "com.mysql.jdbc.Driver"
    user = "bigdata"
    password = "Bigdata@123"




    def __init__(self, spark):
        # A class level variable
        self.spark = spark

    def ingest_table_customer(self):
        print("Ingesting from customer table")
        df_customers1 =  self.spark.read\
            .format("jdbc")\
            .option("driver", self.driver)\
            .option("url", self.url)\
            .option("user", self.user)\
            .option("password", self.password)\
            .option("dbtable", "project.customers")\
            .load()

        df_customers=df_customers1.exceptAll(df_customers1.limit(1))

        return df_customers

    def ingest_table_payment(self):
        print("Ingesting from payment table")
        df_payments1 =  self.spark.read\
            .format("jdbc")\
            .option("driver", self.driver)\
            .option("url", self.url)\
            .option("user", self.user)\
            .option("password", self.password)\
            .option("dbtable", "project.payments")\
            .load()

        df_payments=df_payments1.exceptAll(df_payments1.limit(1))
        return df_payments

    def ingest_orders(self):
        print("Ingesting from orders.csv")
        df_orders = self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("file:///home/talentum/project/orders.csv") 

        return df_orders


    def ingest_order_items(self):
        print("Ingesting from order_items.csv")
        df_order_items = self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("file:///home/talentum/project/order_items.csv") 

        return df_order_items

    
    def ingest_products(self):
        print("Ingesting from products.csv")
        df_products = self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load("file:///home/talentum/project/products.csv") 

        return df_products



# Transformation  

class Transform:

    dictionary={'SP': 'São Paulo',
        'SC': 'Santa Catarina',
        'MG': 'Minas Gerais',
        'PR': 'Paraná',
        'RJ': 'Rio de Janeiro',
        'RS': 'Rio Grande do Sul',
        'PA': 'Pará',
        'GO': 'Goiás',
        'ES': 'Espírito Santo',
        'BA': 'Bahia',
        'MA': 'Maranhão',
        'MS': 'Mato Grosso do Sul',
        'CE': 'Ceará',
        'DF': 'Distrito Federal',
        'RN': 'Rio Grande do Norte',
        'PE': 'Pernambuco',
        'MT': 'Mato Grosso',
        'AM': 'Amazonas',
        'AP': 'Amapá',
        'AL': 'Alagoas',
        'RO': 'Rondônia',
        'PB': 'Paraíba',
        'TO': 'Tocantins',
        'PI': 'Piauí',
        'AC': 'Acre',
        'SE': 'Sergipe',
        'RR': 'Roraima'}

    dictionary_product={
        "perfumery": "Health and Beauty",
        "art": "Art",
        "sports_leisure": "Sports and Leisure",
        "baby": "Baby",
        "housewares": "Housewares",
        "musical_instruments": "Musical Instruments",
        "cool_stuff": "Cool Stuff",
        "furniture_decor": "Furniture",
        "home_appliances": "Home Appliances",
        "toys": "Toys",
        "bed_bath_table": "Homewares",
        "construction_tools_safety": "Construction Tools",
        "computers_accessories": "Electronics",
        "health_beauty": "Health and Beauty",
        "luggage_accessories": "Travel",
        "garden_tools": "Garden",
        "office_furniture": "Furniture",
        "auto": "Automotive",
        "electronics": "Electronics",
        "fashion_shoes": "Fashion",
        "telephony": "Telephony",
        "stationery": "Stationery",
        "fashion_bags_accessories": "Fashion",
        "computers": "Electronics",
        "home_construction": "Home Improvement",
        "watches_gifts": "Gifts",
        "construction_tools_construction": "Construction Tools",
        "pet_shop": "Pets",
        "small_appliances": "Home Appliances",
        "agro_industry_and_commerce": "Business",
        "NA": "Others",
        "furniture_living_room": "Furniture",
        "signaling_and_security": "Security",
        "air_conditioning": "Home Appliances",
        "consoles_games": "Entertainment",
        "books_general_interest": "Books",
        "costruction_tools_tools": "Construction Tools",
        "fashion_underwear_beach": "Fashion",
        "fashion_male_clothing": "Fashion",
        "kitchen_dining_laundry_garden_furniture": "Furniture",
        "industry_commerce_and_business": "Business",
        "fixed_telephony": "Telephony",
        "construction_tools_lights": "Construction Tools",
        "books_technical": "Books",
        "home_appliances_2": "Home Appliances",
        "party_supplies": "Others",
        "drinks": "Food and Drink",
        "market_place": "Others",
        "la_cuisine": "Home Appliances",
        "costruction_tools_garden": "Garden",
        "fashio_female_clothing": "Fashion",
        "home_confort": "Home Improvement",
        "audio": "Electronics",
        "food_drink": "Food and Drink",
        "music": "Entertainment",
        "food": "Food and Drink",
        "tablets_printing_image": "Electronics",
        "books_imported": "Books",
        "small_appliances_home_oven_and_coffee": "Home Appliances",
        "fashion_sport": "Sports and Leisure",
        "christmas_supplies": "Others",
        "fashion_childrens_clothes": "Fashion",
        "dvds_blu_ray": "Entertainment",
        "arts_and_craftmanship": "Arts and Crafts",
        "furniture_bedroom": "Furniture",
        "cine_photo": "Entertainment",
        "diapers_and_hygiene": "Baby",
        "flowers": "Home and Garden",
        "home_comfort_2": "Home Improvement",
        "security_and_services": "Security",
        "furniture_mattress_and_upholstery": "Furniture"
        }



    # Add a constructor here
    def __init__(self, spark):
        self.spark = spark

    def trim_string(self, s):
        return s.strip()


    def transform_data(self, df_customers, df_payments, df_orders, df_order_items, df_products):
        print("Transforming")


        df_c = df_customers.withColumn("customer_state", regexp_replace(col("customer_state").cast("string"), "\\s+", ""))

        
        retail_inner_join = df_c.alias("t1").join(df_orders.alias("t2"), col("t1.customer_id") == col("t2.customer_id")) \
            .join(df_payments.alias("t3"), col("t2.order_id") == col("t3.order_id")) \
            .join(df_order_items.alias("t4"), col("t3.order_id") == col("t4.order_id")) \
            .join(df_products.alias("t5"), col("t4.product_id") == col("t5.product_id")) \
            .select(
                col("t1.customer_id"),
                col("t1.customer_city"),
                col("t1.customer_state"),
                col("t2.order_id"),
                col("t2.order_status"),
                col("t2.order_approved_at"),
                col("t2.order_delivered_timestamp"),
                col("t3.payment_sequential"),
                col("t3.payment_type"),
                col("t3.payment_value"),
                col("t4.order_item_id"),
                col("t4.product_id"),
                col("t4.seller_id"),
                col("t5.product_category_name"))


        # Convert each item of dictionary to map type
        mapping_expr = create_map([lit(x) for x in chain(*self.dictionary.items())])
  
        # Create a new column by calling the function to map the values
        
        df_state = retail_inner_join.withColumn("state",mapping_expr[col("customer_state")])

        mapping_expr1 = create_map([lit(y) for y in chain(*self.dictionary_product.items())])

        df_category = df_state.withColumn("category",mapping_expr1[col("product_category_name")])

        df_final=df_category.drop("customer_state", "order_approved_at", "order_delivered_timestamp","product_category_name")
        
        return df_final




#Loading into hive
class Persist:
    def __init__(self, spark):
        self.spark = spark

    def persist_data(self, df_final):
        print("Loading into hive external table - retail")

        df_final.write.mode('overwrite').option("path","/home/talentum/retail_data").saveAsTable("retail")



class Pipeline:

    def run_pipeline(self):

        print("Running Pipeline")
        ingest_process = Ingest(self.spark)
        df_customers = ingest_process.ingest_table_customer()
        df_payments = ingest_process.ingest_table_payment()
        df_orders = ingest_process.ingest_orders()
        df_order_items = ingest_process.ingest_order_items()
        df_products = ingest_process.ingest_products()


        tranform_process = Transform(self.spark)
        transformed_df = tranform_process.transform_data(df_customers,df_payments,df_orders,df_order_items,df_products)
        transformed_df.show(3, truncate=True) # Show transformed DataFrame

        
        persist_process = Persist(self.spark)
        persist_process.persist_data(transformed_df)
        return

    def create_spark_session(self):
        # A class level variable
        self.spark = SparkSession.builder \
            .appName("ETL_pipeline") \
            .enableHiveSupport().getOrCreate()

    

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run_pipeline()



