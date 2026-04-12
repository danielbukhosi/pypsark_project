import time
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from dotenv import load_dotenv

load_dotenv()
def spark_session()-> SparkSession:
      return ( SparkSession.builder \
            .appName("pyspark_project") \
            .master("local[*]") \
            .config("spark.driver.memory","1g") \
            .config("spark.sql.shuffle.partitions","8") \
            .config("spark.ui.port","4040") \
            .config("spark.sql.repl.eagerEval.enabled", True) \
            .config("spark.sql.adaptive.enabled","true") \
            .config("spark.sql.coalescePartitions.enabled","true") \
            .config("spark.sql.adaptive.skewJoin.enabled","true") \
            .getOrCreate()
       )
spark = spark_session()

table_names:tuple[str] = (
                     "public.actor",
                     "public.address",
                     "public.category",
                     "public.city",
                     "public.country",
                     "public.customer",
                     "public.film",
                     "public.film_actor",
                     "public.film_category",
                     "public.inventory",
                     "public.payment",
                     "public.rental"
   )

table_PKs:tuple[str] = (
                    "actor_id",
                    "address_id",
                    "category_id",
                    "city_id",
                    "country_id",
                    "customer_id",
                    "film_id",
                    "inventory_id",
                    "payment_id",
                    "rental_id"
)

def read_tables(dbtable:str,partionColumn:str):
     return ( spark.read \
          .format("jdbc") \
          .option("url",os.getenv("DB_URL"))\
          .option("dbtable",dbtable) \
          .option("user",os.getenv("DB_USER")) \
          .option("password",os.getenv("DB_PASSWORD")) \
          .option("driver","org.postgresql.Driver")\
          .option("inferSchema","true") \
          .option("partitionColumn",partionColumn) \
          .option("lowerBound","1") \
          .option("upperBound","3000") \
          .option("numPartitions","10") \
          .load()
     )

actor = read_tables(table_names[0], table_PKs[0])
address = read_tables(table_names[1], table_PKs[1])
category = read_tables(table_names[2], table_PKs[2])
city = read_tables(table_names[3], table_PKs[3])
country = read_tables(table_names[4], table_PKs[4])
customer = read_tables(table_names[5], table_PKs[5])
film = read_tables(table_names[6], table_PKs[6])
film_actor = read_tables(table_names[7], table_PKs[6])
film_category = read_tables(table_names[8], table_PKs[6])
inventory = read_tables(table_names[9], table_PKs[7])
payment = read_tables(table_names[10], table_PKs[8])
rental = read_tables(table_names[11], table_PKs[9])

# category -> film_category -> film(left join)
result1 = (
          category.alias("c").join(film_category.alias("fc"),
                                   F.col("c.category_id")==F.col("fc.category_id"),
                                   "left"
                              )
                             .join(film.alias("f"),
                                   F.col("fc.film_id")==F.col("f.film_id"),
                                   "left"
                              )
                             .groupBy("c.name")
                             .agg(
                               F.count(F.col("f.film_id")).alias("movies_in_category")
                             )
                             .select("name","movies_in_category")
                             .orderBy(F.desc("movies_in_category"))
)
result1.show(10)

# actor->film_actor->inventory->rental(left join)
result2  = ( 
           actor.alias("a").join(
                            film_actor.alias("fa"),
                            F.col("a.actor_id")==F.col("fa.actor_id"),
                            "left"
                          )
                          .join(inventory.alias("i"),
                                F.col("fa.film_id")==F.col("i.film_id"),
                                "left"
                                
                          )
                          .join(rental.alias("r"),
                                F.col("i.inventory_id")==F.col("r.inventory_id"),
                                "left"
                          )
                          .groupBy("a.actor_id","a.first_name","a.last_name")
                          .agg(
                            F.count(F.col("r.rental_id")).alias("rental_count")
                          )
                          .withColumn("full_name",
                                      F.concat_ws(" ", F.col("a.first_name"),F.col("a.last_name"))
                          )
                          .select("actor_id","full_name","rental_count")
                          .orderBy(F.desc("rental_count"))

    )

result2.show(10)

# category->film_category->film->inventory->rental->payment(left join)
result3 = (
          category.alias("c").join(
                              film_category.alias("fc"),
                              F.col("c.category_id")==F.col("fc.category_id"),
                              "left"
                             )
                             .join(
                              film.alias("f"),
                              F.col("fc.film_id")==F.col("f.film_id"),
                              "left"
                             )
                             .join(
                              inventory.alias("i"),
                              F.col("f.film_id")==F.col("i.film_id"),
                              "left"
                             )
                             .join(
                              rental.alias("r"),
                              F.col("i.inventory_id")==F.col("r.inventory_id"),
                              "left"
                             )
                             .join(
                               payment.alias("p"),
                               F.col("r.rental_id")==F.col("p.rental_id"),
                               "left"
                             )
                             .groupBy("c.name")
                             .agg(
                               F.sum("p.amount").alias("total_amount")
                             )
                             .select("name","total_amount")
                             .orderBy(F.desc("total_amount"))
                             .limit(1)
)

result3.show(10)

# film -> inventory(anti join)
result4 = (
           film.alias("f").join(
                           inventory.alias("i"),
                           F.col("f.film_id")==F.col("i.film_id"),
                           "anti"
                          )
                          .select(F.col("title").alias("name_of_movie"))
)

result4.show(10)

# actor->film_actor->film->film_category->category(left join)
result5 = (
          actor.alias("a").join(
                           film_actor.alias("fa"),
                           F.col("a.actor_id")==F.col("fa.actor_id"),
                           "left"
                          )
                          .join(
                            film.alias("f"),
                            F.col("fa.film_id")==F.col("f.film_id"),
                            "left"
                          )
                          .join(
                            film_category.alias("fc"),
                            F.col("f.film_id")==F.col("fc.film_id"),
                            "left"
                          )
                          .join(
                            category.alias("c"),
                            F.col("fc.category_id")==F.col("c.category_id"),
                            "left"
                          )
                          .filter(F.col("c.name")=="Children")
                          .groupBy("a.actor_id","a.first_name","a.last_name")
                          .agg(F.count("a.actor_id").alias("appearences"))
                          .withColumn("full_name",F.concat_ws(" ",F.col("a.first_name"),F.col("a.last_name")))
                          .select("actor_id","full_name","appearences")
                          .orderBy(F.desc("appearences"))
)

result5.show(10)

result6 = (
          city.alias("ci").join(
                          country.alias('co'),
                          F.col("ci.country_id")==F.col("co.country_id"),
                          "left"
                         )
                         .join(
                           address.alias("a"),
                           F.col("ci.city_id")==F.col("a.city_id"),
                           "left"
                         )
                         .join(
                           customer.alias("c"),
                           F.col("a.address_id")==F.col("c.address_id")
                         )
                         .groupBy("ci.city_id","ci.city")
                         .agg(
                           F.sum(F.when(F.col("c.active")==1,1).otherwise(0)).alias("active_customers"),
                           F.sum(F.when(F.col("c.active")==0,1).otherwise(0)).alias("Inactive_customers")
                         )
                         .select("city_id","city","active_customers","Inactive_customers")
                         .orderBy(F.desc("Inactive_customers"))
)

result6.show(10)

w_rank = Window.partitionBy("city").orderBy(F.desc("total_rental_hours"))

result7_01 = (
    category.alias("ca")
    .join(film_category.alias("fc"), F.col("ca.category_id") == F.col("fc.category_id"), "left")
    .join(film.alias("f"), F.col("fc.film_id") == F.col("f.film_id"), "left")
    .join(inventory.alias("i"), F.col("f.film_id") == F.col("i.film_id"), "left")
    .join(rental.alias("r"), F.col("i.inventory_id") == F.col("r.inventory_id"), "left")
    .join(customer.alias("c"), F.col("r.customer_id") == F.col("c.customer_id"), "left")
    .join(address.alias("a"), F.col("c.address_id") == F.col("a.address_id"), "left")
    .join(city.alias("ci"), F.col("a.city_id") == F.col("ci.city_id"), "left")
    .filter(F.col("ca.name").ilike("a%")) 
    #  Perform Aggregation FIRST
    .groupBy(F.col("ci.city").alias("city"), F.col("ca.name").alias("category_name"))
    .agg(F.sum("f.rental_duration").alias("total_rental_hours"))
    #  Apply Rank AFTER Aggregation
    .withColumn("total_rental_hours_ranking", F.dense_rank().over(w_rank))
    .filter(F.col("total_rental_hours_ranking") == 1)
    .orderBy(F.desc("total_rental_hours"))
)

result7_01.show(10)

result7_02 = (
    category.alias("ca")
    .join(film_category.alias("fc"), F.col("ca.category_id") == F.col("fc.category_id"), "left")
    .join(film.alias("f"), F.col("fc.film_id") == F.col("f.film_id"), "left")
    .join(inventory.alias("i"), F.col("f.film_id") == F.col("i.film_id"), "left")
    .join(rental.alias("r"), F.col("i.inventory_id") == F.col("r.inventory_id"), "left")
    .join(customer.alias("c"), F.col("r.customer_id") == F.col("c.customer_id"), "left")
    .join(address.alias("a"), F.col("c.address_id") == F.col("a.address_id"), "left")
    .join(city.alias("ci"), F.col("a.city_id") == F.col("ci.city_id"), "left")
    .filter(F.col("ci.city").contains("-")) 
    # Perform Aggregation FIRST
    .groupBy(F.col("ci.city").alias("city"), F.col("ca.name").alias("category_name"))
    .agg(F.sum("f.rental_duration").alias("total_rental_hours"))
    #  Apply Rank AFTER Aggregation
    .withColumn("total_rental_hours_ranking", F.dense_rank().over(w_rank))
    .filter(F.col("total_rental_hours_ranking") == 1)
    .orderBy(F.desc("total_rental_hours"))
)

result7_02.show(10)


time.sleep(20) # Chance to see spark UI
spark.stop()