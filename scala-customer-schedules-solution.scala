// Databricks notebook source
// MAGIC %md ## Final evaluation
// MAGIC 
// MAGIC For the last activity of the semester, we have been asked to help design the visit schedule of 50K customers for a large company. In each visit, the company's staff will offer a promotion of some defined products. Previous studies showed that pushing too many promotions per day can lead to the customer feeling overwhelmed.
// MAGIC 
// MAGIC The dataset consists of a list of customers, their applicable products (can be different per customer), and the valid days (valid = 1, not valid = 0) in which each product can be offered. Below is the data we have received.

// COMMAND ----------

var df = spark.read.format("csv").option("header",true).load("/FileStore/tables/case_study.csv")
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Objective
// MAGIC 
// MAGIC The task is to define a personalized schedule for each customer, in which every applicable product is offered on one valid day of the week. As there are many possible schedules per customer (and we want to comply with the company's concerns), we will evaluate the "fitness" of the schedule as the maximum number of products offered on any day of the week. The higher the maximum number of offerings in a day, the worst the fitness.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Example
// MAGIC 
// MAGIC Below is an example where the company is scheduling Lex's visits:
// MAGIC 
// MAGIC | CustomerId     | product     | Monday     | Tuesday     | Wednesday     | Thursday     | Friday     | Saturday     |
// MAGIC |:----------:    |:-------:    |:------:    |:-------:    |:---------:    |:--------:    |:------:    |:--------:    |
// MAGIC |     Lex        |    A        |    0       |    1        |     0         |     1        |    0       |     0        |
// MAGIC |     Lex        |    B        |    1       |    0        |     0         |     1        |    0       |     0        |
// MAGIC |     Lex        |    C        |    0       |    1        |     1         |     0        |    0       |     0        |
// MAGIC |     Lex        |    D        |    0       |    0        |     0         |     0        |    1       |     0        |
// MAGIC 
// MAGIC For product A, we have 2 possible days to be suggested, Tuesday and Thursday. Similarly, there is only one day for product D. Let's define the promotion offering schedule as follows:
// MAGIC 
// MAGIC | CustomerId     | Monday     | Tuesday     | Wednesday     | Thursday     | Friday     | Saturday     |
// MAGIC |:----------:    |:------:    |:-------:    |:---------:    |:--------:    |:------:    |:--------:    |
// MAGIC |     Lex        |    B       |   A, C      |     -         |     -        |    D       |     -        |
// MAGIC 
// MAGIC We can observe that the highest amount of products being offered is on Tuesday, being it 2. Therefore, we can evaluate this schedule with a fitness of 2 (maximum number of simultaneous offerings).
// MAGIC 
// MAGIC As we are dealing with 50,000 customers, we will aggregate the fitness metric by taking the average of it across all customers. Let us say that we only have the following two customers:
// MAGIC 
// MAGIC | CustomerId     | Monday     | Tuesday     | Wednesday     | Thursday     | Friday     | Saturday     | Fitness     |
// MAGIC |:----------:    |:------:    |:-------:    |:---------:    |:--------:    |:------:    |:--------:    |:-------:    |
// MAGIC |     Lex        |    B       |   A, C      |     -         |     -        |    D       |     -        |    2        |
// MAGIC |    Yann        |    A       |    P        |     J         |     E        |  B,Y,K     |     -        |    3        |
// MAGIC 
// MAGIC The total fitness of the scheduling heuristic would be (2+3)/2 = 2.5. 

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

// Cast types to integer 
df = df.select($"CustomerId", $"product".cast(IntegerType), $"MONDAY".cast(IntegerType), $"TUESDAY".cast(IntegerType), $"WEDNESDAY".cast(IntegerType), $"THURSDAY".cast(IntegerType), $"FRIDAY".cast(IntegerType), $"SATURDAY".cast(IntegerType))

// COMMAND ----------

// drop all rows that have 0's in all days
// which are the ones that are not valid, which means that they can't be promoted on any day of the week
df = df.filter(!(lit($"MONDAY") === 0 && lit($"TUESDAY") === 0 && lit($"WEDNESDAY") === 0 && lit($"THURSDAY") === 0 && lit($"FRIDAY") === 0 && lit($"SATURDAY") === 0))
display(df)

// COMMAND ----------

//        calculate the total number of days that each product has
df = df.withColumn("n_days", lit($"MONDAY") + lit($"TUESDAY") + lit($"WEDNESDAY") + lit($"THURSDAY") + lit($"FRIDAY") + lit($"SATURDAY"))
//        columns with the day of the which in which the product can be promoted
       .withColumn("M", when($"MONDAY" === 1, "Monday").otherwise(0))
       .withColumn("T", when($"TUESDAY" === 1, "Tuesday").otherwise(0))
       .withColumn("W", when($"WEDNESDAY" === 1, "Wednesday").otherwise(0))
       .withColumn("TH", when($"THURSDAY" === 1, "Thursday").otherwise(0))
       .withColumn("F", when($"FRIDAY" === 1, "Friday").otherwise(0))
       .withColumn("S", when($"SATURDAY" === 1, "Saturday").otherwise(0))

// COMMAND ----------

display(df)

// COMMAND ----------

// Arrays with the days 
df = df.withColumn("d_array", array($"MONDAY", $"TUESDAY", $"WEDNESDAY", $"THURSDAY", $"FRIDAY", $"SATURDAY"))
       .withColumn("days", array($"M", $"T", $"W", $"TH", $"F", $"S"))

display(df)

// COMMAND ----------

var w = Window.partitionBy("CustomerId")
// var w3 = Window.partitionBy("CustomerId").orderBy($"n_days".asc).rowsBetween(Window.unboundedPreceding, Window.currentRow)

// remove 0's from arrays
df = df.withColumn("sorted_items", array_remove(df("d_array"), 0))
          .withColumn("d", array_remove(df("days"), "0"))
          .select("CustomerId", "product", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "n_days", "sorted_items", "d")
//           total of products by customer
          .withColumn("n_prod", count("product") over w)
          .orderBy($"CustomerId".asc, $"n_days".asc)

display(df)

// COMMAND ----------

var w = Window.partitionBy("CustomerId")
var w2 = Window.partitionBy("CustomerId", "number_days")
var w3 = Window.partitionBy("CustomerId", "number_days").orderBy($"number_days".asc).rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = df
//             day of week when the number of days is 1
           .withColumn("firstday", when($"n_days" === 1, $"d".getItem(0)).otherwise(0))
//           column with the first day by customerId
           .withColumn("f_day", collect_list("firstday") over w)
           .withColumn("f_day", $"f_day".getItem(0))
//             removes the first day from the arrays that have more than one day
           .withColumn("new_days", when($"n_days" > 1, array_remove($"d", lit($"f_day"))).otherwise($"d"))
//            .withColumn("new_days", $"d")
//           recalculates the number of days
           .withColumn("number_days", when($"n_days" > 1 && array_contains($"d", $"f_day"), (lit($"n_days") - 1)).otherwise($"n_days"))
//            .withColumn("number_days", $"n_days")
//             total amount of products per customer
           .withColumn("total_day", count("product") over w2)
           .withColumn("ones", lit(1))
//           cummulative sum of the number of days (enumerates them by customer and amount of days)
           .withColumn("sum_days", sum("ones") over w3)
//             calculates the amount of products when the amount of days is greater than 1
           .withColumn("amount_perday", when($"number_days" > 1, ceil($"total_day" / $"number_days")).otherwise($"total_day"))

display(df)

// COMMAND ----------

// once we have the amount of products per day
// for each product, it selects the day depending on the acumulated value (sum_days), to better distribute the amount of products
df = df.withColumn("finalday", when($"sum_days" <= $"amount_perday", $"new_days".getItem(0))
//                when a product has 2 or more days 
               .otherwise(when(lit($"number_days") >= lit(2) && lit($"sum_days") <= ($"amount_perday" * lit(2)), $"new_days".getItem(1))
//                       when a product has 3 or more days 
                          .otherwise(when(lit($"number_days") >= lit(3) && lit($"sum_days") <= ($"amount_perday" * lit(3)), $"new_days".getItem(2))
//                                      when a product has 4 or more days 
                                     .otherwise(when(lit($"number_days") >= lit(4) && lit($"sum_days") <= ($"amount_perday" * lit(4)), $"new_days".getItem(3))
//                                                 when a product has 5 or 6 days 
                                                .otherwise(when(lit($"number_days") >= lit(5) && lit($"sum_days") <= ($"amount_perday" * lit(5)), $"new_days".getItem(4))
//                                                           get the last day of the array (new_days)
                                                           .otherwise(element_at($"new_days", lit($"number_days"))))))))

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %md #### Expected output
// MAGIC 
// MAGIC The previous example serves only to explain the problem, however, the expected output schema should comply with the following table:
// MAGIC 
// MAGIC | CustomerId (Long)    | product (Str)    |   day (Str)      | fitness (Int) |
// MAGIC |:----------:        |:-------:        |:-------:        |:-------:        |
// MAGIC |     505            |    A            | Tuesday         |    2            |
// MAGIC |     505            |    B            |  Monday         |    2            |
// MAGIC |     505            |    C            | Tuesday         |    2            |
// MAGIC |     505            |    D            |  Friday         |    2            |
// MAGIC |    6780            |    A            |  Monday         |    3            |
// MAGIC |    6780            |    B            |  Friday         |    3            |
// MAGIC |    6780            |    P            | Tuesday         |    3            |

// COMMAND ----------

var w = Window.partitionBy("CustomerId", "finalday")
var w2 = Window.partitionBy("CustomerId")

// calculate the number of products for each customer on each day
val df2 = df.withColumn("count", count("finalday") over w)
//           obtain the maximum fitness for each customer
          .withColumn("fitness", max($"count") over w2)
//           display the customerId, the product, day on which it will be promoted, and the maximum fitness of all days 
          .select($"CustomerId", $"product", $"finalday".alias("day"), $"fitness")
display(df2)

// COMMAND ----------

// MAGIC %md #### Performance
// MAGIC 
// MAGIC To get the overall performance of your scheduling heuristic, apply the following function to the expected output DataFrame. The lower the overall fitness, the better.

// COMMAND ----------

// display the overall performance of the scheduling heuristic
display(
  df2.select("CustomerId", "fitness").distinct.agg(avg("fitness").alias("Overall Performance"))
)

// COMMAND ----------

// display the overall performance of the scheduling heuristic
display(df.groupBy("CustomerId", "finalday").agg(count("*").alias("count"))
       .groupBy("CustomerId").agg(max($"count").alias("fitness"))
       .agg(avg("fitness").alias("Overall Performance"))
)

// COMMAND ----------

// display(
//   output.select("CustomerId", "fitness").distinct.agg(avg("fitness").alias("Overall Performance"))
// )

// COMMAND ----------

// MAGIC %md #### Evaluation
// MAGIC The company has a solution implemented already, however, they think that it can be improved. The performance of your submission X will be then compared against the score S of the company's algorithm (which will be shared with you at the end). The score you get will depend on the next table:
// MAGIC 
// MAGIC | Your performance     | Grade     |
// MAGIC |:--------------------:|:---------:|
// MAGIC |    X < S+40          |   70      |
// MAGIC |    X < S+30          |   80      |
// MAGIC |    X < S+20          |   85      |
// MAGIC |    X < S+10          |   90      |
// MAGIC |    X < S+5           |   95      |
// MAGIC |    X <= S            |  100      |
// MAGIC 
// MAGIC Any selection that is worse than S+40 will be graded as 60.

// COMMAND ----------


