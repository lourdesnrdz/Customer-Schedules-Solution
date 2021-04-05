# Customer-Schedules-Solution

For this activity, we have been asked to help design the visit schedule of 50K customers for a large company. In each visit, the company's staff will offer a promotion of some defined products. Previous studies showed that pushing too many promotions per day can lead to the customer feeling overwhelmed.

The dataset consists of a list of customers, their applicable products (can be different per customer), and the valid days (valid = 1, not valid = 0) in which each product can be offered. Below is the data we have received.

## Objective

The task is to define a personalized schedule for each customer, in which every applicable product is offered on one valid day of the week. As there are many possible schedules per customer (and we want to comply with the company's concerns), we will evaluate the "fitness" of the schedule as the maximum number of products offered on any day of the week. The higher the maximum number of offerings in a day, the worst the fitness.

## Example

Below is an example where the company is scheduling Lex's visits:

| CustomerId     | product     | Monday     | Tuesday     | Wednesday     | Thursday     | Friday     | Saturday     |
|:----------:    |:-------:    |:------:    |:-------:    |:---------:    |:--------:    |:------:    |:--------:    |
|     Lex        |    A        |    0       |    1        |     0         |     1        |    0       |     0        |
|     Lex        |    B        |    1       |    0        |     0         |     1        |    0       |     0        |
|     Lex        |    C        |    0       |    1        |     1         |     0        |    0       |     0        |
|     Lex        |    D        |    0       |    0        |     0         |     0        |    1       |     0        |

For product A, we have 2 possible days to be suggested, Tuesday and Thursday. Similarly, there is only one day for product D. Let's define the promotion offering schedule as follows:

| CustomerId     | Monday     | Tuesday     | Wednesday     | Thursday     | Friday     | Saturday     |
|:----------:    |:------:    |:-------:    |:---------:    |:--------:    |:------:    |:--------:    |
|     Lex        |    B       |   A, C      |     -         |     -        |    D       |     -        |

We can observe that the highest amount of products being offered is on Tuesday, being it 2. Therefore, we can evaluate this schedule with a fitness of 2 (maximum number of simultaneous offerings).

As we are dealing with 50,000 customers, we will aggregate the fitness metric by taking the average of it across all customers. Let us say that we only have the following two customers:

| CustomerId     | Monday     | Tuesday     | Wednesday     | Thursday     | Friday     | Saturday     | Fitness     |
|:----------:    |:------:    |:-------:    |:---------:    |:--------:    |:------:    |:--------:    |:-------:    |
|     Lex        |    B       |   A, C      |     -         |     -        |    D       |     -        |    2        |
|    Yann        |    A       |    P        |     J         |     E        |  B,Y,K     |     -        |    3        |

The total fitness of the scheduling heuristic would be (2+3)/2 = 2.5. 