# import mysql.connector as mariadb
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Cap_T.com').getOrCreate()
df_branch = spark.read.json("cdw_sapp_branch.json")
df_branch.createTempView("branch")
# import pandas as pd
# import numpy as np

user = str(input("Hello human! What should I call you? And by that I mean to say, what is your name? "))
print('')
print(f'Hello {user}! It is nice to meet you. I bet you would like to know some transaction details. Right? Right.')
print("Okay, great. Now let me know which details you want to know. Here are your options:")
print('...')
print("1) transactions made by customers living in a given zip code for a given month and year.")
print("2) number and total values of transactions for a given type.")
print("3) number and total values of transactions for branches in a given state.")
print("...")
choice = str(input("Enter the number of the option you are interested in, e.g., '1' (without the quotes): "))
if choice == '1':
    df_credit = spark.read.json("cdw_sapp_credit.json")
    df_customer = spark.read.json("cdw_sapp_custmer.json")
    zip = input("Enter the 5-digit zipcode: ")
    month = input("Enter the month in digits, e.g., '3' if you want to know about March: ")
    year = input("Enter the 4-digit year: ")
    print("Okay, wait right there, I am figuring it our for you...")
    df_credit.createOrReplaceTempView("credit")
    df_customer.createOrReplaceTempView("customer")
    reduced1 = spark.sql(f"SELECT BRANCH_CODE, credit.CREDIT_CARD_NO, CUST_SSN, DAY, MONTH, TRANSACTION_ID, TRANSACTION_TYPE, TRANSACTION_VALUE, YEAR, customer.CUST_ZIP FROM credit JOIN customer ON customer.SSN == credit.CUST_SSN WHERE MONTH == {month} AND YEAR == {year} AND customer.CUST_ZIP == {zip}")
    print(reduced1.show())


elif choice == '2':
    df_credit = spark.read.json("cdw_sapp_credit.json")
    print("You can choose from the following options:")
    df_credit.createOrReplaceTempView("credit")
    spark.sql("SELECT DISTINCT(TRANSACTION_TYPE) FROM credit").show()
    type = input("Enter the type of transaction, exactly the way it's written in the list, e.g., 'Gas' (without the quotes): ")
    print(f"Okay, you want to know the number and total value of transactions for {type} transactions.")
    print("Wait right there, I am figuring it our for you...")
    df_credit = df_credit.filter(df_credit.TRANSACTION_TYPE == type)
    df_credit.createOrReplaceTempView("credit")
    spark.sql("SELECT COUNT(*) AS number FROM credit").show()
    spark.sql("SELECT SUM(TRANSACTION_VALUE) AS total_value FROM credit").show()
    # print(f"First, here is the number of transactions for {type} transactions:")
    # print(f"Voila! {number.show()}") 
    # print(f"Now, here is the total value for {type} transactions:")
    # print(f"And voila! {total.show()}")   
elif choice == '3':
    state = input("Enter the two letter abbreviation for the state, e.g., 'MA': ")
    df_credit = spark.read.json("cdw_sapp_credit.json")
    df_branch = spark.read.json("cdw_sapp_branch.json")
    df_branch = df_branch.filter(df_branch.BRANCH_STATE == state) 
    df_branch.createOrReplaceTempView("branch")
    df_credit.createOrReplaceTempView("credit")
    spark.sql("SELECT COUNT(TRANSACTION_ID) as number from credit \
              JOIN branch on branch.BRANCH_CODE == credit.BRANCH_CODE").show()
    spark.sql("SELECT SUM(TRANSACTION_VALUE) as total_value from credit \
              JOIN branch on branch.BRANCH_CODE == credit.BRANCH_CODE").show()


elif choice == 'cat':
    print('''
    
.==============================================.
|                                              |
|                           .'\                |
|                          //  ;               |
|                         /'   |               |
|        .----..._    _../ |   \               |
|         \'---._ `.-'      `  .'               |
|          `.    '              `.             |
|            :            _,.    '.            |
|            |     ,_    (() '    |            |
|            ;   .'(().  '      _/__..-        |
|            \ _ '       __  _.-'--._          |
|            ,'.'...____'::-'  \     `'        |
|           / |   /         .---.              |
|     .-.  '  '  / ,---.   (     )             |
|    / /       ,' (     )---`-`-`-.._          |
|   : '       /  '-`-`-`..........--'\         |
|   ' :      /  /                     '.       |
|   :  \    |  .'         o             \      |
|    \  '  .' /          o       .       '     |
|     \  `.|  :      ,    : _o--'.\      |     |
|      `. /  '       ))    (   )  \>     |     |
|        ;   |      ((      \ /    \___  |     |
|        ;   |      _))      `'.-'. ,-'` '     |
|        |    `.   ((`            |/    /      |
|        \     ).  .))            '    .       |
|     ----`-'-'  `''.::.________:::mx'' ---    |
|                                              |
|                                              |
|                                              |
'=============================================='
''')

else:
    print("That's not a real choice. Bye.")
           
#Order by day in descending order.
# 2)    Used to display the number and total values of transactions for a given type.
# 3)    Used to display the number and total values of transactions for branches in a given state.")

#elif proceed == 'n':
#     print("I'm not sure why you ran this program then. Anyway, if you change your mind, run this program again. Bye.")

# else:
#     print("That wasn't a real answer. Run the program if you want to start over. Otherwise, see ya!")
# TRANSACTION DETs MODULE
# 1)    Used to displpytay the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.
# 2)    Used to display the number and total values of transactions for a given type.
# 3)    Used to display the number and total values of transactions for branches in a given state.

#docker run --name Jenkins -p 8080:8080 -p 50000:50000 -v jenkins_home:/var/jenkins_home jenkins/jenkins:lts-jdk11