import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Cap_C.com').getOrCreate()
df_branch = spark.read.json("cdw_sapp_branch.json")
df_branch.createTempView("branch")
# import pandas as pd
# import numpy as np

user = str(input("Hello human! What should I call you? And by that I mean to say, what is your name? "))
print('')
print(f'Hello {user}! It is nice to meet you. Have I met you before?')
print("...") 
print("...") 
print("...") 
print("...") 
print("...") 
print("Oh well, whatever.")
print("I bet you would like to know or modify some customer details. Right? Am I right.")
print("Okay, great. Now let me know which option you want. Here are your options:")
print('...')
print("1) check the existing account details of a customer.")
print("2) modify the existing account details of a customer.")
print("3) view a monthly bill for a credit card number for a given month and year.")
print("4) view the transactions made by a customer between two dates.")
print("...")
choice = str(input("Enter the number of the option you are interested in, e.g., '1' (without the quotes): "))
if choice == '1':
    print()
    first = input("What is the customer's first name? ")
    middle = input("What is the customer's middle name? ")
    last = input("What is the customer's last name? ")
    print("...")
    print("Okay, don't go anywhere, I'll get you the information you request.")
    df_customer = spark.read.json("cdw_sapp_custmer.json")
    df_customer = df_customer.filter(df_customer.FIRST_NAME == first) 
    df_customer = df_customer.filter(df_customer.MIDDLE_NAME == middle) 
    df_customer = df_customer.filter(df_customer.LAST_NAME == last) 
    df_customer.createTempView("customer")
    spark.sql("SELECT * from customer").show()
    
elif choice == '2':
    year = 1
elif choice == '3':
    df_credit = spark.read.json("cdw_sapp_credit.json")
    number = input("What is the credit card number? 16 digits. No spaces, no dashes please: ")
    month = input("What is the month? Give it as digits (no leading zeros): ")
    year = input("What is the year? Give it as 4 digits: ")
    df_credit = df_credit.filter(df_credit.CREDIT_CARD_NO == number)
    df_credit = df_credit.filter(df_credit.MONTH == month)
    df_credit = df_credit.filter(df_credit.YEAR == year)
    df_credit.createTempView("credit")
    spark.sql("SELECT * from credit ORDER BY DAY").show()
    spark.sql("SELECT sum(TRANSACTION_VALUE) as BALANCE from credit").show()


elif choice == '4':
    df_credit = spark.read.json("cdw_sapp_credit.json")
    number = input("What is the customer's social security number? Obviously you know what it is.")
    print("I will ask you for a start date and an end date, and I can show you the transactions between those dates.")
    print("I will ask you for this information for both dates in a piecemeal fashion. Ready? Yes, great.")
    print("...")
    print("First, let's get the start date. This will be the first day of the time period I will analyze for you.")
    s_year = input("What is the starting year? ")
    s_month = input("What is the starting month (in digits, please)? ")
    s_day = input("What is the starting day (digits)? ")
    print("...")
    df_credit = df_credit.filter(df_credit.YEAR >= s_year)
    df_credit = df_credit.filter(df_credit.MONTH >= s_month)
    df_credit = df_credit.filter(df_credit.DAY >= s_day)
    print("Now give me the ending date, in the same way. This will be the last day of the...yeah, you get it.")
    e_year = input("What is the ending year? ")
    e_month = input("What is the ending month? ")
    e_day = input("What is the ending day? ")

# CUSTOMER DETAILS MODULE

# 1) Used to check the existing account details of a customer.
# 2) Used to modify the existing account details of a customer.
# 3) Used to generate a monthly bill for a credit card number for a given month and year.
# 4) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.