# data-engineering-capstone

This project involved ETL processes, for the construction of two datasets. The project was split into two major sections. The first was for constructing a Credit Card System database consisting of 3 tables respectively on branches, credit card transactions, and customers, and the second was for adding a Loan Application table into the database.

1.
For the first section, I created a database in MariaDB, and used SparkSQL to load three supplied .json files into the database to compose its tables (for branches, credit transactions, and customers.

2.
The next step was to create a console-based Python program to allow a user to query the data from the front end. The program I created is split across 2 .py files. One is for users interested in customer details, and the other is for users interested in transaction details.

My general strategy was, for each type of query (there were 7 in total) to rely first on Pyspark to load the .json files, create alternate rdds in various steps of transformation, so as to drop information that would not be needed for the query, and then set up realtively simple SparkSQL queries on the transformed rdds to display tables. When percentages were necessary to calculate, I would gather the numerator and demoninator for the ratio to calculate through separate SparkSQL queries, and then let normal Python do the math and formatting.

The most complicated one of these query types in these front end Python programs, was one where a user wanted to update information in the data pertaining to a specific customer. In order to both display menus and information in a readable way, and to stay anchored in what the user wanted to know, some lists of tuples and functions and for loops were needed that, while not all that elaborate really, were more elaborate than anything else in this capstone process.

3.
For my own data analysis and visualizations, I would convert from rdd to Pandas dataframe, and then use matplotlib on the Pandas dataframe data. Below are the charts I devised from this analysis and visualization portion of the capstone. 

3.1. Transaction types with high quantities of transactions. Bill transactions have the highest rate.
![image](https://user-images.githubusercontent.com/8931602/221990824-37d7586f-35db-4065-bed6-19741777b9f9.png)

3.2a. Customers (total number of transactions) by state. NY has the highest number. 
![image](https://user-images.githubusercontent.com/8931602/221990908-5dec6e19-3d2a-4a54-b504-2f82b565e6dc.png)

3.2b. Customers (total number of customers as persons, tracked my SSN) by state.
![image](https://user-images.githubusercontent.com/8931602/221991161-fae446bd-98d2-4a40-a002-b810ffd75fa0.png)

3.2c. Same as above (customers/SSNs by state), but focused in on the top 5.
![image](https://user-images.githubusercontent.com/8931602/221991304-116a6873-855a-42cf-9470-a1f885198f5c.png)

3.3. Total transactions for the top 10 customers (largest sum of transactions). The customer with SSN 123451125 has the highest sum.
![image](https://user-images.githubusercontent.com/8931602/222017217-18025a17-32fd-4249-9f54-f211f179e8c9.png)

![image](https://user-images.githubusercontent.com/8931602/221991567-474a8099-fd72-4336-81e4-2e9623acc5a3.png)

![image](https://user-images.githubusercontent.com/8931602/221991633-bbb22cfd-9ee3-4d1c-9bd3-041a03692634.png)

![image](https://user-images.githubusercontent.com/8931602/221991702-b264960d-c37b-4d32-96d9-6192e4145427.png)

![image](https://user-images.githubusercontent.com/8931602/221991751-5313c0e7-4f66-42f3-8ec8-ac4e1313a4e4.png)

![image](https://user-images.githubusercontent.com/8931602/221991809-b1188697-fdbc-4d45-8914-da58402d35db.png)

![Screenshot (20)](https://user-images.githubusercontent.com/8931602/222005046-6087c25f-c91a-4579-a17d-130fffab6aec.png)
