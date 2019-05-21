# Spark_Mini_Project

We have two XML DS1 and DS2.  

Task 1:
For every individual in DS1 find the avarage salary. When their degree is ‘bachelor’


1. read DS1 and DS2 in Rdd.
2.Join them on alteast first name.
3.Then filter the data which contain ‘bachelor’.NOTE ‘bachelor’ is in small caps.
4.Convert the dataset into DF and find the avarge salary.
5. Save the file in csv format

Task 2:

Find the avarage salary of individuals grouped by state in US.

1. read DS1 and DS2 in Rdd.
2. Join them on alteast first name.
3. Filter the data for US.
4.Convert the dataset into DF and find the avarge salary of every individual grouped by state.
5. Save the file in tsv format.

Task 3:

Find the maximum and minimum salary drawn by all individual in DS1.

1. read DS1 and DS2 in Rdd.
2. Join them on alteast first name.
3.Convert the dataset into DF and find the max and min salary .
5. Save the file in tsv format.
