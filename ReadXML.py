import xml.etree.ElementTree as ET

import spark

import pyspark
from pyspark import SparkContext
import ReadXML2
from pyspark.sql import SparkSession, Row


sc =SparkContext(master="local", appName="Project2")

root = ET.parse('/home/lucky/Downloads/DS1.xml').getroot()

# print(ET.tostring(root, encoding='utf8').decode('utf8'))

list1 = []
list2 = []
list3 = []
list4 = []
list_d1=[]

i=0
for fullname in root.iter('fullname'):

    y = fullname.text
    x = y.capitalize()
    list1.append(x)

for degree in root.iter('degree'):
    b = degree.text

    list2.append(b)

for country in root.iter('country'):
    c = country.text

    list3.append(c)

for country in root.iter('state'):
    c = country.text

    list4.append(c)

while (True):

    if (i < len(list1)):
        x=(list1[i],list2[i],list3[i], list4[i])
        list_d1.append(x)
        i=i+1
        continue

    else:
        break

d1 = sc.parallelize(list_d1)

d2 = sc.parallelize(ReadXML2.list_d2)

nd11=d1.map(lambda x: (x[0].split("_")[0],x[1],x[2], x[3]))

nd1 = nd11.map(lambda x: (x[0:2], x[2:]))

nd2=d2.map(lambda x: (x[0:2], x[2:]))

join = nd1.join(nd2)

spark = SparkSession(sc)

df = join.map(lambda (k, v): Row(k, v[0], v[1])).toDF()

df.createTempView("temp")

temp = spark.sql("select _1._1 as Name, _1._2 as Degree, _2._1 as Country, _2._2 as State, _3._1 as Hotel_1, _3._2 as Hotel_2, "
                "cast (_3._3 as int) as Salary_1, cast (_3._4 as int) as Salary_2 from temp ")

temp.createTempView("resume")

res = spark.sql("select * from resume")

res.show()

res1 = spark.sql("select Name, ((Salary_1+Salary_2)/2) as Average_Salary from resume where Degree like 'Bachelor%'")

res1.show()

# res1.write.option("header","true").csv("/home/lucky/Downloads/Output/Result1.csv")

res2 = spark.sql("select State, ((Salary_1+Salary_2)/2) as Average_Salary from resume where Country ='US'")

res2.createTempView("res2")

res21 = spark.sql("select State, Avg(Average_Salary) from res2 group by State")

res21.show()

# res21.coalesce(1).write.option("header","true").option("delimiter", "\t").csv("/home/lucky/Downloads/Output/Result2.tsv")

res3 = spark.sql("select Name, min(Salary_1) as Min_Salary1, max(Salary_2) as Max_Salary2 from resume group by Name")

res3.show()

# res3.coalesce(1).write.option("header","true").option("delimiter", "\t").csv("/home/lucky/Downloads/Output/Result3.tsv")

