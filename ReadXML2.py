import re
import xml.etree.ElementTree as ET
from pyspark import SparkContext

# sc =SparkContext(master="local", appName="Project")

root = ET.parse('/home/lucky/Downloads/DS2.xml').getroot()

# print(ET.tostring(root, encoding='utf8').decode('utf8'))

list1 = []
list2 = []
list3 = []
list4 = []
list5 = []
list6 = []
list_d2 = []

i=0
for givenname in root.findall(".//givenname"):
    a = givenname.text
    list1.append(a)

for employer in root.findall(".//employer[1]"):
    b = employer.text
    list2.append(b)

for Salary1 in root.findall(".//Salary[1]"):
    c = Salary1.text
    y = c.split('USD')[0]
    x = re.sub(',', '', y)
    x = x.strip()
    list3.append(x)

for Salary2 in root.findall(".//Salary[2]"):
    d = Salary2.text
    y = d.split('USD')[0]
    x = re.sub(',', '', y)
    x = x.strip()
    list4.append(x)

for employer2 in root.findall(".//employer[2]"):
    e = employer2.text
    list5.append(e)

for degree in root.iter('degree'):
    b = degree.text

    list6.append(b)


while (True):

    if (i < len(list1)):
        y=(list1[i], list6[i], list2[i],list5[i],list3[i],list4[i])

        list_d2.append(y)

        i=i+1
        continue

    else:
        break
