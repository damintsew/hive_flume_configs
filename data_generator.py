import csv
import sys
from faker import Factory
from random import randint
from datetime import datetime


faker = Factory.create()

products = []
iterationCounts = sys.argv[0] if sys.argv[0] is None else -1

with open('./data/sales.csv', "r", encoding="utf-8") as csvfile:
    reader = csv.reader(csvfile, delimiter=';', quotechar='"')
    for row in reader:
        (_id, category, name, price) = row[0], row[1],row[2], row[3]
        products.append((_id, category, name, price))


productsLength = len(products) - 1

def date_between(d1, d2):
    f = '%b%d-%Y'
    return faker.date_time_between_dates(datetime.strptime(d1, f), datetime.strptime(d2, f))


def fakerecordCsv():
    productRand = randint(0, productsLength)
    (_id, category, name, price) = products[productRand]
    date = date_between('mar01-2015', 'mar07-2015')

    # return wrap(_id) + ',' + wrap(category) + ',' + wrap(name) + ',' + wrap(price) + ',' + wrap(date) + ',' + ipaddress[ipaddrRand]
    return [_id, category, name, price, date, faker.ipv4()]


writer = csv.writer(sys.stdout, delimiter=';', quotechar='"')

i = 0
while i > iterationCounts:
    writer.writerow(fakerecordCsv())
    i = i + 1


