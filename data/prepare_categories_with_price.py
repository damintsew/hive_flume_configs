import csv
import sys
import random
import re

_dict = set()
mu, sigma = 10000, 4000 # mean and standard deviation


def randGauss():
    val = abs(random.gauss(mu, sigma)) #abs for safety
    return "{0:.2f}".format(val)


with open('category_2.csv', "r", encoding="utf-8") as csvfile:
     reader = csv.reader(csvfile, delimiter=';')
     writer = csv.writer(sys.stdout, delimiter=';', quotechar='"')

     for row in reader:
         m = re.search('(.+?) \/ (.*)', row[4])
         if m:
             filmName = m.group(1)
         else:
             continue

         writer.writerow([row[2], row[1], filmName, randGauss()])

