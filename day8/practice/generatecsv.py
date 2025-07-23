import csv
import random

names = ["Alice", "Bob", "Charlie", "Dave", "Eva", "Frank", "Grace", "Hank", "Ivy", "Jack"]
countries = ["India", "USA", "Germany", "Brazil", "Canada"]

with open("data.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name", "country", "value"])
    
    for i in range(1, 1001):
        writer.writerow([
            i,
            random.choice(names),
            random.choice(countries),
            random.randint(100, 500)
        ])
