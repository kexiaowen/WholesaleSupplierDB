import csv

orders = csv.reader(open('../4224-project-files/data-files/order.csv'))
records = list(orders)
carrier_id_index = 4

for record in records:
    if record[carrier_id_index] == "null":
        record[carrier_id_index] = "-1"

writer = csv.writer(open('../4224-project-files/data-files/updated-order.csv', 'w'))
writer.writerows(records)


orderlines = csv.reader(open('../4224-project-files/data-files/order-line.csv'))
records = list(orderlines)
delivery_d_index = 5

for record in records:
    if record[delivery_d_index] == "null":
        record[delivery_d_index] = "1970-01-01 00:00:01.001"

writer = csv.writer(open('../4224-project-files/data-files/updated-order-line.csv', 'w'))
writer.writerows(records)