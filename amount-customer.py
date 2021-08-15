from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AmountCustomer")
sc = SparkContext(conf = conf)

def parsedLines(line):
    fields = line.split(',')
    customerID = fields[0]
    dollarAmount = fields[2]
    return (int(customerID), float(dollarAmount))

lines = sc.textFile("./data/customer-orders.csv")
parsedLines = lines.map(parsedLines)
total_customer = parsedLines.reduceByKey(lambda x, y: x + y)
total_customer_amount_sort = total_customer.map(lambda x: (x[1], x[0])).sortByKey()

for i in total_customer_amount_sort.collect():
    print(i)
