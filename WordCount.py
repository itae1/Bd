text_file = sc.textFile("#paste the spark text file URL")
counts = text_file.flatMap(lambda line: line.split()) \

.map(lambda word: (word, 1)) \

.reduceByKey(lambda a, b: a + b)

counts.collect()
