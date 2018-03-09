from collections import Counter
counter = Counter()
text = ['c','a','b','a','a','b']
for item in text:
    counter[item] = counter[item] + 1
result = list(map((lambda x: x[0]), counter.most_common()[:2]))
print(result)
