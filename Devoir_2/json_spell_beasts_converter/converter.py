to_convert = open("toconvert.json")
converted = open("converted.json", "w")

converted.write("[")

line = to_convert.readline()
while line:
    converted.write(line[:-1])
    line = to_convert.readline()
    if line:
        converted.write(",\n")

converted.write("]")