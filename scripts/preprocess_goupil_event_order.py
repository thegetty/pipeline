import csv

with open("/home/vouidaskis/PycharmProjects/getty/pipeline/data/goupil/goupil_0.csv", "r") as input_f:
    with open("/home/vouidaskis/PycharmProjects/getty/pipeline/data/goupil/goupil_0_out.csv", "w") as output_f:
        reader = csv.reader(input_f)
        writer = csv.writer(output_f, lineterminator="\n")
        header = next(reader)
        header.append("last")
        header.append("start")
        header.append("saled")
        header.append("two")
        header.append("three")
        header.append("four")
        header.append("five")
        header.append("six")
        header.append("seven")
        writer.writerow(header)

OBJECT_ID_COL = 34
EVENT_ORDER_COL = 35

start = {}
occurrences = {}
lowest = {}
x2={}
x3={}
x4={}
x5={}
x6={}
x7={}

with open("/home/vouidaskis/PycharmProjects/getty/pipeline/data/goupil/goupil.csv", "r") as input_f:
    reader = csv.reader(input_f)

    for row in reader:
        if row[1]=='G-24565':
            print("dddd")
        if row[OBJECT_ID_COL] not in occurrences:
            occurrences[row[OBJECT_ID_COL]] = row[EVENT_ORDER_COL]

            occurrences['max'] = row[EVENT_ORDER_COL]
            start[row[OBJECT_ID_COL]] = f'{row[2]},{row[4]},{row[5]}'
            lowest[row[OBJECT_ID_COL]] = f'{row[2]},{row[4]},{row[5]}'
        if row[OBJECT_ID_COL] in occurrences and row[EVENT_ORDER_COL] > occurrences[row[OBJECT_ID_COL]]:
            occurrences[row[OBJECT_ID_COL]] = row[EVENT_ORDER_COL]
            occurrences['max'] = row[EVENT_ORDER_COL]
            lowest[row[OBJECT_ID_COL]] = f'{row[2]},{row[4]},{row[5]}'
        if row[OBJECT_ID_COL] in occurrences and row[EVENT_ORDER_COL] == 1:
            occurrences['min'] = row[EVENT_ORDER_COL]
            start[row[OBJECT_ID_COL]] = f'{row[2]},{row[4]},{row[5]}'


        if  int(row[EVENT_ORDER_COL])==2 :
            x2[row[OBJECT_ID_COL]] = f'{row[2]},{row[4]},{row[5]}'
        elif int(row[EVENT_ORDER_COL]) == 3:
            x3[row[OBJECT_ID_COL]] = f'{row[2]},{row[4]},{row[5]}'
        elif int(row[EVENT_ORDER_COL]) == 4:
            x4[row[OBJECT_ID_COL]] = f'{row[2]},{row[4]},{row[5]}'
        elif int(row[EVENT_ORDER_COL]) == 5:
            x5[row[OBJECT_ID_COL]] = f'{row[2]},{row[4]},{row[5]}'
        elif int(row[EVENT_ORDER_COL]) == 6:
            x6[row[OBJECT_ID_COL]] = f'{row[2]},{row[4]},{row[5]}'
        elif int(row[EVENT_ORDER_COL]) == 7:
            x7[row[OBJECT_ID_COL]] = f'{row[2]},{row[4]},{row[5]}'


    # reset file pointer to read the file again and add the last column
    input_f.seek(0)


    with open("/home/vouidaskis/PycharmProjects/getty/pipeline/data/goupil/goupil_out.csv", "w") as output_f:
        writer = csv.writer(output_f, lineterminator="\n")
        rows = []

        for row in reader:
            row.append(str(occurrences[row[OBJECT_ID_COL]] == row[EVENT_ORDER_COL]))
            if str(occurrences[row[OBJECT_ID_COL]] == row[EVENT_ORDER_COL]):
                row.append(start[row[OBJECT_ID_COL]])
                row.append(lowest[row[OBJECT_ID_COL]])
                if x2!={} and row[OBJECT_ID_COL] in x2:
                    row.append(x2[row[OBJECT_ID_COL]])
                else:
                    row.append("")
                if x3 != {} and row[OBJECT_ID_COL] in x3:
                   row.append(x3[row[OBJECT_ID_COL]])
                else:
                   row.append("")
                if x4 != {} and row[OBJECT_ID_COL] in x4:
                   row.append(x4[row[OBJECT_ID_COL]])
                else:
                    row.append("")
                if x5 != {} and row[OBJECT_ID_COL] in x5:
                   row.append(x5[row[OBJECT_ID_COL]])
                else:
                    row.append("")
                if x6 != {} and row[OBJECT_ID_COL] in x6:
                   row.append(x6[row[OBJECT_ID_COL]])
                else:
                    row.append("")
                if x7 != {} and row[OBJECT_ID_COL] in x7:
                   row.append(x7[row[OBJECT_ID_COL]])
                else:
                    row.append("")


            rows.append(row)

        writer.writerows(rows)
