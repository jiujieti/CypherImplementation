from os import listdir
from os.path import isfile, join
from datetime import datetime
import xlsxwriter

dir = "path_of_the_folder/where/you_store_your_log_files"

workbook = xlsxwriter.Workbook(dir + 'results.xlsx')
#range(0, 25) indicate specific query numbers, used to distingush queries
worksheets = {str(i): workbook.add_worksheet('q' + str(i)) for i in range(0, 25)}
rows = {str(i): 0 for i in range(0, 25)}

onlyfiles = [f for f in listdir(dir) if isfile(join(dir, f)) and f.endswith('.txt')]
col = 0
for i in onlyfiles:
    fields = i[:-6].split('-')
    d, p, q = fields[0], fields[1], fields[2]
    worksheet = worksheets[q]
    row = rows[q]
    with open(dir + i, 'r') as fh:
        first = next(fh).decode()[0:19]
        for line in fh:
            pass
        last = line[0:19]
        d2 = datetime.strptime(last, "%m/%d/%Y %H:%M:%S")
        d1 = datetime.strptime(first, "%m/%d/%Y %H:%M:%S")
        tdelta = d2 - d1
        worksheet.write(row, 0, d.rstrip('k'))
        worksheet.write(row, 1, p)
        worksheet.write(row, 2, first)
        worksheet.write(row, 3, last)
        worksheet.write(row, 4, tdelta.total_seconds())
    rows[q] += 1
workbook.close()