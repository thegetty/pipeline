
# Error log parser

fh = open('remote_output/log_20190226.txt')

errors = []
curr = []

for l in fh.readlines():
	if l.startswith("ERR."):
		# begin new error
		if curr:
			errors.append(''.join(curr))
		curr = []

	elif l.find('return self.wrapped') > -1:
		curr = []
	else:
		curr.append(l)

errors.append(''.join(curr))
fh.close()

uniq = {}

for e in errors:
	try:
		uniq[e] += 1
	except:
		uniq[e] = 1


out = sorted(list(uniq.keys()))
fh = open('remote_output/error_uniq.txt', 'w')
for o in out:
	fh.write(o)
	fh.write("\n")
fh.close()
