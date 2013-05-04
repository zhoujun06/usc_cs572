#!/usr/bin/env python

querys = ['Ford', 'Honda', 'Toyota', 'Lincoln', 'Dodge', 'Nissan', 'BMW', 'Jeep', 'Chevrolet', 'Audi']
#querys = ['Ford']

lines = []
google = []
bing = []
ask = []

f1 = open('table1.txt', 'w')
f2 = open('table2.txt', 'w')
f3 = open('jaccard.txt', 'w')
f4 = open('kendall.txt', 'w')
f5 = open('spearman.txt', 'w')

for query in querys:
	fp = open('new'+query+'.txt')
	lines = fp.readlines()
	while "\n" in lines:
		lines.remove("\n")
	newline = [w.strip() for w in lines]
	lines = newline

	google = lines[:10]
	bing = lines[10:20]
	ask = lines[20:30]

	go = [w for w in google if w not in bing and w not in ask]
	bo = [w for w in bing if w not in google and w not in ask]
	ao = [w for w in ask if w not in google and w not in bing]

	gbo = [w for w in google if w in bing and w not in ask]
	gao = [w for w in google if w in ask and w not in bing]
	bao = [w for w in bing if w in ask and w not in google]

	gba = [w for w in google if w in bing and w in ask]

	gb = [w for w in google if w in bing]
	ga = [w for w in google if w in ask]
	ba = [w for w in bing if w in ask]

	bg = [w for w in bing if w in google]
	ag = [w for w in ask if w in google]
	ab = [w for w in ask if w in bing] 

	uniq = float(len(set(lines)))

	f1.write("%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n" % (query, len(go), len(bo), len(ao), len(gbo), len(gao), len(bao), len(gba)))
	f2.write("%s\t%d\t%.4f\t%.4f\t%.4f\n" % (query, uniq, len(go+bo+ao)/uniq, len(gbo+gao+bao)/uniq, len(gba)/uniq))

	gblen = float(len(set(google+bing)))
	galen = float(len(set(google+ask)))
	balen = float(len(set(bing+ask)))

	f3.write("%s\t%.4f\t%.4f\t%.4f\n" % (query, len(gb)/gblen, len(ga)/galen, len(ba)/balen))

#	print google
#	print bing
#	print ask
#	print gb
#	print ga
#	print ba

	gb_ken = 0
	ga_ken = 0
	ba_ken = 0
	for i in range(0, len(gb)):
		for j in range(i+1, len(gb)):
			if bing.index(gb[i]) > bing.index(gb[j]):
				gb_ken += 1

	for i in range(0, len(ga)):
		for j in range(i+1, len(ga)):
			if ask.index(ga[i]) > ask.index(ga[j]):
				ga_ken += 1

	for i in range(0, len(ba)):
		for j in range(i+1, len(ba)):
			if ask.index(ba[i]) > ask.index(ba[j]):
				ba_ken += 1
	
	f4.write("%s\t%.4f\t%.4f\t%.4f\n" %(query, float(gb_ken)/(len(gb)*(len(gb)-1)/2), 
						float(ga_ken)/(len(ga)*(len(ga)-1)/2), 
						float(ba_ken)/(len(ba)*(len(ba)-1)/2)))

	gb_sp = 0
	gb_sp_a = 0
	ga_sp = 0
	ga_sp_a = 0
	ba_sp = 0
	ba_sp_a = 0

	for i in gb:
		gb_sp += abs(gb.index(i) - bg.index(i))
	for i in ga:
		ga_sp += abs(ga.index(i) - ag.index(i))
	for i in ba:
		ba_sp += abs(ba.index(i) - ab.index(i))

	if len(gb) % 2 == 0:
		gb_sp_a = len(gb)*len(gb)/2
	else:
		gb_sp_a = (len(gb)+1)*(len(gb)-1)/2

	if len(ga) % 2 == 0:
		ga_sp_a = len(ga)*len(ga)/2
	else:
		ga_sp_a = (len(ga)+1)*(len(ga)-1)/2

	if len(ba) % 2 == 0:
		ba_sp_a = len(ba)*len(ba)/2
	else:
		ba_sp_a = (len(ba)+1)*(len(ba)-1)/2

	#print len(gb),len(ga),len(ba)
	f5.write("%s\t%.4f\t%.4f\t%.4f\n" %(query, float(gb_sp)/gb_sp_a, float(ga_sp)/ga_sp_a, float(ba_sp)/ba_sp_a))
		

