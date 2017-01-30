#!/usr/bin/python



file=open('queues','r')
for i in file.read().splitlines() :
	out = i.split('.')
#	print i.split('.')
	max =  len(i.split('.'))
	print out[max-1]
