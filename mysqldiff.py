#!/usr/bin/env python
# -*- coding:utf-8 -*-

import re
import sys


#print 'Usage: mysqldiff dbname1.sql dbname2.sql'

if  len(sys.argv) != 3:
	print '..........................'
	print 'Usage: ./mysqldiff.py dbname1.sql dbname2.sql'
	print '..........................'
	sys.exit()
	

f1=open(sys.argv[1],'r')
f2=open(sys.argv[2],'r')
f1=f1.readlines()
f2=f2.readlines()



def diffList(a,b):
        b3 = [val for val in a if val not in b]
        b4 = [val for val in b if val not in a]
        b5 = [val for val in b if val  in a]
        for i in b3:
                print '\033[32;40m'+i.ljust(40)+'\033[0m'+'\033[33;40m'+"`not found`"+'\033[0m'
        for i in b4:
                print '\033[32;40m'+"`not found`".ljust(40)+'\033[0m'+'\033[33;40m'+i+'\033[0m'
        return b5



def diffString(a,b,c):
        if a !=b:
                #print "对比 "+'\033[1;31;40m'+c+'\033[0m'+" 失败"
                print '\033[1;31;40m'+c+'\033[0m'+" 存在差异"



def tableList(file):
	tab=re.compile('CREATE TABLE')
	tabList=[]
	for line in file: 
		if tab.search(line):
			tabList.append(line.split()[2])
	return tabList

def tableStructure(tableName,f1):
	tabStart=re.compile('CREATE TABLE '+tableName)
	tabEnd=re.compile('\) ENGINE')
	flag= False
	tabStructureList=[]
	for line in f1:
		if tabStart.search(line):
			tabStructureList.append(line.strip())
			flag= True 
		elif flag :
			tabStructureList.append(line.strip())
        		if tabEnd.search(line):
				flag= False	
		

	return tabStructureList



def ColumnKeyList(a):
        akey=[]
        acloumn=[]
        for line in   a[1:(len(a)-1)]:
        #for line in   a:
                if re.search('KEY ',line):
                        akey.append(line)
                else:
                        acloumn.append(line.split()[0])
	
        #acloumn.append(a[(len(a)-1)])
	#print acloumn
        return akey,acloumn


def diffKey(a,b,c):
         aColumnKey=ColumnKeyList(a)
         bColumnKey=ColumnKeyList(b)
         if aColumnKey[0] !=bColumnKey[0]:
                #print "对比表 "+'\033[1;31;40m'+c+'\033[0m'+" 索引失败"
                print "对比表 "+'\033[1;31;40m'+c+'\033[0m'+" 索引存在差异"
                diffList(aColumnKey[0],bColumnKey[0])

def diffColumn(a,b,c):
        aColumnKey=ColumnKeyList(a)
        bColumnKey=ColumnKeyList(b)
	 #print aColumnKey
	#print bColumnKey
        if aColumnKey[1] != bColumnKey[1]:
               #print "对比表 "+'\033[1;31;40m'+c+'\033[0m'+" 字段失败"
               print "对比表 "+'\033[1;31;40m'+c+'\033[0m'+" 字段存在差异"
               ab=diffList(aColumnKey[1],bColumnKey[1])
        for linea in a:
                for lineb in b:
                        if lineb.split()[0]==linea.split()[0] and  linea!=lineb and not  re.search('KEY ',linea):
                                print '\033[32;40m'+linea.ljust(40)+'\033[0m'+'\033[33;40m'+lineb+'\033[0m'
def diffEC(a,b,c):        
	if a[len(a)-1] != b[len(b)-1]:
		print "对比表 "+'\033[1;31;40m'+c+'\033[0m'+" 引擎或字符集存在差异"
		print  '\033[32;40m'+a[len(a)-1].replace(') ','').strip().ljust(40)+'\033[0m' +  '\033[33;40m'+b[len(b)-1].replace(') ','').strip()+'\033[0m'

def triggerList(file):
        triggerRE=re.compile(".*"+"CREATE"+".*"+"trigger")
        triggerList=[]
        for line in file:
                if triggerRE.search(line):
                        triggerList.append(line.split()[6])
        return triggerList

def triggerStructure(triggerName,f1):
        triggerStart=re.compile(".*"+"CREATE"+".*"+"trigger "+triggerName+" ")
        #triggerEnd=re.compile('end \*/;;')
        triggerEnd=re.compile('DELIMITER ;')
        flag= False
        triggerStructureList=[]
        for line in f1:
                if triggerStart.search(line):
                        triggerStructureList.append(line.strip())
                        flag= True
                elif flag and line.strip() !='':
                        triggerStructureList.append(line.strip())
                        if triggerEnd.search(line):
                                flag= False

        return triggerStructureList



def procedureList(file):
        procedureRE=re.compile("CREATE"+".*"+"PROCEDURE ")
        procedureList=[]
        for line in file:
                if procedureRE.search(line):
                        procedureList.append(line.split()[3].split('(')[0])
        return procedureList

def procedureStructure(procedureName,f1):
        procedureStart=re.compile("CREATE"+".*" +"PROCEDURE "+procedureName)
        #procedureEnd=re.compile('END ;;')
        procedureEnd=re.compile('DELIMITER ;')
        flag= False
        procedureStructureList=[]
        for line in f1:
                if procedureStart.search(line):
                        procedureStructureList.append(line.strip())
                        #print line
                        flag= True
                elif flag and line.strip() !='':
                        procedureStructureList.append(line.strip())
                        #print line
                        if procedureEnd.search(line):
                                flag= False

        return procedureStructureList





def viewList(file):
        viewRE=re.compile("/*!50001 VIEW")
        viewList=[]
        for line in file:
                if viewRE.search(line):
                        viewList.append(line.split()[2])
        return viewList


def viewStructure(viewName,f1):
        viewStart=re.compile("/*!50001 VIEW "+viewName)
        #viewEnd=re.compile('END ;;')
        flag= False
        viewStructureList=[]
        for line in f1:
                if viewStart.search(line):
                        viewStructureList.append(line.strip())

        return viewStructureList




print ""
#print sys.argv[1].ljust(40),sys.argv[2]
print sys.argv[1].rjust(6),sys.argv[2].rjust(40)
#########################对比表#########################

atabList=tableList(f1)
btabList=tableList(f2)
print "*"*20+" 对比表 ".ljust(37,'*')
#print "."*20+" 对比表 ".ljust(34,'.')

abtabList=diffList(atabList,btabList)
print ""
print ""
print "*"*18+" 对比表结构 ".ljust(41,'*')
#print "."*18+" 对比表结构 ".ljust(38,'.')
for tab in abtabList:
        atab=tableStructure(tab,f1)
        btab=tableStructure(tab,f2)
        diffKey(atab,btab,tab)
        diffColumn(atab,btab,tab)
	diffEC(atab,btab,tab)
#	if atab != btab:
#		print ""


print ""
print ""
print ""


##########################对比triggers##################

atriggerList=triggerList(f1)
btriggerList=triggerList(f2)
#print "="*18+" 对比triggers ".ljust(38,'=')
print "*"*18+" 对比triggers ".ljust(38,'*')
abtriggerList=diffList(atriggerList,btriggerList)
print ""
print ""
#print "==== 对比trigger 结构"
#print "="*16+" 对比triggers结构 ".ljust(42,'=')
print "*"*16+" 对比triggers结构 ".ljust(42,'*')
for triggerName in abtriggerList:
        atrigger=triggerStructure(triggerName,f1)
        btrigger=triggerStructure(triggerName,f2)
        diffString(atrigger,btrigger,triggerName)

print ""
print ""
#print "================================================================="

#######################对比存储过程##################

aprocedureList=procedureList(f1)
bprocedureList=procedureList(f2)
#print "="*18+" 对比procedures ".ljust(38,'=')
print "*"*18+" 对比procedures ".ljust(38,'*')
abprocedureList=diffList(aprocedureList,bprocedureList)
print ""
print ""

#print "="*16+" 对比procedure结构 ".ljust(42,'=')
print "*"*16+" 对比procedure结构 ".ljust(42,'*')
for procedureName in abprocedureList:
        aprocedure=procedureStructure(procedureName,f1)
        bprocedure=procedureStructure(procedureName,f2)
       #print atab
        #diffList(aprocedure,bprocedure)
        diffString(aprocedure,bprocedure,procedureName)
#	if aprocedure !=  bprocedure:
#		print ""

print ""
print ""
#print "================================================================="
#########################对比views#####

aviewList=viewList(f1)
bviewList=viewList(f2)
#print "="*18+" 对比views ".ljust(38,'=')
print "*"*18+" 对比views ".ljust(38,'*')
abviewList=diffList(aviewList,bviewList)
print ""
print ""
#viewList(f1)
#viewStructure('`sponsorview`',f1)
#print "="*16+" 对比view结构 ".ljust(42,'=')
print "*"*16+" 对比view结构 ".ljust(42,'*')
for viewName in abviewList:
        aview=viewStructure(viewName,f1)
        bview=viewStructure(viewName,f2)
        if aview != bview:
                diffString(aview,bview,viewName)

print ""
print ""
