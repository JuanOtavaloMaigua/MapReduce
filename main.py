docReader = open('pruebaTexto.txt', encoding='UTF8')


# Read all file and write in file of 25 lines of content
partitionFileNamesList = []    #Store names of partitionFiles
cont = 0
lines = ''
lastLines = 0
for data in docReader.readlines():
    lines = lines + data
    #print(cont,data)
    cont = cont + 1
    if (cont % 25)==0:
        namePartitionFile = 'partition-'+str(cont)+'.txt'
        open(namePartitionFile,'w').write(lines)
        partitionFileNamesList.append(namePartitionFile)
        lines = ''
        lastLines = cont

if(lastLines!=cont):
    open('partition-' + str(cont) + '.txt', 'w').write(lines)


# ManagerPollThread ----- Cooordinator thread


import threading
import time
import re
import sys


def map(indexStartFilesToMap, indexEndFilesToMap):

    print(indexStartFilesToMap, indexEndFilesToMap)

    wordDictionary = {}

    print(indexStartFilesToMap, indexEndFilesToMap)
    # Read Files in the range that input in parameters
    # Later create new files join all the files that input in parameters
    if(indexStartFilesToMap != indexEndFilesToMap):
        with open('block-'+str(indexEndFilesToMap)+'.txt','w') as mapFile:
            for index in range(indexStartFilesToMap, indexEndFilesToMap):
                nameFileTemp = partitionFileNamesList[index]
                textTemp = str(open(nameFileTemp, 'r').read().lower())
                textTemp = re.sub(r'[^\w\s]','',textTemp) #Remove punctuations from text
                mapFile.write(textTemp)    #Text lower to analisys every word the same and find words with similarity

    # for readingIndex in range(indexEndFilesToMap, indexEndFilesToMap):
    # If range of parameters are the same
    else:
        nameFileTemp = partitionFileNamesList[indexStartFilesToMap]
        textTemp = open(str(nameFileTemp),'r').read()
        open('map-'+str(indexStartFilesToMap)+'.txt','w').write(str(textTemp))

    # Mapper
    mapperFileAnalisys = open('block-'+str(indexEndFilesToMap)+'.txt','r')
    mapperFile = open('map-'+str(indexEndFilesToMap)+'.txt','w')
    for line in mapperFileAnalisys.readlines():
        partialLineWords = line.replace('\n','').split()
        for words in partialLineWords:
            if words.__len__()>0:
                textTemp = words+' , ' + '1\n'
                mapperFile.write(textTemp)

    #Combiner
    combinerFile = open('combiner-'+str(indexEndFilesToMap)+'.txt','w')
    mapperFile = open('map-' + str(indexEndFilesToMap) + '.txt', 'r')
    for line in mapperFile.readlines():
        textTemp = str(line).replace('\n','').split(' , ')
        wordTemp = textTemp[0]
        number = int(textTemp[1])
        if wordDictionary.__contains__(wordTemp):
            valueTemp = wordDictionary[wordTemp]
            wordDictionary[wordTemp] = valueTemp + number
        else:
            wordDictionary[wordTemp] = number

    for data in wordDictionary.items():
        combinerFile.write(str(data[0])+' , '+str(data[1])+'\n')

    sys.exit()

def reduce():
    print('reduce')

if __name__== '__main__':
    threadNumbers = 6
    filesListSize = partitionFileNamesList.__len__()
    numberOfTextInThread = int(filesListSize/threadNumbers)
    equalPartitionNumberText = numberOfTextInThread*threadNumbers

    print('Numero de archivos: ',partitionFileNamesList.__len__())
    print('Numero de textos en cada thread: ',numberOfTextInThread)
    print('Total de thread en la particion: ',equalPartitionNumberText)

    coordinatorThreat = list()

    # Creating pool of threads to map in equal distribution of files, arguments start index number and end index number
    countIndexEndFiles = numberOfTextInThread
    countIndexStartFiles = 0
    for threadNumber in range(0,threadNumbers):
        if threadNumber!=0:
            countIndexStartFiles = (threadNumber*numberOfTextInThread)+1
        # else:
        #     countIndexStartFiles = (threadNumber * numberOfTextInThread)
        mapThread = threading.Thread(target=map, args=(countIndexStartFiles,countIndexEndFiles,))
        mapThread.start()
        coordinatorThreat.append(mapThread)
        countIndexEndFiles = countIndexEndFiles + numberOfTextInThread

    # Code when no equitative distribution
    if (filesListSize-(equalPartitionNumberText+1)) != 0:
        if (equalPartitionNumberText+1) == filesListSize:
           mapThread = threading.Thread(target=map, args=(filesListSize-1,filesListSize-1,))
        else:
            mapThread = threading.Thread(target=map, args=((equalPartitionNumberText+1), (filesListSize-1),))

        mapThread.start()
        coordinatorThreat.append(mapThread)

        print('Repartición de archivos No equitativa')
    else:
        print('Reparticion de archivos equitativa')
    failInfo = ''
    while True:
        for x in coordinatorThreat:
            print(x.getName())
            if x.is_alive() == False:
                failInfo = x.getName()
                print('Fallo en el hilo: '+failInfo)
                break

        if failInfo != '':
            break





