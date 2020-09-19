import glob
import ast


docReader = open('lincoln.txt', encoding='UTF8')


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
    namePartitionFile = 'partition-' + str(cont) + '.txt'
    open(namePartitionFile, 'w').write(lines)
    partitionFileNamesList.append(namePartitionFile)



import threading
import time
import re
import sys

#Mapper Function
def map(indexStartFilesToMap, indexEndFilesToMap):

    # Read Files in the range that input in parameters
    # Later create new files join all the files that input in parameters
    if(indexStartFilesToMap != indexEndFilesToMap):
        with open('map-'+str(indexEndFilesToMap)+'.txt','a') as mapFile:
            for index in range(indexStartFilesToMap, indexEndFilesToMap):
                nameFileTemp = partitionFileNamesList[index]
                print('Nombres archivos analizandose: '+nameFileTemp+'------------')
                textTemp = str(open(nameFileTemp, 'r').read().lower())
                textTemp = re.sub(r'[^\w\s]','',textTemp) #Remove punctuations from text
                listWords = textTemp.split()#esto lista todas las palabras y remueve el signo de salto de linea
                # print('despues-----'+str(listWords.__len__()))
                for words in listWords:
                    textMapTemp = words +' , ' + '1\n'
                    mapFile.write(textMapTemp)
    else:
        nameFileTemp = partitionFileNamesList[indexStartFilesToMap]
        print('nombres archivos: ' + nameFileTemp + '------------')
        textTemp = open(str(nameFileTemp),'r').read()
        open('map-'+str(indexStartFilesToMap)+'.txt','w').write(str(textTemp))
    mapFile.close()
    sys.exit()


#Combiner
def combiner(indexEndFilesToMap):
    print('Analizando el archivo: '+'combiner-'+str(indexEndFilesToMap)+'.txt')
    wordDictionary = {}
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

    mapperFile.close()
    combinerFile.close()


def shuffle(listThreads, nameFile):
    listAnalysis = []
    dictTemp = {}
    print('Se van a analizar estos archivos: '+str(listThreads))
    for dataTempFiles in listThreads:
        print('Se esta analizando este archivo: '+dataTempFiles)
        fileTemp = open(dataTempFiles+'.txt','r')
        for textTemp in fileTemp.readlines():
            listTemp = textTemp.split(' , ')
            if not dictTemp.get(listTemp[0]):
                dictTemp[listTemp[0]] = [listTemp[1].replace('\n','')]
            else:
                valor = dictTemp.get(listTemp[0])
                valor.append(listTemp[1].replace('\n',''))
                dictTemp[listTemp[0]] = valor

    fileTempShufle = open('shufle-'+nameFile+'.txt', 'a')
    for k,v in dictTemp.items():
        # key [1,4,6,7]
        fileTempShufle.write(str(k)+":"+str(v)+"\n")

    fileTempShufle.close()


def reducer(fileShufferNumber):
    dictTemp ={}
    reducerTempFile = open('reducer-' + str(fileShufferNumber) + '.txt', 'a')
    fileTemporal = open('shufle-'+str(fileShufferNumber)+'.txt', 'r')
    for dictionary in fileTemporal.readlines():
        key = dictionary.split(':')[0]
        value = dictionary.split(':')[1].replace('\n','').replace('[','').replace(']','').replace('\'','').split(', ')
        tempValue = 0
        for som in value:
            tempValue = tempValue + int(som)

        reducerTempFile.write(str(key) +' : '+ str(tempValue)+'\n')

    reducerTempFile.close()




if __name__== '__main__':
    threadNumbers = 6
    filesListSize = partitionFileNamesList.__len__()
    numberOfTextInThread = int(filesListSize/threadNumbers)
    equalPartitionNumberText = numberOfTextInThread*threadNumbers

    print('Numero de archivos: ',len(partitionFileNamesList))
    print('Numero de textos en cada thread: ',numberOfTextInThread)
    print('Total de thread en la particion: ',equalPartitionNumberText)

    coordinatorThreat = list()
    listEndNumberFiles = []
    listBackUpInformation = {}

    # Creating pool of threads to map in equal distribution of files, arguments start index number and end index number
    countIndexEndFiles = numberOfTextInThread
    countIndexStartFiles = 0

    contEndIndex = numberOfTextInThread
    contStarIndex = 0
    for threadNumber in range(0,threadNumbers):
        mapThread = threading.Thread(target=map, args=(contStarIndex,contEndIndex,))
        print('El hilo del map: ' + mapThread.getName() + ' ha iniciado')
        mapThread.start()
        mapThread.join()
        print('El hilo del map: ' + mapThread.getName() + ' ha finalizado')
        listEndNumberFiles.append(contEndIndex)
        contStarIndex = contStarIndex + numberOfTextInThread
        contEndIndex = contStarIndex + numberOfTextInThread



    if (filesListSize!=equalPartitionNumberText):
        print('Distribucion no equitativa')
        mapThread = threading.Thread(target=map, args=(contStarIndex, filesListSize,))
        print('El hilo del map: ' + mapThread.getName() + ' ha iniciado')
        listEndNumberFiles.append(filesListSize)
        mapThread.start()
        mapThread.join()
        print('El hilo del map: ' + mapThread.getName() + ' ha finalizado')



    #Combiner
    for indexCombiner in listEndNumberFiles:
        combinerThread = threading.Thread(target=combiner, args=(indexCombiner,))
        print('El hilo del combiner: ' + combinerThread.getName() + ' ha iniciado')
        combinerThread.start()
        combinerThread.join()
        print('El hilo del combiner: ' + combinerThread.getName() + ' ha terminado')



    #Shuffle
    listShuffleThread = []
    listFiles = glob.glob('./*.txt')
    separator = ', '
    listCombiner = []
    for nameFile in listFiles:
        if nameFile.__contains__('combiner'):
            listCombiner.append(nameFile.replace('.\\','').replace('.txt',''))

    sizeCombinerList = listCombiner.__len__()


    listCombinerDefinitive = []
    contFileShufferNumber = 1
    if sizeCombinerList%2==0:
        tempListCombiner = []
        middleSizeList = int(sizeCombinerList/2)
        for indexList in range(0,middleSizeList):
            tempListCombiner.append(listCombiner.pop(0))

        listCombinerDefinitive.append(tempListCombiner)
        listCombinerDefinitive.append(listCombiner)


        for dataExtracted in listCombinerDefinitive:
            shuffleThread = threading.Thread(target=shuffle, args=(dataExtracted,str(contFileShufferNumber),))
            listShuffleThread.append(shuffleThread)
            print('El hilo del shuffle: '+shuffleThread.getName()+' ha empezado a ejecutarse')
            shuffleThread.start()
            shuffleThread.join()  #This is main idea to shuffle
            print('El shuffle : '+shuffleThread.getName()+' ha finalizado')
            contFileShufferNumber = contFileShufferNumber + 1

    if sizeCombinerList%2!=0:
        newSizeCombinerList = sizeCombinerList - 1
        tempListCombiner = []
        middleSizeList = int(sizeCombinerList / 2)
        for indexList in range(0,middleSizeList):
            tempListCombiner.append(listCombiner.pop(0))

        listCombinerDefinitive.append(tempListCombiner)
        listCombinerDefinitive.append(listCombiner)

        for dataExtracted in listCombinerDefinitive:
            shuffleThread = threading.Thread(target=shuffle, args=(dataExtracted,str(contFileShufferNumber),))
            listShuffleThread.append(shuffleThread)
            print('El hilo del shuffle: ' + shuffleThread.getName() + ' ha empezado a ejecutarse')
            shuffleThread.start()
            shuffleThread.join()
            print('El shuffle : ' + shuffleThread.getName() + ' ha finalizado')
            contFileShufferNumber = contFileShufferNumber + 1



    # Reducer
    for index in range(1,contFileShufferNumber):
        shuffleCurrentThread = threading.Thread(target=reducer, args=(index,))
        print('Empezando el hilo del reducer: '+shuffleCurrentThread.getName())
        shuffleCurrentThread.start()
        shuffleCurrentThread.join()
        print('Termino el hilo del reducer: ' + shuffleCurrentThread.getName())





    #joining counts
    listaDefinitiva = open('reducer-1.txt').read().split('\n')+open('reducer-2.txt').read().split('\n')
    dictFinal = {}
    for finalData in listaDefinitiva:
        # keyFinal = finalData.replace('\n','').split(' : ')[0]
        # valueFinal = finalData.replace('\n','').split(' : ')[1]
        listaFinal = finalData.replace('\n','').split(' : ')
        # print(listaFinal)
        if listaFinal.__len__()>1:
            keyFinal = listaFinal[0]
            valueFinal = listaFinal[1]

            if not dictFinal.get(keyFinal):
                dictFinal[keyFinal] =  int(valueFinal)
            else:
                temValorFinal = int(dictFinal.get(keyFinal))
                dictFinal[keyFinal] = temValorFinal + int(valueFinal)

    # print(dictFinal)

    sortedDictionary = sorted(dictFinal.items(), key=lambda x:x[1], reverse=True)

    # print(sortedDictionary)
    print('Rango de palabras: ')
    for data in sortedDictionary:
        print(data)








































































        # if not dictTemp.get(listTemp[0]):
        #     dictTemp[listTemp[0]] = [listTemp[1].replace('\n', '')]
        # else:
        #     valor = dictTemp.get(listTemp[0])
        #     valor.append(listTemp[1].replace('\n', ''))
        #     dictTemp[listTemp[0]] = valor











    # cont = 0
    # while cont==6:
    #     for index, thread in enumerate(coordinatorThreat):
    #         nameThread = thread.getName() + ''
    #         print('Estamos en este thread: '+ nameThread)
    #
    #         try:
    #             if thread.is_alive()==False:
    #                 print('Hay un error en el : '+nameThread)
    #                 print('Estamos solucionando el problema ...')
    #                 startIndexTemp = listBackUpInformation[nameThread][0]
    #                 endIndexTemp = listBackUpInformation[nameThread][1]
    #                 tempThread = threading.Thread(target=map, args=(startIndexTemp, endIndexTemp,))
    #                 tempThread.start()
    #                 tempThread.join()
    #                 print('El problema se ha solucionado')
    #                 print('El ' + nameThread + ' ha culminado con exito')
    #
    #         except:
    #             if thread.is_alive():
    #                 thread.join()
    #                 print('El '+nameThread+' ha culminado con exito')
    #
    #     cont = cont + 1


    # contThead = 0
    # while contThead < threadNumbers:
    #     if coordinatorThreat[contThead].is_alive():

    # # This code is is for coordinator
    # failInfo = ''
    # while True:
    #     for x in coordinatorThreat:
    #         print(x.getName())
    #         if x.is_alive() == False:
    #             failInfo = x.getName()
    #             print('Fallo en el hilo: '+failInfo)
    #             break
    #
    #     if failInfo != '':
    #         break





