# CursoMineriaDatos
Codigos del Curso Mineria de Datos USFQ 2020
MapReduce ---- Compila perfectamente, se tiene que ingresar el nombre del archivo al principio.
-------------- Es preferible que el archivo no tenga puntos o comas, sin embargo el algoritmo
-------------- elimina todos estos caracteres. En cada proceso del map reduce se crea un archivo
-------------- asi es como se crea, partition, que es la particion del archivo grande a pedazos 
-------------- pequeños que contienen 25 lineas del archivo gigante. Map es el siguiente archivo
-------------- en crearse este archivo contiene todas las palabras separadas y contabilizadas como 1.
-------------- El archivo combiner suma cada palabra de los archivos combiner. Suffle reune la cuenta 
-------------- de palabras de cada combiner. Reduce es el archivo final y contiene la cuenta final
-------------- de cada shuffle. Al final se muestra el resultado final.
