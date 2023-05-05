# -*- coding: utf-8 -*-

from pyspark import SparkContext
import sys
import string

# Función para contar las palabras de una fila
# primero sustituimos todos los signos de puntuación por espacios
# y luego hacemos "len(line.split()), donde split nos separa por 
# espacios y luego nos quedamos con su longitud para saber el nº de palabras
def word_split(line):
    for c in string.punctuation+"¿!«»":
        line = line.replace(c,' ')
        line = line.lower()
    return len(line.split())

# Función para contar las palabras de un fichero completo.
# Leemos el fichero con spark y luego hacemos un map (por filas) con la función anterior,
# que nos devolverá el nº de palabras por filas. Después sumamos todos esos resultados 
# para quedarnos con el total.
def main(infile, outfile):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        data = sc.textFile(infile)
        words_rdd = data.map(word_split)
        n = words_rdd.sum()
        print(n)
        with open(outfile, "w") as file:
            file.write(str(n))


if __name__=="__main__":
    if len(sys.argv)<3:
        print(f"Usage: {sys.argv[0]} <infilename> <outfilename>")
        print("Por defecto hemmos usado los siguientes valores:") 
        print(" - infilename = quijote_s05.txt") 
        print(" - outfilename = out_quijote_s05.txt") 
        infilename, outfilename = "quijote_s05.txt", "out_quijote_s05.txt"
    else:
        infilename = sys.argv[1]
        outfilename = sys.argv[2]
    main(infilename, outfilename)
