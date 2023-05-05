import random
import sys
import string
from pyspark import SparkContext

# Con una probabilidad "ratio" nos quedamos con la linea o no. Si es que no, 
# entonces devolvemos la string vacía.
def porcentaje(line, ratio):
    output = ""
    if random.random()<=ratio:
        output = line
    return output

# Leemos un fichero con spark y luego hacemos un map (por filas) de "porcentaje
# finalmente unimos el resultado con "".join(data.collect()) para crear la string y
# la insertamos en el fichero final con file.write()
def main(infile, outfile, ratio):
    with SparkContext() as sc:
        sc.setLogLevel("ERROR")
        data = sc.textFile(infile)
        data2 = data.map(lambda x : porcentaje(x, ratio))
        with open(outfile, "w") as file:
            ss = "".join(data2.collect())
            file.write(ss)
            print("Archivo escrito correctamente!")
            

if __name__=="__main__":
    if len(sys.argv)<4:
        print(f"Usage: {sys.argv[0]} <infilename> <outfilename> <ratio")
        print("Por defecto hemmos usado los siguientes valores:") 
        print(" - infilename = quijote.txt") 
        print(" - outfilename = quijote_s05.txt") 
        print(" - ratio = 0.001") 
        infilename, outfilename, ratio = "quijote.txt", "quijote_s05.txt", 0.001
    else:
        infilename = sys.argv[1]
        outfilename = sys.argv[2]
        ratio = float(sys.argv[3])
    main(infilename, outfilename, ratio)