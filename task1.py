import pywren_ibm_cloud as pywren
import numpy as np
from io import StringIO


__author__      = "Geovanny Risco y Damian Maleno"
bucketname = 'geolacket' #nombre del bucket en el IBM cloud, 'geolacket'or 'damianmaleno'

def random_matrix(m,n):
    return np.random.randint(100, size=(m,n))

def multiply_matrix_sequencial(matrixA,matrixB):
    result = [[0 for col in range(len(matrixA))] for row in range(len(matrixB[0]))]
    # iterate through rows of X
    for i in range(len(matrixA)):
    # iterate through columns of Y
        for j in range(len(matrixB[0])):
            # iterate through rows of Y
            for k in range(len(matrixB)):
                result[i][j] += matrixA[i][k] * matrixB[k][j]
    return result


"""
Divides matrix A into submatrices of a x n (row-wise) and matrix B into submatrices of n x a (column-wise) 
and uploads each submatrix to COS.
"""
def inicializacion(bucketname, matrixA, matrixB, nworkers, ibm_cos):
    
    iterdata=[]
    if nworkers <= len(matrixA):
        a=len(matrixA)//nworkers #define how many chunks we have to divide the matrix into. It must be the same number as len(matrixB[0])/nworkers.
        #mod=len(matrixA)%nworkers #Para cuando el tamaño de la matriz no es divisible por la nº de workers
        dividirMatrizB=False
    else:      #COMPROBAR: Si no, nworkers == m*l (Se ha comprobado previamente que nworkers no pueda estar entre m y m*l) 
        a=1
        #mod=0
        dividirMatrizB=True
    
    
      
    #Matrix A division
    nfitxer=1 #variable para mantener el nombre de cada fichero en orden normal ascendente
    for i in range(0,nworkers*a, a):  # range(start,end,step). Ex: range(0,10,2) -> [0,2,4,6,8]
        ibm_cos.put_object(Bucket=bucketname, Key="A({},:).txt".format(nfitxer), Body=np.array2string(matrixA[i:i+a]).translate(str.maketrans("", "", "[]"))) #se tiene que pasar el array a una string para poder ser guardada como "Body", el translate es para eliminar los [] que añade el array2string
        nfitxer+=1
    ibm_cos.put_object(Bucket=bucketname, Key="A({},:).txt".format(nfitxer), Body=np.array2string(matrixA[i:len(matrixA)]).translate(str.maketrans("", "", "[]"))) #De esta manera las filas que sobren estarán en el ultimo fichero
    #FUNCIONA BIEN ASI, pero la ultima submatriz se sube dos veces: durante la ultima iteración en en el bucle y despues es sustituida (linea de arriba)
    #Se solucionaría cambiando el for por un while

    #MatrixB division
    matrixB_trans=np.transpose(matrixB) #Transposed Matrix B, dado que tiene que ser divida column-wise
    if dividirMatrizB:
        nfitxer=1
        for i in range(0, nworkers*a, a):
            ibm_cos.put_object(Bucket=bucketname, Key="B({},:).txt".format(nfitxer), Body=np.array2string(matrixB_trans[i:i+a]).translate(str.maketrans("", "", "[]")))
            nfitxer+=1
        ibm_cos.put_object(Bucket=bucketname, Key="B({},:).txt".format(nfitxer), Body=np.array2string(matrixB_trans[i:len(matrixB_trans)]).translate(str.maketrans("", "", "[]"))) #De esta manera las filas que sobren estarán en el ultimo fichero

        
        for i in range(1, len(matrixA)+1):
            for j in range(1, len(matrixB_trans)+1):
                iterdata.append({"A": 'A({},:).txt'.format(i), "B": 'B({},:).txt'.format(j)})
    else:    
        ibm_cos.put_object(Bucket=bucketname, Key="B(*,:).txt", Body=np.array2string(matrixB_trans).translate(str.maketrans("", "", "[]")))
        for i in range(1, nworkers+1):
            iterdata.append({"A": 'A({},:).txt'.format(i), "B": "B(*,:).txt"}) #Se sube la matriz B entera

    return iterdata


"""
TODO:añadir descripcion
"""
def map_multiply_matrix(A, B, ibm_cos):
    
    #submatrixA=[]
    #for nom_fitxer in A:
    submatrixA=ibm_cos.get_object(Bucket=bucketname, Key=A)['Body'].read().decode('utf-8') #junta toda las submatrices A que a este worker le toque multiplicar
    #submatrixA = "\n".join(submatrixA) #Para juntar cada submatriz en una linea diferente
    submatrixA = np.genfromtxt(StringIO(submatrixA),dtype=int) #convierte la submatriz total en una matriz del tipo numpy (array) -> necesario para hacer la multiplicacion con numpy.dot

    #submatrixB=[]
    #for nom_fitxer in B:
    submatrixB=ibm_cos.get_object(Bucket=bucketname, Key=B)['Body'].read().decode('utf-8')
    #submatrixB = "\n".join(submatrixB)
    submatrixB = np.genfromtxt(StringIO(submatrixB),dtype=int)
    submatrixB = np.transpose(submatrixB)

    return submatrixA.dot(submatrixB)

"""
TODO:añadir descripcion

"""

"""TODO: PENSAR COMO JUNTAR LOS RESULTADOS DE CADA WORKER EN UNA MATRIZ C (seria necesario pasarle m y l )"""
def reduce_matrix(results,ibm_cos):
    i=1
    data=[]
    for subresult in results:
        ibm_cos.put_object(Bucket=bucketname, Key="C({}).txt".format(i), Body=np.array2string(subresult).translate(str.maketrans("", "", "[]")))
        data.append(ibm_cos.get_object(Bucket=bucketname, Key="C({}).txt".format(i))['Body'].read().decode('utf-8'))
        i+=1

    return data 



if __name__ == '__main__':

    """TODO: Hacer comprobaciones:
            - Pedir tamaño de matrices
            - La matriz no puede tener un tamaño mayor de 100
            - Comprobar que las matrices se puedan multipicar, es decir: m x n y n x p (n tienen que ser iguales)
            - Comprobar que el nº de workers requerido no esta entre m y m*l"""
    
    matrixA=random_matrix(3,3)
    print("Matriz A \n", matrixA)
    matrixB=random_matrix(3,3)
    print("Matriz B \n", matrixB)
    result_matrix=multiply_matrix_sequencial(matrixA,matrixB)
    print("Resultado \n", result_matrix)

    nworkers=3
    pw = pywren.ibm_cf_executor()
    pw.call_async(inicializacion, [bucketname, matrixA, matrixB, nworkers])
    iterdata= pw.get_result()
    print(iterdata)
    pw.map_reduce(map_multiply_matrix, iterdata, reduce_matrix)
    print(pw.get_result())


