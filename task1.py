import pywren_ibm_cloud as pywren
import numpy as np
from io import StringIO
import time

__author__      = "Geovanny Risco y Damian Maleno"
__credits__     = ["Geovanny Risco", "Damian Maleno"]
__version__     = "1.0"
__email__       = ["geovannyalexan.risco@estudiants.urv.cat", "franciscodamia.maleno@estudiants.urv.cat"]
__status__      = "Finished"


bucketname = 'geolacket' #nombre del bucket en el IBM cloud, 'geolacket'or 'damianmaleno'
MAX_NUMBER=100          # Valor máximo para las matrices aleatorias
MAX_LINE_WIDTH=1000000  # Necesario al trabajar con matrices de un tamaño muy grande -> array2string inserta un salto de linea al llegar a este valor máximo,
MAX_ARRAY_ITEMS=1000000 #                                                                que por defecto es 75 (numpy.get_printoptions()['linewidth'])
                        #                                                                El nº máximo de elementos es de 1000 (numpy.get_printoptions()['threshold']),
                        #                                                                por tanto es también necesario cambiarlo.

def random_matrix(m,n):
    return np.random.randint(MAX_NUMBER, size=(m,n))


def inicializacion(bucketname, matrixA, matrixB, nworkers, ibm_cos):
    """
    Divides matrix A into submatrices of a x n (row-wise) and matrix B into submatrices of n x a (column-wise) 
    and uploads each submatrix to COS.
    Divides matrices in function of the number of workers given by parameter (nworkers):
    -In case the number of workers is equal to the number of rows of matrix A, each worker will be asigned with the multiplication of
    one row of matrixA times matrixB.
    -In case the number of workers is lower than the number of rows of matrix A, matrix A will be divided into equal sized chunks,
    in a worst case scenario the last chunk will hold the biggest number of rows due to the remainder of the division.
    -In case the number of workers is equal to rows of matrixA times colums of matrixB, each worker will get: one row of matrixA 
    (divided row-wise) times one column of matrixB (divided column-wise), representing the case where workers get the lightest
    work load.

    :param bucketname: nombre del bucket de ibmcloud
    :param matrixA: matriz A generada con valores aleatorios
    :param matrixB: matriz B generada con valores aleatorios
    :param ibm_cos: instancia de ibm_boto3.CLient(), necesaria para subir y descargar archivos del ibm cloud
    :returns iterdata: diccionario contenedor del nombre de las submatrices generadas por el método

    """
    
    iterdata=[]
    if nworkers <= len(matrixA):
        a=len(matrixA)//nworkers # define cuantas filas le corresponderá a cada submatriz
        dividirMatrizB=False
    else:      #Se ha comprobado previamente que nworkers no pueda estar entre m y m*l
        a=1
        dividirMatrizB=True
     
      
    #DIVISION DE LA MATRIZ A 
    nfitxer=1 #variable para mantener el nombre de cada fichero en orden normal ascendente
    i=0
    while i < (nworkers-1)*a and i < len(matrixA)-1: #cada submatriz tendrá "a" filas. La condición de i<len(matrixA)-1 es necesaria para que no cree mas ficheros que 
                                                     #filas tiene A (porque si nworkers es m*l la 1º condición no es suficiente)
        ibm_cos.put_object(Bucket=bucketname, Key="A({},:).txt".format(nfitxer), 
                        Body=np.array2string(matrixA[i:i+a],max_line_width=MAX_LINE_WIDTH,threshold=MAX_ARRAY_ITEMS).translate(str.maketrans("", "", "[]"))) 
                        #se tiene que pasar el array a una string para poder ser guardada como "Body", el translate es para eliminar los [] que añade el array2string
        i+=a
        nfitxer+=1
    ibm_cos.put_object(Bucket=bucketname, Key="A({},:).txt".format(nfitxer), 
                        Body=np.array2string(matrixA[i:len(matrixA)],max_line_width=MAX_LINE_WIDTH,threshold=MAX_ARRAY_ITEMS).translate(str.maketrans("", "", "[]"))) 
                        #La ultima submatriz se hace fuera del bucle por si quedan filas que sobren, las cuales las tratará el último worker


    #DIVISION DE LA MATRIZ B
    matrixB_trans=np.transpose(matrixB) #Matrix B transpuesta de manera que se utiliza el mismo código que con la matriz A. En el ibm cloud estará la submatriz transpuesta. 
                                        #Esto se tiene que volver a transponer al realizar la multiplicación.
    if dividirMatrizB:
        nfitxer=1
        i=0
        while i < (nworkers-1)*a and i < len(matrixB_trans)-1:
            ibm_cos.put_object(Bucket=bucketname, Key="B(:,{}).txt".format(nfitxer), 
                        Body=np.array2string(matrixB_trans[i:i+a],max_line_width=MAX_LINE_WIDTH,threshold=MAX_ARRAY_ITEMS).translate(str.maketrans("", "", "[]")))
            i+=a
            nfitxer+=1
        ibm_cos.put_object(Bucket=bucketname, Key="B(:,{}).txt".format(nfitxer), 
                        Body=np.array2string(matrixB_trans[i:len(matrixB_trans)],max_line_width=MAX_LINE_WIDTH,threshold=MAX_ARRAY_ITEMS).translate(str.maketrans("", "", "[]"))) 

        
        for i in range(1, len(matrixA)+1):
            for j in range(1, len(matrixB_trans)+1): #Cada fila de A se multiplicará por cada uno de las columnas de B
                iterdata.append({"A": 'A({},:).txt'.format(i), "B": 'B(:,{}).txt'.format(j), "C": 'C({},{})'.format(i,j)})
    else:    
        ibm_cos.put_object(Bucket=bucketname, Key="B(:,*).txt", Body=np.array2string(matrixB_trans,max_line_width=MAX_LINE_WIDTH,threshold=MAX_ARRAY_ITEMS).translate(str.maketrans("", "", "[]")))
        for i in range(1, nworkers+1):
            iterdata.append({"A": 'A({},:).txt'.format(i), "B": "B(:,*).txt", "C": 'C({},:)'.format(i)}) #Se sube la matriz B entera

    return iterdata



def map_multiply_matrix(A, B, C, ibm_cos):
    """
    Realiza la multiplicación de cada submatriz A con su corresponiendo submatriz B. Esta función se ejecuta en paralelo, es decir, cada worker esta ejecutando esta misma función por separado
    y de manera independiente. 
    :param A: nombre (key) de la submatriz A en el bucket del ibm cloud
    :param B: nombre (key) de la submatriz A en el bucket del ibm cloud
    :param C: posición que ocupará el resultado en la matriz C final
    :param ibm_cos: instancia de ibm_boto3.CLient(), necesaria para subir y descargar archivos del ibm cloud
    :returns: diccionario con el parámetro C sin modificar y el resultado de multiplicar la submatriz A y B
    """
    #Obtener submatriz A
    submatrixA=ibm_cos.get_object(Bucket=bucketname, Key=A)['Body'].read().decode('utf-8') #junta toda las submatrices A que a este worker le toque multiplicar
    submatrixA = np.genfromtxt(StringIO(submatrixA),dtype=int) #convierte la submatriz total en una matriz del tipo numpy (array) -> necesario para hacer la multiplicacion con numpy.dot

    #Obtener submatriz B
    submatrixB=ibm_cos.get_object(Bucket=bucketname, Key=B)['Body'].read().decode('utf-8')
    submatrixB = np.genfromtxt(StringIO(submatrixB),dtype=int)
    submatrixB = np.transpose(submatrixB) #Es necesario transponerla para que vuelva al estado original

    return {'C': C, 'res': submatrixA.dot(submatrixB)} 


def reduce_matrix(results,ibm_cos):
    """
    Recibe los resultados de los workers y los junta en una matriz C. El resultado de cada worker va llegando en el mismo orden que el iterdata.
    :param results: diccionario con el parámetro C, el cual contiene la posición que ocupará el resultado en la matriz C, y el resultado de multiplicar la submatriz A y B
    :param ibm_cos: instancia de ibm_boto3.CLient(), necesaria para subir y descargar archivos del ibm cloud
    :returns: matriz C
    """
    matrixC=[]
    for subresult in results:
        (fila,col)=subresult['C'].strip('C()').split(',') #Teniendo en cuenta el formato: C(fila,columna)
        fila=int(fila)
        if (col==':'):
            matrixC.insert(fila,np.array2string(subresult['res'],max_line_width=MAX_LINE_WIDTH,threshold=MAX_ARRAY_ITEMS).translate(str.maketrans("", "", "[]")))
        else:
            if fila-1 >= len(matrixC): #La primera vez que llega el valor de una fila hay que crear una lista, que correspondrá a una de las filas de la matriz C
                matrixC.append("")
            matrixC[fila-1]=matrixC[fila-1]+" "+np.array2string(subresult['res'],max_line_width=MAX_LINE_WIDTH,threshold=MAX_ARRAY_ITEMS).translate(str.maketrans("", "", "[]")) 
            #Podemos hacer esto porque los resultados van llegando en orden (El iterdata esta ordenado)
    ibm_cos.put_object(Bucket=bucketname, Key="C.txt", Body='\n'.join(matrixC))
    
    return matrixC


if __name__ == '__main__':

    #PETICIONES Y COMPROBACION DE DATOS:
    rowsA = int(input("Number of rows of matrix A: "))
    columnsA = int(input("Number of colums of matrix A: "))
    print("There will be",columnsA,"columns in matrix B so that they can be multiplied")
    columnsB = int(input("Number of columns of matrix B: ")) 
    rowsB = columnsA
    
    print("\nGenerating matrices randomdly...")
    matrixA=random_matrix(rowsA,columnsA)
    print("Matriz A \n", matrixA)
    matrixB=random_matrix(rowsB,columnsB)
    print("Matriz B \n", matrixB)

    nworkers = int(input("Number of workers: "))
    while 1<=nworkers>100 or rowsA<nworkers<rowsA*columnsB:
        print("\nNumber of workers should be a number between 1 and 100")
        nworkers = input("Number of workers: ")
    
    #OPERACIONES EN IBM CLOUD:
    pw = pywren.ibm_cf_executor()
    pw.call_async(inicializacion, [bucketname, matrixA, matrixB, nworkers])
    iterdata= pw.get_result()
    #print(iterdata)
    start_time= time.time() #Para calcular el tiempo de calculo de pywren
    futures = pw.map_reduce(map_multiply_matrix, iterdata, reduce_matrix)
    pw.wait(futures) # wait for the completion of map_reduce() call
    elapsed_time = time.time() - start_time
    print(pw.get_result())
    print("Tiempo total: ",elapsed_time,"s")

    


