import pywren_ibm_cloud as pywren

def add_seven(x):
    return x + 7

if __name__ == '__main__':
    ibmcf = pywren.ibm_cf_executor()
    ibmcf.call_async(add_seven, 4)
    print(ibmcf.get_result())