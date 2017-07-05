import numpy as np

# A Person is a Node (has an id and purchase history)
class Person:    
    
    def __init__(self, index = None):
        self.index          = index
        self.purchase_array = np.empty(0,dtype='double')
        self.time_array     = np.empty(0)
        self.anom_amount    = 10000000000.0 # some large number
        self.mean           = 10000000000.0
        self.std            = 10000000000.0

    def setIndex(self, index):
        self.index = index        
        
    def setAnomAmount(self, amount):
        self.anom_amount = amount
        
    def setMean(self, mean):
        self.mean = mean
        
    def setSTD(self, std):
        self.std = std
        
        
    # Horribly inefficient, but what to do?    
    def addPurchase(self,value,timestamp):
        self.purchase_array = np.append(self.purchase_array,value)
        self.time_array     = np.append(self.time_array,timestamp)
        
    def showPurchase(self):
        print self.purchase_array
        
    def showTime(self):
        print self.time_array
