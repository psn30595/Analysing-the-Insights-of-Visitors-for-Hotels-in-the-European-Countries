# -*- coding: utf-8 -*-
"""
Created on Mon Aug  5 11:19:37 2018

@author: psn30595
"""

#Cleaning of the dataset

#Importing packages
import pandas as pd
import numpy as np

#Importing the file in python
df = pd.read_csv("E:/PDA/Hotel_Reviews.csv")

df.head() #Showing output for the first 5 rows

#Removing Spaces in the Dataframe and replacing them with underscores 
df.columns = df.columns.str.replace(' ', '_')
df.columns = [x.strip().replace(' ', '_') for x in df.columns]


#Removing NA values
df.isnull().any().any() #Checking NA values in the entire dataframe
df.isnull() #Checking NA values for each and every column
df.isnull().sum() #Checking the total number of NA values for each and every column
df.shape #Displays the dataframe's rows and columns in numerical form
df_drop = df.dropna() #dropping the NA values from the data frame
#df_drop.shape #No. of columns and rows of the dataframe after dropping the NA values



#Saving the output i.e. cleaned dataset
df_drop.to_csv("E:/PDA/Hotel_reviews.csv", sep=',') 
