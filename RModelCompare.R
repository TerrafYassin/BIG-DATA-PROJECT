library(ggplot2)
library(dplyr)
library(maps)
library(ggmap)
library(mongolite)
library(lubridate)
library(gridExtra)

my_collection = mongo(collection = "accuracies", db = "myalldata") # create connection, database and collection

DF=my_collection$find()

# En dehors des barres
ggplot(data=DF, aes(x=AlgoName, y=accuracy)) +
  geom_bar(stat="identity", fill="steelblue")+
  
  geom_text(aes(label=round((accuracy*100),2)), vjust=-0.3, size=3.5)+
  theme_minimal()