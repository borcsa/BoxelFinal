library(pROC)
library(caret)




#DOING ROC WITH ALL PREDICTIONS TOGETHER RBIND- SUPPORT VECTOR MACHINE


tablePathCross1 = "C:\\Users\\Raskolnikov\\Desktop\\BigdatacourseFU\\Assignment_11\\MLR\\PredictionsCross1.csv"
tableCross1<- read.csv(tablePathCross1,sep="\t",col.names=c("A","B"))
tablePathCross2 = "C:\\Users\\Raskolnikov\\Desktop\\BigdatacourseFU\\Assignment_11\\MLR\\PredictionsCross3.csv"
tableCross2<- read.csv(tablePathCross2,sep="\t",col.names=c("A","B"))
tablePathCross3 = "C:\\Users\\Raskolnikov\\Desktop\\BigdatacourseFU\\Assignment_11\\MLR\\PredictionsCross5.csv"
tableCross3<- read.csv(tablePathCross3,sep="\t",col.names=c("A","B"))
tablePathCross4 = "C:\\Users\\Raskolnikov\\Desktop\\BigdatacourseFU\\Assignment_11\\MLR\\PredictionsCross7.csv"
tableCross4<- read.csv(tablePathCross4,sep="\t",col.names=c("A","B"))
tablePathCross5 = "C:\\Users\\Raskolnikov\\Desktop\\BigdatacourseFU\\Assignment_11\\MLR\\PredictionsCross9.csv"
tableCross5<- read.csv(tablePathCross5,sep="\t",col.names=c("A","B"))
tableTogetherCross<-rbind(tableCross1,tableCross2,tableCross3,tableCross4,tableCross5)

myRocCross<-roc(tableTogetherCross[,1],tableTogetherCross[,2],plot=TRUE,smooth=FALSE)

#For C=10 (regularization parameter) we have in different runs: 0.6963, 0.7094, 0.6595, 0.722,0.6794
#For C=1 we have in different runs:  0.7141, 0.7373, 0.7093, 0.6888, 0.682, 0.7083, 0.6905, 0.7043, 0.6923, 0.7146, 0.6924, 0.7138  

length(vector)
vector<-c(0.7141, 0.7373, 0.7093, 0.6888, 0.682, 0.7083, 0.6905, 0.7043, 0.6923, 0.7146, 0.6924, 0.7138)
a <- mean(vector)
s <- sd(vector)
n <- length(vector)
error <- qnorm(0.975)*s/sqrt(n)
left <- a-error
right <- a+error
print(left)
print(right)
print(a)
sd(vector)
mean(vector)




#DOING ROC WITH ALL PREDICTIONS TOGETHER RBIND- LINEAR REGRESSION

tablePathCross1 = "C:\\Users\\Raskolnikov\\Desktop\\BigdatacourseFU\\Assignment_11\\MLR\\PredictionsCross1.csv"
tableCross1<- read.csv(tablePathCross1,sep="\t",col.names=c("A","B"))
tablePathCross2 = "C:\\Users\\Raskolnikov\\Desktop\\BigdatacourseFU\\Assignment_11\\MLR\\PredictionsCross3.csv"
tableCross2<- read.csv(tablePathCross2,sep="\t",col.names=c("A","B"))
tablePathCross3 = "C:\\Users\\Raskolnikov\\Desktop\\BigdatacourseFU\\Assignment_11\\MLR\\PredictionsCross5.csv"
tableCross3<- read.csv(tablePathCross3,sep="\t",col.names=c("A","B"))
tablePathCross4 = "C:\\Users\\Raskolnikov\\Desktop\\BigdatacourseFU\\Assignment_11\\MLR\\PredictionsCross7.csv"
tableCross4<- read.csv(tablePathCross4,sep="\t",col.names=c("A","B"))
tablePathCross5 = "C:\\Users\\Raskolnikov\\Desktop\\BigdatacourseFU\\Assignment_11\\MLR\\PredictionsCross9.csv"
tableCross5<- read.csv(tablePathCross5,sep="\t",col.names=c("A","B"))
tableTogetherCross<-rbind(tableCross1,tableCross2,tableCross3,tableCross4,tableCross5)

myRocCross<-roc(tableTogetherCross[,1],tableTogetherCross[,2],plot=TRUE,smooth=FALSE)

launo<-tableCross5
lados<-tableCross5
latres<-tableCross5
sum(launo[,1])
sum(lados[,1])
sum(latres[,1])
#We have in different runs: 0.7325, 0.7434, 0.7563, 0.754, 0.7229, 0.7703, 0.7528, 0.7679
# stratified cross validation
#unbalanced: 
b<-tableTogetherCross
sum(b[,1])/length(b[,1])
sum(-b[,1]+1)/length(b[,1])
#variable selection (specially linear regression)
vector<-c(0.7325, 0.7434, 0.7563, 0.754, 0.7229, 0.7703, 0.7528, 0.7679)
a <- mean(vector)
s <- sd(vector)
n <- length(vector)
error <- qnorm(0.975)*s/sqrt(n)
left <- a-error
right <- a+error
print(left)
print(right)
print(a)

