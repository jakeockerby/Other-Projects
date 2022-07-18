library(xts)
library(forecast)
hobb <- read.csv('Y:/R/HOBB008 Time Series/Cricket.csv')
hobb <- xts(hobb[, 2], order.by=as.Date(hobb[, 1], "%Y-%m-%d"))
plot.xts(hobb, main = 'Cricket Time Series', col = 'blue',
         grid.ticks.on = 'years',
         minor.ticks = 'years')
acf(hobb, lag.max = 100)
pacf(hobb, lag.max = 100)

set.seed(250)
timeseries=arima.sim(list(order = c(1,1,2), ma=c(0.32,0.47), ar=0.8), n = 50)+20
## partition into train and test
train_series=timeseries[1:40]
test_series=timeseries[41:50]

plot(timeseries)


## make arima models
arimaModel_1=arima(train_series, order=c(0,1,2))
arimaModel_2=arima(train_series, order=c(1,1,0))
arimaModel_3=arima(train_series, order=c(1,1,2))

lines(fitted(arimaModel_1), col="red")

## look at the parameters
print(arimaModel_1);print(arimaModel_2);print(arimaModel_3)

forecast1 <- forecast(arimaModel_1, 10)



