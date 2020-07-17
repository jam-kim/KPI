# install.packages('dlookr')
library(dlookr)

url <- 'https://raw.githubusercontent.com/guru99-edu/R-Programming/master/adult_data.csv'
data <- read.csv(url, na.strings = '?')

data %>%
    diagnose_report(output_format = "html", output_file = "report.html")


matrix(c(1:9), nrow=3)
