#install.packages("dplyr")
#install.packages("tibble")
#install.packages("sparklyr")
#install.packages("ggthemes")
#install.packages("shiny")

#packages = c("dplyr", "tibble","sparklyr", "ggthemes","shiny")
#
### Now load or install&load all
#package.check <- lapply(
#  packages,
#  FUN = function(x) {
#    if (!require(x, character.only = TRUE)) {
#      install.packages(x, dependencies = TRUE)
#    }
#  }
#)


list.of.packages <- c("dplyr", "tibble","sparklyr", "ggthemes","shiny")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)