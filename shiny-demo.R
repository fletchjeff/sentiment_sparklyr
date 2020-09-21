library(shiny)
library(dplyr)
library(ggplot2)

happiest_characters <- read.csv("happiest_chars.csv")
happiest_mean <- mean(happiest_characters$weighted_sum)
#library(r2d3)


app <- shinyApp(
  ui <- fluidPage(


  titlePanel("Happiest Simpsons Character"),


  sidebarLayout(
    sidebarPanel(

      selectInput(
          "var",
          "Character:",
          happiest_characters$raw_char,
          size = 10,
          selectize = FALSE
        )    
    ),

    mainPanel(
      textOutput("selected_var"),
      textOutput("happiness_val"),
      plotOutput("happiness_plot")
    )
  )
),

  server <- function(input, output) {
    output$selected_var <- renderText({ 
      paste(input$var)
    })
    
    output$happiness_val <- renderText({ 
      paste((happiest_characters %>% filter(raw_char == input$var))$weighted_sum)
    })
    
    output$happiness_plot <- renderPlot({
      small_list <- data.frame(
        c(input$var,"Average"),
        c((happiest_characters %>% filter(raw_char == input$var))$weighted_sum,happiest_mean),
        c(ifelse((happiest_characters %>% filter(raw_char == input$var))$weighted_sum>happiest_mean,"green","red"),"grey")
      ) 
      colnames(small_list) <- c("raw_char", "weighted_sum" ,"color")
      ggplot(small_list, aes(x=reorder(raw_char,weighted_sum), y=weighted_sum, fill=color))+
      geom_col(width = 0.7) + 
      scale_fill_identity() +
      coord_flip() +
      xlab("Character") + ylab("Happiness Value") +
      ggtitle("Simpson Happiness Index")
    })    
    
  }
)

runApp(app,port=as.numeric(Sys.getenv("CDSW_READONLY_PORT")), host="127.0.0.1", launch.browser="FALSE")


# library(httr)
# result <- POST(
#   "https://modelservice.ml-c9056e76-593.se-sandb.a465-9q4k.cloudera.site/model", 
#   body = '{"accessKey":"mzn85fzd9u9g9jhgq2evvbxbqody8w1b","request":{"sentence":"Woo hoo!"}} ', 
#   add_headers("Content-Type" = "application/json")
# )
# 
# final_response = fromJSON(rawToChar(result$content))$response$result
# final_response
