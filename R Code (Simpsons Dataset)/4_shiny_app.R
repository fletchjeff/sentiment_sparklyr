library(shiny)
library(dplyr)
library(httr)

fetch_result <- function (sentence, model) {
  if (model == "simp") {
    accessKey <-  "mfd0yk8o4tfi13uua8hc9gzqxej0jc2s"
  }
  else {
    accessKey <-  "m7zzyhlbtr3ovq3tvaa2myowglhzpf3f"
  }
  result <- POST(
    paste("https://modelservice.", Sys.getenv("CDSW_DOMAIN"), "/model", sep=""),
    body = paste('{"accessKey":"',accessKey,'","request":{"sentence":"',sentence,'"}} ',sep = ""),
    add_headers("Content-Type" = "application/json")
  )
  
  model_response <- fromJSON(rawToChar(result$content))#$response
  return_ouput <- paste("The model is", model_response$response["confidence"],"% confident that is", model_response$response["sentiment"])
  return(return_ouput)
}


app <- shinyApp(ui <- fluidPage(
  titlePanel("Sentiment Analysis Model Application"),
  
  sidebarLayout(
    sidebarPanel(
      textAreaInput( 
        "caption", "Test Sentence", "I have had a bad day"
      ),
      radioButtons(
        "model", "Choose model:", c("Sparklyr" = "simp", "Tensorflow" = "dl")
      ),
      submitButton("Get Sentiment", icon("arrow-right"))
    ),
    
    mainPanel(
      markdown(
        "
        #### Model Result Output
        The _Test Sentence_ will be sent to the selected model and the response will be displayed below
        "
      ),
      
      verbatimTextOutput("value")
    )
  )
),

server <- function(input, output) {
  output$value <- renderText({
    fetch_result(input$caption, input$model)
  })
})

runApp(app, port = as.numeric(Sys.getenv("CDSW_READONLY_PORT")), host = "127.0.0.1", launch.browser = "FALSE")
