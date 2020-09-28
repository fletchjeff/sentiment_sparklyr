library(shiny)
library(dplyr)
library(httr)

fetch_result <- function (sentence, model) {
  if (model == "simp") {
    accessKey <-  "mzn85fzd9u9g9jhgq2evvbxbqody8w1b"
  }
  else {
    accessKey <-  "mj8hzqlabyjxlqbiwefqiuldbqdngmuw"
  }
  result <- POST(
    "https://modelservice.ml-c9056e76-593.se-sandb.a465-9q4k.cloudera.site/model",
    body = paste('{"accessKey":"',accessKey,'","request":{"sentence":"',sentence,'"}} ',sep = ""),
    add_headers("Content-Type" = "application/json")
  )
  
  model_response <- fromJSON(rawToChar(result$content))#$response
  return_ouput <- paste("The model is", model_response$response["confidence"],"% confident that is", model_response$response["sentiment"])
  return(return_ouput)
}


app <- shinyApp(ui <- fluidPage(
  titlePanel("Sentiment Analysis Model Test Application"),
  
  sidebarLayout(
    sidebarPanel(
      textAreaInput( 
        "caption", "Test Sentence", "I'm no dunce, I was born an oaf and I'll die an oaf"
      ),
      radioButtons(
        "model", "Choose model:", c("Simpsons Spark" = "simp", "Deep Learning" = "dl")
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
