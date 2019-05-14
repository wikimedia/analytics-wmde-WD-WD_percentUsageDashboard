### ---------------------------------------------------------------------------
### --- WD_percentUsageDashboard.R
### --- Script: server.R, v. Beta 0.1
### --- WMDE 2018.
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- LICENSE:
### ---------------------------------------------------------------------------
### --- GPL v2
### ---
### --- WD_percentUsageDashboard is free software: 
### --- you can redistribute it and/or modify
### --- it under the terms of the GNU General Public License as published by
### --- the Free Software Foundation, either version 2 of the License, or
### --- (at your option) any later version.
### ---
### --- WD_percentUsageDashboard is distributed in the 
### --- hope that it will be useful,
### --- but WITHOUT ANY WARRANTY; without even the implied warranty of
### --- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
### --- GNU General Public License for more details.
### ---
### --- You should have received a copy of the GNU General Public License
### --- along with WD_percentUsageDashboard. 
### --- If not, see <http://www.gnu.org/licenses/>.
### ---------------------------------------------------------------------------

### --- Setup
library(DT)
library(dplyr)
library(httr)
library(curl)
library(stringr)
library(ggplot2)
library(ggrepel)
library(scales)

### --- Fetch files and update stamp from production:
### --- https://analytics.wikimedia.org/datasets/wdUsagePercentArticle/

### --- functions
get_WDCM_table <- function(url_dir, filename, row_names) {
  read.csv(paste0(url_dir, filename), 
           header = T, 
           stringsAsFactors = F,
           check.names = F)
}

# - get update stamp:
h <- new_handle()
handle_setopt(h,
              copypostfields = "WD_percentUsageDashboard");
handle_setheaders(h,
                  "Cache-Control" = "no-cache"
)
timestamp <- curl_fetch_memory('https://analytics.wikimedia.org/datasets/wdUsagePercentArticle/')
timestamp <- rawToChar(timestamp$content)
timestamp <- str_extract_all(timestamp, "[[:digit:]]+.+[[:digit:]]+")[[1]][3]
timestamp <- gsub("<.+", "", timestamp)
timestamp <- trimws(timestamp, which = "right")
timestamp <- paste0("Updated: ", timestamp, " UTC")

### --- Serve table

### --- shinyServer
shinyServer(function(input, output, session) {
  
  # - get current data file:
  publicDir <- 'https://analytics.wikimedia.org/datasets/wdUsagePercentArticle/'
  filename <- 'wdUsage_ProjectStatistics.csv'
  withProgress(message = 'Downloading data', detail = "Please be patient.", value = 0, {
    dataSet <- get_WDCM_table(publicDir, filename)
    dataSet[, 1] <- NULL
  })
  dataSet <- arrange(dataSet, desc(percentWDuse))
  colnames(dataSet) <- c('Project', 
                         'Number of Articles', 
                         'Number of Articles that use WD', 
                         'Percent of Articles that use WD',
                         'Project Type')
  dataSet <- filter(dataSet, !(Project %in% 'wikidatawiki'))
  
  ### --- timestamp
  output$timestamp <- renderText({
    paste0('<p style="font-size:90%;"align="left"><b>',
           round(sum(dataSet$`Number of Articles that use WD`)/sum(dataSet$`Number of Articles`), 6) * 100,
           "%</b> of pages across the WMF projects make use of Wikidata.</p>"
    )
  })
  
  ### --- output: overall % of pages
  ### --- that use WD across the projects
  output$overall <- renderText({
    paste0('<p style="font-size:80%;"align="left"><b>', 
           timestamp,
           "</b></p>"
    )
  })
  
  ### --- TABLE
  ### --- output$overviewDT
  output$overviewDT <- DT::renderDataTable({
    DT::datatable(dataSet, 
              options = list(
                pageLength = 100,
                width = '100%',
                columnDefs = list(list(className = 'dt-center', targets = "_all"))
              ),
              rownames = FALSE
    )
  })
  
  ### --- Percent WD Usage per ProjectType
  output$percentProjectType <- renderPlot({
    plotFrame <- dataSet %>% 
      dplyr::select(`Project Type`, `Number of Articles`, `Number of Articles that use WD`) %>%
      dplyr::group_by(`Project Type`) %>% 
      dplyr::summarise(TotalUsage = sum(`Number of Articles that use WD`), 
                Usage = sum(`Number of Articles`))
    plotFrame$PercentWDUSage <- round(plotFrame$TotalUsage/plotFrame$Usage*100, 2)
    plotFrame$Label <- paste0(plotFrame$TotalUsage, "(", plotFrame$PercentWDUSage, "%)")
    ggplot(plotFrame, aes(x = `Project Type`, y = log10(TotalUsage),
                          color = `Project Type`,
                          fill = `Project Type`,
                          label = Label)) +
      geom_bar(width = .2, stat = 'identity') +
      geom_label_repel(size = 3.5, 
                      segment.size = .25, 
                      show.legend = FALSE, 
                      fill = "white") +
      ggtitle("Total Wikidata Usage [(S)itelinkes excluded]") + 
      ylab("log10(Total WD Usage)") +
      theme(plot.title = element_text(size = 12, hjust = .5)) + 
      theme(legend.position = "top")
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- Top WD Usage per Project
  output$wdUsagePerProject <- renderPlot({
    plotFrame <- dataSet %>% 
      dplyr::select(Project, `Number of Articles that use WD`) %>% 
      dplyr::arrange(desc(`Number of Articles that use WD`)) %>%
      head(20)
    plotFrame$Project <- factor(plotFrame$Project, 
                                levels = plotFrame$Project[order(-plotFrame$`Number of Articles that use WD`)])
    ggplot(plotFrame, aes(x = Project, y = `Number of Articles that use WD`,
                          label = Project)) +
      geom_path(size = .15, color = "darkblue", group = 1) +
      geom_point(size = 1.5, color = "darkblue") +
      geom_point(size = 1, color = "white") + 
      geom_label_repel(size = 3.5, 
                       segment.size = .25, 
                       show.legend = FALSE, 
                       fill = "white") +
      ggtitle("Number of pages using Wikidata [(S)itelinkes excluded]") + 
      scale_y_continuous(labels = comma) + 
      ylab("Number of pages using WD") +
      theme(plot.title = element_text(size = 12, hjust = .5)) + 
      theme(axis.text.x = element_text(angle = 90, size = 11))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- Top WD Usage Proportion per Project
  output$wdUsagePropPerProject <- renderPlot({
    plotFrame <- dataSet %>% 
      dplyr::select(Project, `Percent of Articles that use WD`) %>% 
      dplyr::arrange(desc(`Percent of Articles that use WD`)) %>%
      head(20)
    plotFrame$Project <- factor(plotFrame$Project, 
                                levels = plotFrame$Project[order(-plotFrame$`Percent of Articles that use WD`)])
    ggplot(plotFrame, aes(x = Project, y = `Percent of Articles that use WD`,
                          label = Project)) +
      geom_path(size = .15, color = "darkblue", group = 1) +
      geom_point(size = 1.5, color = "darkblue") +
      geom_point(size = 1, color = "white") + 
      geom_label_repel(size = 3.5, 
                       segment.size = .25, 
                       show.legend = FALSE, 
                       fill = "white") +
      ggtitle("Percent of pages that use WD [(S)itelinkes excluded]") + 
      ylab("% pages") +
      theme(plot.title = element_text(size = 12, hjust = .5)) + 
      theme(axis.text.x = element_text(angle = 90, size = 11))
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})
  
  ### --- WD Usage Proportion per ProjectType
  output$wdUsagePropPerProjectType <- renderPlot({
    plotFrame <- dataSet %>%
      dplyr::select(`Project Type`, `Number of Articles`, `Number of Articles that use WD`) %>%
      dplyr::group_by(`Project Type`) %>%
      dplyr::summarise(`Number of Articles` = sum(`Number of Articles`), 
                       `Number of Articles that use WD` = sum(`Number of Articles that use WD`))
    plotFrame$Percent <- round(
      plotFrame$`Number of Articles that use WD`/sum(plotFrame$`Number of Articles that use WD`)*100, 3)
    plotFrame$`Project Type` <- factor(plotFrame$`Project Type`,
                                       levels = plotFrame$`Project Type`[order(-plotFrame$Percent)])
    ggplot(plotFrame, aes(x = "", y = Percent,
                          label = paste0(`Project Type`, "(", Percent, "%)"),
                          color = `Project Type`, 
                          fill = `Project Type`)) +
      geom_bar(stat = "identity", width = .2) + 
      coord_polar("y", start = 0) +
      geom_label_repel(size = 3.5, 
                       segment.size = .25, 
                       show.legend = FALSE, 
                       fill = "white") +
      ggtitle("Percent WD usage across Project Types [(S)itelinkes excluded]") + 
      ylab("% of WD Usage") + xlab("") + 
      theme_bw() +
      theme(plot.title = element_text(size = 12, hjust = .5)) + 
      theme(axis.text.x = element_text(size = 11)) + 
      theme(panel.border = element_blank())
  }) %>% withProgress(message = 'Generating plot',
                      min = 0,
                      max = 1,
                      value = 1, {incProgress(amount = 1)})

})







