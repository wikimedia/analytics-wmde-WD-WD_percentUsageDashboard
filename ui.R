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
### --- WD_percentUsageDashboard.R is free software: 
### --- you can redistribute it and/or modify
### --- it under the terms of the GNU General Public License as published by
### --- the Free Software Foundation, either version 2 of the License, or
### --- (at your option) any later version.
### ---
### --- WD_percentUsageDashboard.R is distributed in the 
### --- hope that it will be useful,
### --- but WITHOUT ANY WARRANTY; without even the implied warranty of
### --- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
### --- GNU General Public License for more details.
### ---
### --- You should have received a copy of the GNU General Public License
### --- along with Wiktionary: Cognate Dashboard Update. 
### --- If not, see <http://www.gnu.org/licenses/>.
### ---------------------------------------------------------------------------

### --- Setup
library(shiny)
library(shinycssloaders)

### --- shinyUI
shinyUI(
  
  fluidPage(
  
    fluidRow(
      column(width = 12,
             br(),
             img(src = 'Wikidata-logo-en.png',
                 align = "left"),
             br(), br(), br(), br(), br(), br(), br(),
             HTML('<p style="font-size:80%;"align="left"><b>Percentage of articles making use of data from Wikidata</b><br>
                  The definition of an <i>Article</i> used here is: namespace = 0, no redirects.<br>
                  Wikidata (WD) usage upon which the reported data are based excludes Sitelinks<br>
                  [see: <b>S</b> usage aspect, <a href = "https://www.mediawiki.org/wiki/Wikibase/Schema/wbc_entity_usage" target = "_blank">
                  wbc_entity_usage table</a> in the <a href = "https://www.mediawiki.org/wiki/Wikibase/Schema" target = "_blank">
                  Wikibase schema</a>].</p>'),
             htmlOutput('timestamp'),
             htmlOutput('overall')
      )
    ),
    fluidRow(
      column(width = 6,
        hr(),
        DT::dataTableOutput('overviewDT', width = "100%"),
        hr(),
        HTML('<p style="font-size:80%;"align="left"><b>Contact:</b> Goran S. Milovanovic, Data Scientist, WMDE<br><b>e-mail:</b> goran.milovanovic_ext@wikimedia.de
                          <br><b>IRC:</b> goransm</p>'),
        hr(),
        br(),
        hr()
      ),
      column(width = 6,
             hr(),
             withSpinner(plotOutput('percentProjectType',
                                    width = "100%")),
             HTML('<p style="font-size:80%;"align="left"><b>Note. </b> Percents refer to the count of articles that use WD 
                  relative to the total number of articles in a given Project Type.</p>'),
             hr(),
             withSpinner(plotOutput('wdUsagePropPerProjectType',
                                    width = "100%")), 
             HTML('<p style="font-size:80%;"align="left"><b>Description. </b> The pie chart represents distribution of total WD usage 
                  (in % of total WD usage) across the Project Types.</p>'),
             hr(),
             withSpinner(plotOutput('wdUsagePerProject',
                                    width = "100%")),
             HTML('<p style="font-size:80%;"align="left"><b>Description. </b> The chart represents the top 20 Wikimedia Projects per 
                  WD usage.</p>'),
             hr(),
             withSpinner(plotOutput('wdUsagePropPerProject',
                                    width = "100%")), 
             HTML('<p style="font-size:80%;"align="left"><b>Description. </b> The chart represents the top 20 Wikimedia Projects per 
                  proprotion of WD usage relative to the total number of articles in them.</p>')
             )
    )
    
  )
)









