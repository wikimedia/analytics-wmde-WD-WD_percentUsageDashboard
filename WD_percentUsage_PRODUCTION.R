#!/usr/bin/env Rscript

### ---------------------------------------------------------------------------
### --- Script: WD_percentUsage_PRODUCTION.R
### --- Author: Goran S. Milovanovic, Data Analyst, WMDE
### --- Developed under the contract between Goran Milovanovic PR Data Kolektiv
### --- and WMDE.
### --- Contact: goran.milovanovic_ext@wikimedia.de
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- Fetch the WD usage data from goransm.wdcm_clients_wb_entity_usage table
### --- (HiveQL, DataLake);
### --- fetch the page tables from SQL to determine the pages in namespace = 0
### --- that are not redirects;
### --- compute the percent of articles that use WD for every WMF project.
### ---------------------------------------------------------------------------
### --- RUN FROM: /home/goransm/wdUsagePerPage on stat1004 (currently)
### --- on crontab as:
### --- 0 0 * * * export USER=goransm && nice -10 Rscript 
### --- /home/goransm/wdUsagePerPage/WD_percentUsage_PRODUCTION.R  >> 
### --- /home/goransm/wdUsagePerPage/WD_percentUsage_PRODUCTION_LOG.log 2>&1
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- LICENSE:
### ---------------------------------------------------------------------------
### --- GPL v2
### --- This file is part of Wikidata Concepts Monitor (WDCM)
### ---
### --- WDCM is free software: you can redistribute it and/or modify
### --- it under the terms of the GNU General Public License as published by
### --- the Free Software Foundation, either version 2 of the License, or
### --- (at your option) any later version.
### ---
### --- WDCM is distributed in the hope that it will be useful,
### --- but WITHOUT ANY WARRANTY; without even the implied warranty of
### --- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
### --- GNU General Public License for more details.
### ---
### --- You should have received a copy of the GNU General Public License
### --- along with WDCM. If not, see <http://www.gnu.org/licenses/>.
### ---------------------------------------------------------------------------

# - toReport
print(paste0("Initiate on: ", Sys.time()))

### --- Setup
library(data.table)
library(dplyr)
filename <- 'wdUsagePerPage.tsv'
dataDir <- '/home/goransm/wdUsagePerPage'

### --- Functions
# - projectType() to determine project type
projectType <- function(projectName) {
  unname(sapply(projectName, function(x) {
    if (grepl("commons", x, fixed = T)) {"Commons"
    } else if (grepl("mediawiki|meta|species|wikidata", x)) {"Other"
    } else if (grepl("wiki$", x)) {"Wikipedia"
    } else if (grepl("quote$", x)) {"Wikiquote"
    } else if (grepl("voyage$", x)) {"Wikivoyage"
    } else if (grepl("news$", x)) {"Wikinews"
    } else if (grepl("source$", x)) {"Wikisource"
    } else if (grepl("wiktionary$", x)) {"Wiktionary"
    } else if (grepl("versity$", x)) {"Wikiversity"
    } else if (grepl("books$", x)) {"Wikibooks"
    } else {"Other"}
  }))
}

# - toReport
print("Fetch usage data from goransm.wdcm_clients_wb_entity_usage: HiveQL.")

### --- Deliver counts from wdcm_clients_wb_entity_usage
hiveQLquery <- "use goransm;
          set hive.mapred.mode=nonstrict;
          select distinct eu_page_id, wiki_db from wdcm_clients_wb_entity_usage 
          where eu_aspect != 'S' ;
"
# - run query
Rquery <- system(command = paste('/usr/local/bin/beeline --incremental=true --silent -e "',
                                 hiveQLquery,
                                 '" > ', dataDir,
                                 "/", filename,
                                 sep = ""),
                 wait = TRUE)

# - load resulting table
setwd(dataDir)
wdUsage <- fread(filename, sep = "\t", quote = "")
setkey(wdUsage, wiki_db)

### --- Iterate across page tables per project, fetch namespace 0 pages
projectsTracking <- sort(unique(wdUsage$wiki_db))
file.remove('currentProject.tsv')

# - toReport
print("SQL iterate across clients, fetch data, and produce the dataset.")
projectStats <- list()
tStart <- Sys.time()
c <- 0
for (i in 1:length(projectsTracking)) {
  pages <- tryCatch(
    {
      mySqlArgs <- 
        '--defaults-file=/etc/mysql/conf.d/analytics-research-client.cnf -h analytics-store.eqiad.wmnet -A'
      mySqlInput <- paste0('"USE ', 
                           projectsTracking[i], 
                           '; SELECT page_id FROM page WHERE (page_namespace = 0 AND page_is_redirect != 1);" > /home/goransm/wdUsagePerPage/currentProject.tsv')
      # - command:
      mySqlCommand <- paste0("mysql ", mySqlArgs, " -e ", mySqlInput, collapse = "")
      system(command = mySqlCommand, wait = TRUE)
      fread('currentProject.tsv', sep = "\t", quote = "")
    },
    error = function(condtition) {
      return(NULL)
    },
    warning = function(condtition) {
      return(NULL)
    }
  )
  file.remove('currentProject.tsv')
  if (!is.null(pages)) {
    numPages <- length(unique(pages$page_id))
    localProject <- wdUsage %>%
      filter(wiki_db %in% projectsTracking[i])
    if (dim(localProject)[1] > 0) {
      c <- c + 1
      wdUsePages <- length(which(unique(pages$page_id) %in% localProject$eu_page_id))
      projectStats[[c]] <- data.frame(project = projectsTracking[i], 
                                      numPages = numPages,
                                      wdUsePages = wdUsePages,
                                      percentWDuse = round(wdUsePages/numPages*100, 2),
                                      stringsAsFactors = F)
      # - toReport
      print(paste0("Project ", projectStats[[c]]$project, 
                   " has ", projectStats[[c]]$numPages, " pages, of which ", 
                   projectStats[[c]]$wdUsePages, " make use of WD."))
    }
  }
}
# - bind
projectStats <- rbindlist(projectStats)
# - toReport
print(paste0("Fetch and Compare took: ", tEnd <- Sys.time() - tStart, " total time."))
# - add projectType
projectStats$projectType <- projectType(projectStats$project)
# - store
write.csv(projectStats, "wdUsage_ProjectStatistics.csv")
# - toReport
print("Copy to production.")
# - migrate to /srv/published-datasets
system(command = 'cp /home/goransm/wdUsagePerPage/wdUsage_ProjectStatistics.csv /srv/published-datasets/wdUsagePercentArticle', 
       wait = T)
# - toReport
print(paste0("DONE: ", Sys.time()))

