#!/usr/bin/env Rscript

##################################################
## #This script parses the output HTML files from the NLP filter pipeline and 
## stores them into 4 separate folders, one for each step
##
## This script assumes that default AWS credentials are set up on the local machine where this is run.
## 
## 
## This script was written to support the following paper
## "Parikh JR, Genetti CA et al.  A data-driven architecture using natural language processing to improve phenotyping efficiency and accelerate genetic diagnoses of rare disorders."
##################################################
## Author: Jiggy Parikh
## Version: 0.1.0
## Email: jiggy@jsquarelabs.com
## Status: Dev



library(dplyr)
library(tidyr)
library(data.table)
library(BBmisc)
library(rvest)
library(yaml)
library(aws.s3)

args = commandArgs(trailingOnly=TRUE)

#read in yaml file
yaml_file <- args[1]
#yaml_file <- "/home/ubuntu/pheno_pipeline_params.yaml"
yaml_data <- read_yaml(yaml_file)

#set working directory
setwd(yaml_data$workdir)

#get patient ids from vcf files
ids = gsub("s3://.+/(MAN_.+)\\.vcf", "\\1", yaml_data$vcf_files)
#ids = c("MAN_0455-01", "MAN_1037-01", "MAN_1380-01")

#get all NLP* folders from exomiser_results - assume exomiser_results have completed runs
runNames <- system(paste0('docker run --rm -it amazon/aws-cli s3 ls s3://', yaml_data$s3_bucket_name, '/exomiser_results/ | grep -oh "NLP.*/" | sed "s/\\s//g" | sed "s/\\n+/\\n/g" | sed "s/\\///g"'), intern=T)
manual_dir <- "Manual"
runNames <- c(runNames, manual_dir)

#compile results
df = data.frame()
all_patient_data <- list()
for (inx in 1:length(ids)) {
  print(paste("Getting data for patient", inx, "of", length(ids)))
  
  for (run_type in runNames) {
    r = s3read_using(read.csv, bucket = yaml_data$s3_bucket_name, object = paste0("exomiser_results/", run_type, "/tab/", ids[inx], ".tab"), header=T, sep="\t")
    patient_data <- r
    patient_data$Gene <- as.character(patient_data$Gene)
    patient_data$run <- run_type
    patient_data$patient <- ids[inx]
    patient_data$rank <- 1:nrow(patient_data)
    patient_data$genes <- dim(r)[1]
    all_patient_data <- c(all_patient_data, list(patient_data))
  }
  
  
}

all_patient_data <- rbindlist(all_patient_data)

#create an ensemble NLP algorithm, which is the mean score per gene per patient across all NLP but rank is based on majority votes (they are the same)
getmode <- function(v) {
  uniqv <- unique(v)
  uniqv[which.max(tabulate(match(v, uniqv)))]
}

ensemble_algo_ <- all_patient_data %>% filter(run != "Manual") %>% group_by(patient, Gene) %>% select(Combined_Score, Genetic_Score, Phenotype_score, genes) %>% 
  summarise_all(mean) %>% ungroup() %>% 
  group_by(patient) %>% arrange(desc(Combined_Score), .by_group=T) %>% mutate(rank=row_number()) %>% ungroup() %>%
  mutate(run="NLP_ensemble")


df <- bind_rows(all_patient_data %>% select("run", "patient", "Gene", "rank", "score"="Combined_Score", "genes"), 
                ensemble_algo_ %>% select("run", "patient", "Gene", "rank", "score"="Combined_Score", "genes")
)



#create parsed_results dirs
parsed_results_dir <- file.path(yaml_data$workdir, "filter_pipeline_results/")
dir.create(parsed_results_dir)
dir.create(file.path(parsed_results_dir, "step1"))
dir.create(file.path(parsed_results_dir, "step2"))
dir.create(file.path(parsed_results_dir, "step3"))
dir.create(file.path(parsed_results_dir, "step4"))


html_filter_func <- function(html_folder, filter_genes_per_patient, filter_number, write_folder="/home/ubuntu/pipeline/filter_pipeline_results/step1/", search_in_full_list=F) {
  #read in html

  for (patient in unique(filter_genes_per_patient$patient)) {
    html_file <- file.path(html_folder, paste0(patient, ".html"))
    filter_genes <- filter_genes_per_patient %>% filter(patient == !!patient) %>% arrange(rank) %>% select(Gene) %>% pull()
    if (filter_number > length(filter_genes)) {
      #download html file to write_folder
      save_object(object=html_file, bucket=yaml_data$s3_bucket_name, file=file.path(write_folder, paste0(patient, ".html")))
      next
    }
    
    
    html_data <- s3read_using(read_html, object=html_file, bucket=yaml_data$s3_bucket_name)
    
    
    divs <- html_nodes(html_data, xpath='//*[@id="content"]/div')
    gene_divs <- divs[is.na(html_attr(divs, "id"))]
    
    if (search_in_full_list) {
      gene_divs_to_keep <- gene_divs  
    } else {
      gene_divs_to_keep <- gene_divs[1:filter_number]
    }
    
    
    #confirm gene divs have genes of interest
    tryCatch({
      min(sapply(1:length(gene_divs_to_keep), FUN=function(i) {html_text(html_nodes(gene_divs_to_keep[i], xpath='./div[1]/div/div[1]/h4/a')) %in% filter_genes})) == 1
    }, error=function(e) {
      print("Top genes are different than expected")
      return(0)
    })
    
    #sort genes to keep in expected rank
    gene_divs_to_keep_sorted <- sapply(1:length(filter_genes), FUN=function(j) {
      x <- filter_genes[j]
      for (i in 1:length(gene_divs_to_keep)) {
        gene_name <- html_text(html_nodes(gene_divs_to_keep[i], xpath='./div[1]/div/div[1]/h4/a'))  
        if (gene_name == x) {
          #return(as.character(gene_divs_to_keep[i]))
          return(gsub('(.+http://grch37.ensembl.org/.+>)(.+?)(</a>.+)', paste0('\\1', j, '\\. \\2\\3'), as.character(gene_divs_to_keep[i])))
        }
      }
    })
    
    #get html content before and after gene divs
    head_node <- html_node(html_data, xpath='/html/head')
    h2 <- html_nodes(html_data, xpath='//*[@id="content"]/h2')
    ul <- html_nodes(html_data, xpath='//*[@id="tabs"]')
    non_gene_divs_before <- sapply(setdiff(divs, gene_divs)[1:(length(setdiff(divs, gene_divs))-1)], as.character)
    non_gene_divs_after <- sapply(setdiff(divs, gene_divs)[length(setdiff(divs, gene_divs))], as.character)
    
    html_content = paste0(    
      '<html xmlns="http://www.w3.org/1999/xhtml">',
      as.character(head_node),
      '<body>',
      '<div id="content" class="container">',
      as.character(h2),
      as.character(ul),
      paste(as.character(non_gene_divs_before), collapse="\n"),
      paste(gene_divs_to_keep_sorted, collapse="\n"),
      non_gene_divs_after,
      '</div>',
      '</body></html>'
    )
    
    writeLines(html_content, file.path(write_folder, paste0(patient, ".html")))
    
    
  }
  
  return(0)  
}

results_dir_path_html <- "exomiser_results"
#filter fp80_c6_d6 for top 5 genes
step1_filter_run <- "NLP_fp80_c6_d6"
step1_filter_html <- file.path(results_dir_path_html, step1_filter_run, "html")
step1_filter_number <- 5
step1_filter_genes <- df %>% filter(run == step1_filter_run & rank <= step1_filter_number & score > 0) %>% select(patient, Gene, rank)
#


#filter fp90_c6_d6 for top 20 genes
step2_filter_run <- "NLP_fp90_c6_d6"
step2_filter_html <- file.path(results_dir_path_html, step2_filter_run, "html")
step2_filter_number <- 20
step2_filter_genes <- df %>% filter(run == step2_filter_run & rank <= step2_filter_number & score > 0) %>% select(patient, Gene, rank)


#filter regular NLP for 50 genes from ensemble ranking
step3_filter_run <- "NLP_ensemble"
step3_filter_html <- file.path(results_dir_path_html, "NLP", "html")
step3_filter_number <- 50
step3_filter_genes <- df %>% filter(run == step3_filter_run & rank <= step3_filter_number & score > 0) %>% select(patient, Gene, rank)

#Return Manual results for all genes
step4_filter_run <- "Manual"
step4_filter_html <- file.path(results_dir_path_html, "Manual", "html")
step4_filter_number <- Inf
step4_filter_genes <- df %>% filter(run == step4_filter_run & rank <= step4_filter_number & score > 0) %>% select(patient, Gene, rank)



rm(df, all_patient_data)
gc()

html_filter_func(step1_filter_html, step1_filter_genes, step1_filter_number, write_folder=file.path(parsed_results_dir, "step1"))
html_filter_func(step2_filter_html, step2_filter_genes, step2_filter_number, write_folder=file.path(parsed_results_dir, "step2"))
html_filter_func(step3_filter_html, step3_filter_genes, step3_filter_number, write_folder=file.path(parsed_results_dir, "step3"), search_in_full_list=T)
html_filter_func(step4_filter_html, step4_filter_genes, step4_filter_number, write_folder=file.path(parsed_results_dir, "step4"))

