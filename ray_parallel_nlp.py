#!/usr/bin/python

##################################################
## This is a helper script to parallelize the containerized exomiser runs needed for the gene prioritization pipeline using Ray
## It takes as input the set of VCF and HPO files to be processed, assumes that the HPO files have a user-defined prefix, and
## that docker is available. A set of initial instructions for after the first time the docker image is pulled can be found commented out below
## 
## This script was written to support the following paper
## "Parikh JR, Genetti CA et al.  A data-driven architecture using natural language processing to improve phenotyping efficiency and accelerate genetic diagnoses of rare disorders."
##################################################
## Author: Jiggy Parikh
## Version: 0.1.0
## Email: jiggy@jsquarelabs.com
## Status: Dev
##################################################

import ray
import time
import os
import yaml
import sys



#get directories/filenames from yaml (first command line argument or override the line below)
yaml_filename = sys.argv[1]
#yaml_filename = "/home/ubuntu/pheno_pipeline_params.yaml"

with open(yaml_filename, "r") as fh:
	yaml_data = yaml.safe_load(fh)

nlp_hpo_filename_prefix = yaml_data["nlp_hpo_filename_prefix"] #this prefix is appended to each filtered filename
manual_hpo_filename = yaml_data["manual_hpo_filename"] #this prefix is appended to each filtered filename

s3_bucket_name = yaml_data["s3_bucket_name"] #s3 bucket where VCF and HPO files (not nested in "directories") can be found and where output are to be written

#get list of vcf files
vcf_files = yaml_data['vcf_files']

#RunName: ["Manual | NLP", minPercentFrequency, minDepth, maxDepth, maxClades]
run_map = {
	"NLP": ["NLP", 0, 0, 100, 100],
	
	"NLP_fp40": ["NLP", 40, 0, 100, 100],
	"NLP_fp50": ["NLP", 50, 0, 100, 100],		
	"NLP_fp60": ["NLP", 60, 0, 100, 100],
	"NLP_fp70": ["NLP", 70, 0, 100, 100],	
	"NLP_fp80": ["NLP", 80, 0, 100, 100],	
	"NLP_fp90": ["NLP", 90, 0, 100, 100],		
		
	"NLP_c2": ["NLP", 0, 0, 100, 2],		
	"NLP_c4": ["NLP", 0, 0, 100, 4],	
	"NLP_c6": ["NLP", 0, 0, 100, 6],		
	"NLP_c8": ["NLP", 0, 0, 100, 8],
	"NLP_c10": ["NLP", 0, 0, 100, 10],				
	"NLP_c12": ["NLP", 0, 0, 100, 12],	

	"NLP_d4": ["NLP", 0, 4, 100, 100],					
	"NLP_d5": ["NLP", 0, 5, 100, 100],	
	"NLP_d6": ["NLP", 0, 6, 100, 100],		
	"NLP_d7": ["NLP", 0, 7, 100, 100],	
	"NLP_d8": ["NLP", 0, 8, 100, 100],
			
	
	"NLP_fp40_c2": ["NLP", 40, 0, 100, 2],	
	"NLP_fp40_c4": ["NLP", 40, 0, 100, 4],	
	"NLP_fp40_c6": ["NLP", 40, 0, 100, 6],	
	"NLP_fp40_c8": ["NLP", 40, 0, 100, 8],	
	"NLP_fp40_c10": ["NLP", 40, 0, 100, 10],	
	"NLP_fp40_c12": ["NLP", 40, 0, 100, 12],	

	"NLP_fp50_c2": ["NLP", 50, 0, 100, 2],	
	"NLP_fp50_c4": ["NLP", 50, 0, 100, 4],	
	"NLP_fp50_c6": ["NLP", 50, 0, 100, 6],	
	"NLP_fp50_c8": ["NLP", 50, 0, 100, 8],	
	"NLP_fp50_c10": ["NLP", 50, 0, 100, 10],	
	"NLP_fp50_c12": ["NLP", 50, 0, 100, 12],	

	"NLP_fp60_c2": ["NLP", 60, 0, 100, 2],	
	"NLP_fp60_c4": ["NLP", 60, 0, 100, 4],	
	"NLP_fp60_c6": ["NLP", 60, 0, 100, 6],	
	"NLP_fp60_c8": ["NLP", 60, 0, 100, 8],	
	"NLP_fp60_c10": ["NLP", 60, 0, 100, 10],	
	"NLP_fp60_c12": ["NLP", 60, 0, 100, 12],
	
	"NLP_fp70_c2": ["NLP", 70, 0, 100, 2],	
	"NLP_fp70_c4": ["NLP", 70, 0, 100, 4],	
	"NLP_fp70_c6": ["NLP", 70, 0, 100, 6],	
	"NLP_fp70_c8": ["NLP", 70, 0, 100, 8],	
	"NLP_fp70_c10": ["NLP", 70, 0, 100, 10],	
	"NLP_fp70_c12": ["NLP", 70, 0, 100, 12],
	
	"NLP_fp80_c2": ["NLP", 80, 0, 100, 2],	
	"NLP_fp80_c4": ["NLP", 80, 0, 100, 4],	
	"NLP_fp80_c6": ["NLP", 80, 0, 100, 6],	
	"NLP_fp80_c8": ["NLP", 80, 0, 100, 8],	
	"NLP_fp80_c10": ["NLP", 80, 0, 100, 10],	
	"NLP_fp80_c12": ["NLP", 80, 0, 100, 12],	
	
	"NLP_fp90_c2": ["NLP", 90, 0, 100, 2],	
	"NLP_fp90_c4": ["NLP", 90, 0, 100, 4],	
	"NLP_fp90_c6": ["NLP", 90, 0, 100, 6],	
	"NLP_fp90_c8": ["NLP", 90, 0, 100, 8],	
	"NLP_fp90_c10": ["NLP", 90, 0, 100, 10],	
	"NLP_fp90_c12": ["NLP", 90, 0, 100, 12],	
	
	
	"NLP_fp40_d4": ["NLP", 40, 4, 100, 100],					
	"NLP_fp40_d5": ["NLP", 40, 5, 100, 100],	
	"NLP_fp40_d6": ["NLP", 40, 6, 100, 100],		
	"NLP_fp40_d7": ["NLP", 40, 7, 100, 100],	
	"NLP_fp40_d8": ["NLP", 40, 8, 100, 100],
	
	"NLP_fp50_d4": ["NLP", 50, 4, 100, 100],					
	"NLP_fp50_d5": ["NLP", 50, 5, 100, 100],	
	"NLP_fp50_d6": ["NLP", 50, 6, 100, 100],		
	"NLP_fp50_d7": ["NLP", 50, 7, 100, 100],	
	"NLP_fp50_d8": ["NLP", 50, 8, 100, 100],

	"NLP_fp60_d4": ["NLP", 60, 4, 100, 100],					
	"NLP_fp60_d5": ["NLP", 60, 5, 100, 100],	
	"NLP_fp60_d6": ["NLP", 60, 6, 100, 100],		
	"NLP_fp60_d7": ["NLP", 60, 7, 100, 100],	
	"NLP_fp60_d8": ["NLP", 60, 8, 100, 100],
	
	"NLP_fp70_d4": ["NLP", 70, 4, 100, 100],					
	"NLP_fp70_d5": ["NLP", 70, 5, 100, 100],	
	"NLP_fp70_d6": ["NLP", 70, 6, 100, 100],		
	"NLP_fp70_d7": ["NLP", 70, 7, 100, 100],	
	"NLP_fp70_d8": ["NLP", 70, 8, 100, 100],
	
	"NLP_fp80_d4": ["NLP", 80, 4, 100, 100],					
	"NLP_fp80_d5": ["NLP", 80, 5, 100, 100],	
	"NLP_fp80_d6": ["NLP", 80, 6, 100, 100],		
	"NLP_fp80_d7": ["NLP", 80, 7, 100, 100],	
	"NLP_fp80_d8": ["NLP", 80, 8, 100, 100],
	
	"NLP_fp90_d4": ["NLP", 90, 4, 100, 100],					
	"NLP_fp90_d5": ["NLP", 90, 5, 100, 100],	
	"NLP_fp90_d6": ["NLP", 90, 6, 100, 100],		
	"NLP_fp90_d7": ["NLP", 90, 7, 100, 100],	
	"NLP_fp90_d8": ["NLP", 90, 8, 100, 100],

	
	"NLP_c2_d4": ["NLP", 0, 4, 100, 2],	
	"NLP_c2_d5": ["NLP", 0, 5, 100, 2],	
	"NLP_c2_d6": ["NLP", 0, 6, 100, 2],	
	"NLP_c2_d7": ["NLP", 0, 7, 100, 2],	
	"NLP_c2_d8": ["NLP", 0, 8, 100, 2],	
	
	"NLP_c4_d4": ["NLP", 0, 4, 100, 4],	
	"NLP_c4_d5": ["NLP", 0, 5, 100, 4],	
	"NLP_c4_d6": ["NLP", 0, 6, 100, 4],	
	"NLP_c4_d7": ["NLP", 0, 7, 100, 4],	
	"NLP_c4_d8": ["NLP", 0, 8, 100, 4],					

	"NLP_c6_d4": ["NLP", 0, 4, 100, 6],	
	"NLP_c6_d5": ["NLP", 0, 5, 100, 6],	
	"NLP_c6_d6": ["NLP", 0, 6, 100, 6],	
	"NLP_c6_d7": ["NLP", 0, 7, 100, 6],	
	"NLP_c6_d8": ["NLP", 0, 8, 100, 6],	
	
	"NLP_c8_d4": ["NLP", 0, 4, 100, 8],	
	"NLP_c8_d5": ["NLP", 0, 5, 100, 8],	
	"NLP_c8_d6": ["NLP", 0, 6, 100, 8],	
	"NLP_c8_d7": ["NLP", 0, 7, 100, 8],	
	"NLP_c8_d8": ["NLP", 0, 8, 100, 8],		
	
	"NLP_c10_d4": ["NLP", 0, 4, 100, 10],	
	"NLP_c10_d5": ["NLP", 0, 5, 100, 10],	
	"NLP_c10_d6": ["NLP", 0, 6, 100, 10],	
	"NLP_c10_d7": ["NLP", 0, 7, 100, 10],	
	"NLP_c10_d8": ["NLP", 0, 8, 100, 10],					

	"NLP_c12_d4": ["NLP", 0, 4, 100, 12],	
	"NLP_c12_d5": ["NLP", 0, 5, 100, 12],	
	"NLP_c12_d6": ["NLP", 0, 6, 100, 12],	
	"NLP_c12_d7": ["NLP", 0, 7, 100, 12],	
	"NLP_c12_d8": ["NLP", 0, 8, 100, 12],
	
	
	
	"NLP_fp40_c2_d4": ["NLP", 40, 4, 100, 2],	
	"NLP_fp40_c2_d5": ["NLP", 40, 5, 100, 2],	
	"NLP_fp40_c2_d6": ["NLP", 40, 6, 100, 2],	
	"NLP_fp40_c2_d7": ["NLP", 40, 7, 100, 2],	
	"NLP_fp40_c2_d8": ["NLP", 40, 8, 100, 2],	
	"NLP_fp40_c4_d4": ["NLP", 40, 4, 100, 4],	
	"NLP_fp40_c4_d5": ["NLP", 40, 5, 100, 4],	
	"NLP_fp40_c4_d6": ["NLP", 40, 6, 100, 4],	
	"NLP_fp40_c4_d7": ["NLP", 40, 7, 100, 4],	
	"NLP_fp40_c4_d8": ["NLP", 40, 8, 100, 4],					
	"NLP_fp40_c6_d4": ["NLP", 40, 4, 100, 6],	
	"NLP_fp40_c6_d5": ["NLP", 40, 5, 100, 6],	
	"NLP_fp40_c6_d6": ["NLP", 40, 6, 100, 6],	
	"NLP_fp40_c6_d7": ["NLP", 40, 7, 100, 6],	
	"NLP_fp40_c6_d8": ["NLP", 40, 8, 100, 6],	
	"NLP_fp40_c8_d4": ["NLP", 40, 4, 100, 8],	
	"NLP_fp40_c8_d5": ["NLP", 40, 5, 100, 8],	
	"NLP_fp40_c8_d6": ["NLP", 40, 6, 100, 8],	
	"NLP_fp40_c8_d7": ["NLP", 40, 7, 100, 8],	
	"NLP_fp40_c8_d8": ["NLP", 40, 8, 100, 8],		
	"NLP_fp40_c10_d4": ["NLP", 40, 4, 100, 10],	
	"NLP_fp40_c10_d5": ["NLP", 40, 5, 100, 10],	
	"NLP_fp40_c10_d6": ["NLP", 40, 6, 100, 10],	
	"NLP_fp40_c10_d7": ["NLP", 40, 7, 100, 10],	
	"NLP_fp40_c10_d8": ["NLP", 40, 8, 100, 10],					
	"NLP_fp40_c12_d4": ["NLP", 40, 4, 100, 12],	
	"NLP_fp40_c12_d5": ["NLP", 40, 5, 100, 12],	
	"NLP_fp40_c12_d6": ["NLP", 40, 6, 100, 12],	
	"NLP_fp40_c12_d7": ["NLP", 40, 7, 100, 12],	
	"NLP_fp40_c12_d8": ["NLP", 40, 8, 100, 12],					
				
	"NLP_fp50_c2_d4": ["NLP", 50, 4, 100, 2],	
	"NLP_fp50_c2_d5": ["NLP", 50, 5, 100, 2],	
	"NLP_fp50_c2_d6": ["NLP", 50, 6, 100, 2],	
	"NLP_fp50_c2_d7": ["NLP", 50, 7, 100, 2],	
	"NLP_fp50_c2_d8": ["NLP", 50, 8, 100, 2],	
	"NLP_fp50_c4_d4": ["NLP", 50, 4, 100, 4],	
	"NLP_fp50_c4_d5": ["NLP", 50, 5, 100, 4],	
	"NLP_fp50_c4_d6": ["NLP", 50, 6, 100, 4],	
	"NLP_fp50_c4_d7": ["NLP", 50, 7, 100, 4],	
	"NLP_fp50_c4_d8": ["NLP", 50, 8, 100, 4],					
	"NLP_fp50_c6_d4": ["NLP", 50, 4, 100, 6],	
	"NLP_fp50_c6_d5": ["NLP", 50, 5, 100, 6],	
	"NLP_fp50_c6_d6": ["NLP", 50, 6, 100, 6],	
	"NLP_fp50_c6_d7": ["NLP", 50, 7, 100, 6],	
	"NLP_fp50_c6_d8": ["NLP", 50, 8, 100, 6],	
	"NLP_fp50_c8_d4": ["NLP", 50, 4, 100, 8],	
	"NLP_fp50_c8_d5": ["NLP", 50, 5, 100, 8],	
	"NLP_fp50_c8_d6": ["NLP", 50, 6, 100, 8],	
	"NLP_fp50_c8_d7": ["NLP", 50, 7, 100, 8],	
	"NLP_fp50_c8_d8": ["NLP", 50, 8, 100, 8],		
	"NLP_fp50_c10_d4": ["NLP", 50, 4, 100, 10],	
	"NLP_fp50_c10_d5": ["NLP", 50, 5, 100, 10],	
	"NLP_fp50_c10_d6": ["NLP", 50, 6, 100, 10],	
	"NLP_fp50_c10_d7": ["NLP", 50, 7, 100, 10],	
	"NLP_fp50_c10_d8": ["NLP", 50, 8, 100, 10],					
	"NLP_fp50_c12_d4": ["NLP", 50, 4, 100, 12],	
	"NLP_fp50_c12_d5": ["NLP", 50, 5, 100, 12],	
	"NLP_fp50_c12_d6": ["NLP", 50, 6, 100, 12],	
	"NLP_fp50_c12_d7": ["NLP", 50, 7, 100, 12],	
	"NLP_fp50_c12_d8": ["NLP", 50, 8, 100, 12],						
								
	"NLP_fp60_c2_d4": ["NLP", 60, 4, 100, 2],	
	"NLP_fp60_c2_d5": ["NLP", 60, 5, 100, 2],	
	"NLP_fp60_c2_d6": ["NLP", 60, 6, 100, 2],	
	"NLP_fp60_c2_d7": ["NLP", 60, 7, 100, 2],	
	"NLP_fp60_c2_d8": ["NLP", 60, 8, 100, 2],	
	"NLP_fp60_c4_d4": ["NLP", 60, 4, 100, 4],	
	"NLP_fp60_c4_d5": ["NLP", 60, 5, 100, 4],	
	"NLP_fp60_c4_d6": ["NLP", 60, 6, 100, 4],	
	"NLP_fp60_c4_d7": ["NLP", 60, 7, 100, 4],	
	"NLP_fp60_c4_d8": ["NLP", 60, 8, 100, 4],					
	"NLP_fp60_c6_d4": ["NLP", 60, 4, 100, 6],	
	"NLP_fp60_c6_d5": ["NLP", 60, 5, 100, 6],	
	"NLP_fp60_c6_d6": ["NLP", 60, 6, 100, 6],	
	"NLP_fp60_c6_d7": ["NLP", 60, 7, 100, 6],	
	"NLP_fp60_c6_d8": ["NLP", 60, 8, 100, 6],	
	"NLP_fp60_c8_d4": ["NLP", 60, 4, 100, 8],	
	"NLP_fp60_c8_d5": ["NLP", 60, 5, 100, 8],	
	"NLP_fp60_c8_d6": ["NLP", 60, 6, 100, 8],	
	"NLP_fp60_c8_d7": ["NLP", 60, 7, 100, 8],	
	"NLP_fp60_c8_d8": ["NLP", 60, 8, 100, 8],		
	"NLP_fp60_c10_d4": ["NLP", 60, 4, 100, 10],	
	"NLP_fp60_c10_d5": ["NLP", 60, 5, 100, 10],	
	"NLP_fp60_c10_d6": ["NLP", 60, 6, 100, 10],	
	"NLP_fp60_c10_d7": ["NLP", 60, 7, 100, 10],	
	"NLP_fp60_c10_d8": ["NLP", 60, 8, 100, 10],					
	"NLP_fp60_c12_d4": ["NLP", 60, 4, 100, 12],	
	"NLP_fp60_c12_d5": ["NLP", 60, 5, 100, 12],	
	"NLP_fp60_c12_d6": ["NLP", 60, 6, 100, 12],	
	"NLP_fp60_c12_d7": ["NLP", 60, 7, 100, 12],	
	"NLP_fp60_c12_d8": ["NLP", 60, 8, 100, 12],
	
	"NLP_fp70_c2_d4": ["NLP", 70, 4, 100, 2],	
	"NLP_fp70_c2_d5": ["NLP", 70, 5, 100, 2],	
	"NLP_fp70_c2_d6": ["NLP", 70, 6, 100, 2],	
	"NLP_fp70_c2_d7": ["NLP", 70, 7, 100, 2],	
	"NLP_fp70_c2_d8": ["NLP", 70, 8, 100, 2],	
	"NLP_fp70_c4_d4": ["NLP", 70, 4, 100, 4],	
	"NLP_fp70_c4_d5": ["NLP", 70, 5, 100, 4],	
	"NLP_fp70_c4_d6": ["NLP", 70, 6, 100, 4],	
	"NLP_fp70_c4_d7": ["NLP", 70, 7, 100, 4],	
	"NLP_fp70_c4_d8": ["NLP", 70, 8, 100, 4],					
	"NLP_fp70_c6_d4": ["NLP", 70, 4, 100, 6],	
	"NLP_fp70_c6_d5": ["NLP", 70, 5, 100, 6],	
	"NLP_fp70_c6_d6": ["NLP", 70, 6, 100, 6],	
	"NLP_fp70_c6_d7": ["NLP", 70, 7, 100, 6],	
	"NLP_fp70_c6_d8": ["NLP", 70, 8, 100, 6],	
	"NLP_fp70_c8_d4": ["NLP", 70, 4, 100, 8],	
	"NLP_fp70_c8_d5": ["NLP", 70, 5, 100, 8],	
	"NLP_fp70_c8_d6": ["NLP", 70, 6, 100, 8],	
	"NLP_fp70_c8_d7": ["NLP", 70, 7, 100, 8],	
	"NLP_fp70_c8_d8": ["NLP", 70, 8, 100, 8],		
	"NLP_fp70_c10_d4": ["NLP", 70, 4, 100, 10],	
	"NLP_fp70_c10_d5": ["NLP", 70, 5, 100, 10],	
	"NLP_fp70_c10_d6": ["NLP", 70, 6, 100, 10],	
	"NLP_fp70_c10_d7": ["NLP", 70, 7, 100, 10],	
	"NLP_fp70_c10_d8": ["NLP", 70, 8, 100, 10],					
	"NLP_fp70_c12_d4": ["NLP", 70, 4, 100, 12],	
	"NLP_fp70_c12_d5": ["NLP", 70, 5, 100, 12],	
	"NLP_fp70_c12_d6": ["NLP", 70, 6, 100, 12],	
	"NLP_fp70_c12_d7": ["NLP", 70, 7, 100, 12],	
	"NLP_fp70_c12_d8": ["NLP", 70, 8, 100, 12],	
	
	"NLP_fp80_c2_d4": ["NLP", 80, 4, 100, 2],	
	"NLP_fp80_c2_d5": ["NLP", 80, 5, 100, 2],	
	"NLP_fp80_c2_d6": ["NLP", 80, 6, 100, 2],	
	"NLP_fp80_c2_d7": ["NLP", 80, 7, 100, 2],	
	"NLP_fp80_c2_d8": ["NLP", 80, 8, 100, 2],	
	"NLP_fp80_c4_d4": ["NLP", 80, 4, 100, 4],	
	"NLP_fp80_c4_d5": ["NLP", 80, 5, 100, 4],	
	"NLP_fp80_c4_d6": ["NLP", 80, 6, 100, 4],	
	"NLP_fp80_c4_d7": ["NLP", 80, 7, 100, 4],	
	"NLP_fp80_c4_d8": ["NLP", 80, 8, 100, 4],					
	"NLP_fp80_c6_d4": ["NLP", 80, 4, 100, 6],	
	"NLP_fp80_c6_d5": ["NLP", 80, 5, 100, 6],	
	"NLP_fp80_c6_d6": ["NLP", 80, 6, 100, 6],	
	"NLP_fp80_c6_d7": ["NLP", 80, 7, 100, 6],	
	"NLP_fp80_c6_d8": ["NLP", 80, 8, 100, 6],	
	"NLP_fp80_c8_d4": ["NLP", 80, 4, 100, 8],	
	"NLP_fp80_c8_d5": ["NLP", 80, 5, 100, 8],	
	"NLP_fp80_c8_d6": ["NLP", 80, 6, 100, 8],	
	"NLP_fp80_c8_d7": ["NLP", 80, 7, 100, 8],	
	"NLP_fp80_c8_d8": ["NLP", 80, 8, 100, 8],		
	"NLP_fp80_c10_d4": ["NLP", 80, 4, 100, 10],	
	"NLP_fp80_c10_d5": ["NLP", 80, 5, 100, 10],	
	"NLP_fp80_c10_d6": ["NLP", 80, 6, 100, 10],	
	"NLP_fp80_c10_d7": ["NLP", 80, 7, 100, 10],	
	"NLP_fp80_c10_d8": ["NLP", 80, 8, 100, 10],					
	"NLP_fp80_c12_d4": ["NLP", 80, 4, 100, 12],	
	"NLP_fp80_c12_d5": ["NLP", 80, 5, 100, 12],	
	"NLP_fp80_c12_d6": ["NLP", 80, 6, 100, 12],	
	"NLP_fp80_c12_d7": ["NLP", 80, 7, 100, 12],	
	"NLP_fp80_c12_d8": ["NLP", 80, 8, 100, 12],
	
	"NLP_fp90_c2_d4": ["NLP", 90, 4, 100, 2],	
	"NLP_fp90_c2_d5": ["NLP", 90, 5, 100, 2],	
	"NLP_fp90_c2_d6": ["NLP", 90, 6, 100, 2],	
	"NLP_fp90_c2_d7": ["NLP", 90, 7, 100, 2],	
	"NLP_fp90_c2_d8": ["NLP", 90, 8, 100, 2],	
	"NLP_fp90_c4_d4": ["NLP", 90, 4, 100, 4],	
	"NLP_fp90_c4_d5": ["NLP", 90, 5, 100, 4],	
	"NLP_fp90_c4_d6": ["NLP", 90, 6, 100, 4],	
	"NLP_fp90_c4_d7": ["NLP", 90, 7, 100, 4],	
	"NLP_fp90_c4_d8": ["NLP", 90, 8, 100, 4],					
	"NLP_fp90_c6_d4": ["NLP", 90, 4, 100, 6],	
	"NLP_fp90_c6_d5": ["NLP", 90, 5, 100, 6],	
	"NLP_fp90_c6_d6": ["NLP", 90, 6, 100, 6],	
	"NLP_fp90_c6_d7": ["NLP", 90, 7, 100, 6],	
	"NLP_fp90_c6_d8": ["NLP", 90, 8, 100, 6],	
	"NLP_fp90_c8_d4": ["NLP", 90, 4, 100, 8],	
	"NLP_fp90_c8_d5": ["NLP", 90, 5, 100, 8],	
	"NLP_fp90_c8_d6": ["NLP", 90, 6, 100, 8],	
	"NLP_fp90_c8_d7": ["NLP", 90, 7, 100, 8],	
	"NLP_fp90_c8_d8": ["NLP", 90, 8, 100, 8],		
	"NLP_fp90_c10_d4": ["NLP", 90, 4, 100, 10],	
	"NLP_fp90_c10_d5": ["NLP", 90, 5, 100, 10],	
	"NLP_fp90_c10_d6": ["NLP", 90, 6, 100, 10],	
	"NLP_fp90_c10_d7": ["NLP", 90, 7, 100, 10],	
	"NLP_fp90_c10_d8": ["NLP", 90, 8, 100, 10],					
	"NLP_fp90_c12_d4": ["NLP", 90, 4, 100, 12],	
	"NLP_fp90_c12_d5": ["NLP", 90, 5, 100, 12],	
	"NLP_fp90_c12_d6": ["NLP", 90, 6, 100, 12],	
	"NLP_fp90_c12_d7": ["NLP", 90, 7, 100, 12],	
	"NLP_fp90_c12_d8": ["NLP", 90, 8, 100, 12]			
	
}


hpo_files = {key:"s3://{}/{}_minfreqpercent{}_mindepth{}_maxdepth100_numclades{}.txt".format(s3_bucket_name, nlp_hpo_filename_prefix, value[1], value[2], value[4]) for (key,value) in run_map.items()}

#create jobs for every vcf-hpo pair and store results in a folder defined by the filter combination thresholds (dir_name, which are the run_map/hpo_files dictionary keys)
jobs = [(dir_name, hpo_files[dir_name], vcf_file) for dir_name in hpo_files.keys() for vcf_file in vcf_files]
#add manual jobs
jobs = jobs + [("Manual", "s3://{}/{}".format(s3_bucket_name, manual_hpo_filename), vcf_file) for vcf_file in vcf_files]

#initialize multiprocessing
#ray.init(num_cpus=110, memory=(110*4*1024*1024*1024 + 50))
ray.init()


job_prefix = "" #optionally set this to add a prefix to the default output directory name
@ray.remote
def f(dir_name, hpo_file, vcf_file, maxretries=30):
	job_call = "docker run --mount source=exomiser-data,target=/usr/share/applications/exomiser-cli-12.1.0/data,readonly --rm -e exomiser_job_id={}{} -e exomiser_vcf_file={} -e exomiser_hpo_file={} -e exomiser_base_yml_file=s3://{}/test-analysis-exome.yml -e exomiser_Xmx=4g -e write_bucket={} -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY jiggyjsq/exomiser:12.1.0__hg19_2003__pheno_2003 python3.8 run_exomiser_job.py > out".format(job_prefix, dir_name, vcf_file, hpo_file, s3_bucket_name, s3_bucket_name)
	for i in range(maxretries): #keep trying maxretries times or until job successfully completes (exit code 0)
		job_code = os.system(job_call)
		if job_code == 0:
			break
	return


print("Estimated runtime: {} hours".format(round((len(jobs)/ray.available_resources()['CPU']) * 7 / 60)))
par_jobs = [f.remote(dir_name, hpo_file, vcf_file) for dir_name, hpo_file, vcf_file in jobs]
x = ray.get(par_jobs)

ray.shutdown()


#run this first one time to set up volume and AWS credential env vars
#ssh -i ~/.ssh/mykey.pem -XY ubuntu@ec2_IP
#export AWS_ACCESS_KEY_ID=??????
#export AWS_SECRET_ACCESS_KEY=??????
#docker volume create exomiser-data
#docker run --mount source=exomiser-data,target=/usr/share/applications/exomiser-cli-12.1.0/data --rm -it -e exomiser_job_id=test123 -e exomiser_vcf_file=s3://mybucket/Pfeiffer.vcf -e exomiser_hpo_file=s3://mybucket/NLPoutput_Pfeiffer.txt -e exomiser_base_yml_file=s3://mybucket/test-analysis-exome.yml -e exomiser_Xmx=4g -e write_bucket=mybucket -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY jiggyjsq/exomiser:12.1.0__hg19_2003__pheno_2003 python3.8 run_exomiser_job.py
