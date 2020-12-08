#!/usr/bin/python

##################################################
## This script filters NLP-derived HPO terms based on frequency, depth, and diversity (maxClades), and
## uploads filtered term lists to S3
## 
## The script assumes that HPO terms are saved in 
## files named "####-##.csv" where ####-## is the Manton ID following MAN_ in the VCF filenames, and 
## are in the following format (Clinthink raw) for each patient:
## Criterion,Frequency
## hp0002813_Abnormality_of_limb_bone_morphology,32
##  hp0012531_Pain,16
## 
## The individual patient HPO terms are first combined into a single file:
## PatientID1
## Criterion,Frequency
## hp0002813_Abnormality_of_limb_bone_morphology,32
##  hp0012531_Pain,16
## ## PatientID2
## Criterion,Frequency
## hp0001298_Encephalopathy,19
## hp0002273_Tetraparesis,10
##
## Next the combined lists are filtered for each of the 294 NLP filter combinations.
## The filtered HPO terms are written back in the same format (combined patient Clinithink format) in a user-specified directory, 
## with a user-specified prefix.
## 
## This script assumes that default AWS credentials are set up on the local machine where this is run.
## 
## 
## This script was written to support the following paper
## "Parikh JR, Genetti CA et al.  A data-driven architecture using natural language processing to improve phenotyping efficiency and accelerate genetic diagnoses of rare disorders."
## 
## and can be run as follows:
## python3.8 post_process_NLP.py pheno_pipeline_params.yaml
##################################################
## Author: Jiggy Parikh
## Version: 0.1.0
## Email: jiggy@jsquarelabs.com
## Status: Dev
##################################################

import json
import os
import sys
import os.path as path
from datetime import date
import pandas as pd
import re
import boto3
import time
import requests
import yaml
import io


#get directories/filenames from yaml (first command line argument or override the line below)
yaml_filename = sys.argv[1]
#yaml_filename = "/home/ubuntu/pheno_pipeline_params.yaml"

with open(yaml_filename, "r") as fh:
	yaml_data = yaml.safe_load(fh)

workdir = yaml_data["workdir"] #working directory where filtered output files are stored within nlp_output_dir
nlp_terms_filename = yaml_data["nlp_terms_filename"] #this file contains the Clinithink raw format list of HPO terms
nlp_output_dir = yaml_data["nlp_output_dir"] #this is the local directory where the filtered lists in the same format will be stored
nlp_hpo_filename_prefix = yaml_data["nlp_hpo_filename_prefix"] #this prefix is appended to each filtered filename
nlp_terms_orig_dirname = yaml_data["nlp_terms_orig_dirname"] #this is the "folder" on S3 where the original raw Clinithink output is stored as individual files

s3_bucket_name = yaml_data["s3_bucket_name"]

#set working directory, create it if it does not exist
os.makedirs(workdir, exist_ok=True)
os.chdir(workdir)


s3 = boto3.resource('s3')

#get individual nlp hpo files from S3, concatenate them and upload combined file back to S3
#first map csv filenames to vcf file names 
vcf_files = yaml_data['vcf_files'] #assume 1-to-1 matching
key_map = dict(zip([nlp_terms_orig_dirname + "/" + re.match("s3.+MAN_(\d+-01).+\.vcf", vcf_file).groups()[0]+".csv" for vcf_file in vcf_files], [re.match("s3.+(MAN_.+)\.vcf", vcf_file).groups()[0] for vcf_file in vcf_files]))

fhw = open(nlp_terms_filename, "w")
for NLP_csv_filename in key_map.keys():
	#read in data
	obj = s3.meta.client.get_object(Bucket=s3_bucket_name, Key=NLP_csv_filename)
	df = obj['Body'].read().decode()
	#replace uppercase HP with lowercase HP for consistency with previous formats and docker app requirements
	df = re.sub("\nHP", "\nhp", df)
	fhw.write("{}\n{}".format(key_map[NLP_csv_filename], df))
fhw.close()	


#upload combined nlp hpo file to S3
s3.Bucket(s3_bucket_name).upload_file(os.path.join(workdir, nlp_terms_filename), nlp_terms_filename)

#get hpo summary files
s3.Bucket(s3_bucket_name).download_file("hpo_multishortest_paths_stats.csv", os.path.join(workdir, "hpo_multishortest_paths_stats.csv"))
s3.Bucket(s3_bucket_name).download_file("hpo_multishortest_paths.csv", os.path.join(workdir, "hpo_multishortest_paths.csv"))



#define runs in dictionary as:
#RunName: ["Manual | NLP", minPercentFrequency, minDepth, maxDepth, maxClades]
run_map = {
	"Manual": ["Manual", None, None, None, None],
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


#Step 1: Filter NLP list and upload filtered NLP output to S3
def filter_NLP(input_filename = "UnDx_output.txt", min_freq=0, min_depth=0, max_depth=100, num_clades=100, output_dir="UnDx_filtered_NLP_outputs", filename_prefix="UnDx_NLPoutput"):

	#fixed parameters that can be adjusted later
	min_freq_as_percent = True      
	min_terms = 5
	clade_depth_file = "hpo_multishortest_paths_stats.csv" 
	clades_list_file = "hpo_multishortest_paths.csv" 
	
	output_filename = "{}_minfreq{}{}_mindepth{}_maxdepth{}_numclades{}.txt".format(filename_prefix, "percent" if min_freq_as_percent else "", min_freq, min_depth, max_depth, num_clades)
	if os.path.exists(os.path.join(output_dir, output_filename)):
		return(output_filename)
	
	fh = open(input_filename)
	data = []
	for line in fh:
		line = line.strip().split(",")
		if len(line) == 1:
			key = line[0]
		elif line[0] == "Criterion":
			pass
		else:
			data.append([key, line[0], int(line[1])])

	fh.close()

	data = pd.DataFrame(data, columns=["Patient", "Criterion", "Frequency"])

	#compute percentile frequency per patient
	data["percentile"] = data.groupby("Patient").Frequency.transform(lambda x: x.rank() / len(x))     

	#merge with clade/depth data
	data.loc[:, "HPO_ID"] = data["Criterion"].apply(lambda x: x.split("_")[0].replace("hp", "HP:"))
	clade_depth_data = pd.read_csv(clade_depth_file)
	clades_list_data = pd.read_csv(clades_list_file) #clades list expanded for easier merging
	data = data.join(clade_depth_data.set_index("HPO_ID"), how="left", on="HPO_ID")


	#filter by min_freq
	if min_freq_as_percent:
		data_to_keep = data.loc[data["percentile"] >= (min_freq/100), :].copy()
	else:
		data_to_keep = data.loc[data["Frequency"] >= min_freq, :].copy()


	#filter by min and max depth (keep nans)
	data_to_keep = data_to_keep.loc[((data_to_keep.min_path_length.notna()) & (data_to_keep.min_path_length >= min_depth) & (data_to_keep.min_path_length <= max_depth)) | data_to_keep["min_path_length"].isna(), :]

	#filter by top clades sorted by average frequency (always use percentile)
	#first set nan clades to "unknown"
	data_to_keep = data_to_keep.join(clades_list_data.set_index("HPO_ID"), how="left", on="HPO_ID", lsuffix="", rsuffix="_2")
	data_to_keep.loc[data_to_keep.root_phenos_2.isna(), "root_phenos_2"] = "unknown"
	data_to_keep["clade_frequency"] = data_to_keep.groupby(["Patient", "root_phenos_2"]).percentile.transform(lambda x: x.mean())
	data_to_keep["clade_frequency_rank"] = data_to_keep.groupby(["Patient"]).clade_frequency.transform(lambda x: x.rank(ascending=False, method="dense"))
	data_to_keep = data_to_keep.loc[((data_to_keep.root_phenos_2 != "unknown") & (data_to_keep.clade_frequency_rank <= num_clades)) | data_to_keep.root_phenos_2.eq("unknown"), :]
	data_to_keep = data_to_keep[data_to_keep.columns.drop(list(data_to_keep.filter(regex='_2$')))]
	data_to_keep = data_to_keep[["Patient", "Criterion", "Frequency"]].drop_duplicates()
	#13901 rows


	#check which patients have fewer than min_terms left after filtering
	data = data[["Patient", "Criterion", "Frequency"]].drop_duplicates()
	patient_term_counts = data_to_keep.groupby("Patient").Patient.count() 
	patients_to_reset = patient_term_counts[patient_term_counts <= min_terms].index.to_list()
	#add patients that have been removed
	patients_to_reset = list((set(data.Patient) - set(data_to_keep.Patient)).union(set(patients_to_reset)))

	#first drop patients to reset
	data_to_keep = data_to_keep.loc[data_to_keep.Patient.isin(patients_to_reset) != True,:] #13849 rows (22 and 30 rows to be replaced with 46 and 67 rows respectively)
	#next add new rows from original dataset
	data_to_keep = data_to_keep.append(data.loc[data.Patient.isin(patients_to_reset),:]) #13962 rows


	#write out filtered data
	#group by patient

	fhw = open(os.path.join(output_dir, output_filename), "w")
	for name, group in data_to_keep.groupby("Patient"):
		fhw.write("{}\nCriterion,Frequency\n".format(name))
		for ind, row in group.iterrows():
			fhw.write("{},{}\n".format(row.Criterion, row.Frequency))

	fhw.close()

	return(output_filename)


#create directory to store filtered nlp outputs if it does not exist
os.makedirs(nlp_output_dir, exist_ok=True)	
#run filter nlp for all runs
nlp_output_file_map = {runName: filter_NLP(input_filename = nlp_terms_filename, min_freq=run_map[runName][1], min_depth=run_map[runName][2], max_depth=run_map[runName][3], num_clades=run_map[runName][4], output_dir=nlp_output_dir, filename_prefix=nlp_hpo_filename_prefix) for runName in run_map.keys() if run_map[runName][0] == "NLP"}

#upload to S3
nlp_output_files = list(nlp_output_file_map.values())
for nlp_output_file in nlp_output_files:
	s3.Bucket(s3_bucket_name).upload_file(os.path.join(nlp_output_dir, nlp_output_file), nlp_output_file)
	

