#!/usr/bin/env python
# coding: utf-8
import requests
from bs4 import BeautifulSoup
import pandas as pd
import tabula
from dateutil.parser import parse
import os

iv_visa_url_page = 'https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics/immigrant-visa-statistics/monthly-immigrant-visa-issuances.html'

def get_monthly_iv_urls(iv_visa_url_page) -> (list,list):
    '''This function returns two lists of URLs.
    Input:
        State Dept URL that has links to PDF files with immigrant visa statistics.
    Returns:
        pdf_links_by_visa_post, pdf_links_by_place_of_birth
        pdf_links_by_visa_post: A list of links to PDF files that report on different immigrant visas issed by visa posts. This is a list of bs4.element.Tag objects.
        pdf_links_by_place_of_birth: A list of links to PDF files that report on different immigrant visas aggregated by applicants place of birth/origin country. This is a list of bs4.element.Tag objects.
    '''
    # Get URL
    response = requests.get(iv_visa_url_page)
    # Parse URL response
    soup = BeautifulSoup(response.text, 'html.parser')
    # Get all links that refer to a PDF file.
    pdf_links = soup.select("a[href*='/content/dam/visas/Statistics/Immigrant-Statistics/MonthlyIVIssuances/']")
    # Get visa by post and visa by country of birth/origin
    pdf_links_by_visa_post = [x for x in pdf_links if 'IV Issuances by Post and Visa Class' in x.text]
    pdf_links_by_place_of_birth = [x for x in pdf_links if 'IV Issuances by FSC ' in x.text]
    print(f'URL links to PDF files fetched from {iv_visa_url_page}.')
    return pdf_links_by_visa_post, pdf_links_by_place_of_birth

def convert_to_int(x):
    if type(x) == float:
        return int(x)
    elif type(x) == str:
        return int(x.replace(',', ''))
    elif type(x) == int:
        return x
    else:
        print('Input is not float, str or int.')
        print(f'Input is {type(x)}')
        print('You need to modify the code to account for the new data type encountered')

def parse_uscis_pdf(pdf_link_visa_post: 'bs4.element.Tag') -> pd.DataFrame:
    # import tabula
    base_url = 'https://travel.state.gov/'
    file_url = base_url + pdf_link_visa_post['href'].lstrip('/')
    table_list = tabula.read_pdf(file_url, 
                                 pages='all',
                                 multiple_tables=True,
                                 pandas_options={'header':1},
                                 lattice=True,
                                 stream=True
                               )
    columns_of_interest = table_list[0].columns # Assumption: We are only interested in the columns parsed in page 1. If other columns are persed in subsequent pages, we will ignore them.
    uscis_table = pd.concat([x[columns_of_interest].dropna() for x in table_list])
    if 'Issuance' in uscis_table.columns:
        uscis_table.rename({'Issuance' : 'Issuances'}, axis=1, inplace=True)
    uscis_table =  uscis_table[uscis_table.Issuances != 'Issuances']
    # from dateutil.parser import parse
    dt_parse = parse(pdf_link_visa_post.text, fuzzy=True)
    uscis_table['year'] = dt_parse.year
    uscis_table['month'] = dt_parse.month
    uscis_table.Issuances = uscis_table.Issuances.apply(convert_to_int)
    return uscis_table

def pdf_to_parquet(pdf_list, save_directory) -> None:
    '''Parse pdf file and save as parquet.
    Input:
        pdf_list: a list containing links to PDF files, list of bs4.element.Tag
        save_directory: Directory where parquet file(s) will be saved.
    '''
    for pdf_link in pdf_list:
        try:
            uscis_df = parse_uscis_pdf(pdf_link)
            if not os.path.isdir(save_directory):
                os.mkdir(save_directory)
            df_month = uscis_df.year.unique()[0]
            df_year = uscis_df.month.unique()[0]
            uscis_df.to_parquet(path=f'{save_directory}{df_month}_{df_year}.parquet')
            print(f'Parquet save successful for {save_directory} {df_month}/{df_year}')
        except KeyError as e:
            print(f'Error raised for {pdf_link}')
            print(e)
    return 

def is_parquet_preset(input_string, folder_path) -> bool:
    '''This function checks whether parquet file is already present for the specified year-month. Retruns True if present.'''
    dt_obj = parse(input_string,fuzzy=True)
    file_path = f'{folder_path}{dt_obj.year}_{dt_obj.month}.parquet'
    
    if os.path.isfile(file_path):
        print(f'File aready present for {input_string}.')
        return True
    else:
        False

def main_func(source_url):
    # Getting links to all PDFs.
    pdf_links_by_visa_post, pdf_links_by_place_of_birth = get_monthly_iv_urls(iv_visa_url_page=source_url)
    
    # Dropping PDF-URLs that are already fetched.
    pdf_links_by_visa_post = [x for x in pdf_links_by_visa_post if not is_parquet_preset(input_string = x.text, folder_path='visa_post_data/')]
    pdf_links_by_place_of_birth = [x for x in pdf_links_by_place_of_birth if not is_parquet_preset(input_string = x.text, folder_path='place_of_birth_data/')]

    # Reading PDFs and saving parsed data as parquet files.
    pdf_to_parquet(pdf_links_by_visa_post, save_directory='visa_post_data/')
    pdf_to_parquet(pdf_links_by_place_of_birth, save_directory='place_of_birth_data/')

if __name__ == "__main__":
    main_func(iv_visa_url_page)