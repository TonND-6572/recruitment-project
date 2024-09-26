from util.util import *
from hdfsDriver.Driver import Driver

import pandas as pd 
import argparse

args = argparse.ArgumentParser()
args.add_argument('--source-path', type=str, default='/recruitment/bronze/glints/computer-information-technology/2024/09/09/', required=False)
args.add_argument("--jobCategory", type=str, default="computer-information-technology", required=False)
args.add_argument("--source", type=str, default="glints", required=False)
args.add_argument('--timeExecute', type=str)

arg = args.parse_args()
timeExecute = arg.timeExecute.split('-') # YYYY-MM-DD
year = timeExecute[0]
month = timeExecute[1]
day = timeExecute[2]

destination = "/recruitment/silver/{}/{}/{}/{}/{}/".format(arg.source, arg.jobCategory, year, month, day)

if __name__ == "__main__":
    driver = Driver(url="http://wakuwa:9870", user="hadoop")
    
    df = driver.read_from_folder(arg.source_path)
    
    new_df = df.copy()
    
    ### transform salary
    new_df["t_salary"] = df["Salary"].apply(lambda x: extract_salary(x[0]))
    new_df["avg_salary"] = new_df["t_salary"].apply(lambda x: (x[0] + x[1]) / 2)
    new_df["currency"] = new_df["t_salary"].apply(lambda x: x[2].replace("₫", "VND"))

    # 5 range [0, 5*1e6, 1.2*1e7, 2.0*1e7, 2.7*1e7]
    bins = [0, 5e6, 1.2e7, 2.0e7, 2.7e7, float('inf')]
    bin_labels = ['<5M', '5M-12M', '12M-20M', '20M-27M', '>27M']

    new_df['salary_VND'] = new_df.apply(salary_bin, axis = 1)
    new_df['salary_range'] = pd.cut(new_df['salary_VND'], 
                                        bins=bins, 
                                        labels=bin_labels,
                                        ordered=True)
    
    new_df['salary_range'] = new_df['salary_range'].cat.add_categories(['Unknown']).fillna('Unknown')
    new_df = new_df.sort_values('salary_range')

    ### transform location
    new_df['t_location'] = df['Location'].apply(lambda x: [i.strip() for i in x.split('·')])
    new_df['location'] = new_df['t_location'].apply(lambda x: x[0])
    new_df['work_type'] = new_df['t_location'].apply(lambda x: x[1])
    new_df['work_time_type'] = new_df['t_location'].apply(lambda x: x[2])
    new_df['company_name'] = df['Company'].apply(lambda x: x.split('·')[1].strip())

    ### transfrom date
    new_df['num_day'] = df['Last_updated'].apply(lambda x: x.split(' ')[2])
    new_df['day'] = df['Last_updated'].apply(lambda x: x.split(' ')[3])
    new_df['Created_time'] = pd.to_datetime(new_df['Created_time'], format='%d-%m-%Y %H:%M:%S')
    new_df['Last_updated'] = new_df.apply(lambda x: ajdust_day(x['num_day'], x['day'], x['Created_time']), axis=1)

    drop_cols = ['t_salary', 't_location', 'Company', 'Link', 'Location', 'Salary', 'num_day', 'day']
    new_df.drop(columns=drop_cols, inplace=True)

    # new_df.to_csv('./Data/computer-information-technology/silver_layer.csv', index=False)
    driver.write(destination+arg.jobCategory+'.csv', new_df, 'csv')