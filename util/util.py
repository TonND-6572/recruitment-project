import re
import numpy as np
import pandas as pd

def extract_salary(row):
    unit_dic = {'N': 1000, 'Tr': 1000000}

    def replace_unit(amount, unit=None):
        return amount * unit_dic[unit] if unit else amount

    pattern = r'([₫USD]+)\s*(\d+(?:,\d+\s*)?(?:\s\w+)?)(?:-(\d+(?:,\d+)?\s*[a-zA-Z]+))?'
    match = re.search(pattern, row)
    
    if match:
        currency = match.group(1)
        
        min_salary_group = match.group(2).split(' ')
        max_salary_group = match.group(3).split(' ') if match.group(3) else min_salary_group

        min_salary = float(min_salary_group[0].replace(',', '.'))
        max_salary = float(max_salary_group[0].replace(',', '.'))

        if len(min_salary_group) == 2:
            min_salary = replace_unit(min_salary, min_salary_group[1])
        if min_salary < max_salary:
            min_salary = replace_unit(min_salary, max_salary_group[1])
            
        try:
            max_salary = replace_unit(max_salary, max_salary_group[1])
        except:
            print(row)
        return (int(min_salary), int(max_salary), currency)

    if 'Công ty không công khai' in row:
        return (0, 0, 'Unknown')

    return (0, 0, 'Unknown')

def salary_bin(row):
    curr_dic = {
        'USD': 24000,
        '₫': 1,
        'VND': 1
    }

    return row['avg_salary'] * curr_dic[row['currency']] if row['avg_salary'] > 0 else np.nan

def ajdust_day(date, date_str, curr_date):
    try:
        date = int(date)
        if date_str == 'ngày':
            return curr_date - pd.DateOffset(days=date)
        elif date_str == 'giờ':
            return curr_date - pd.DateOffset(hours=date)
        elif date_str == 'tháng':
            return curr_date - pd.DateOffset(months=date)
        elif date_str == 'năm':
            return curr_date - pd.DateOffset(years=date)
        else:
            return curr_date
    except:
        return curr_date