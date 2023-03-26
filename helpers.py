import os, re, json
import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
from unidecode import unidecode
from pdfminer.high_level import extract_text

def get_clinical_tests_available(file:str) -> dict:
    """
    Load available clinical tests from json file with each regular expression
    used to extract your results and personal information based on template
    file

    Args:
        file : filename
    Returns:
        dictionary with all clinical testes splited by regex used to extract 
        results from text
    """

    # reading the data from the file
    with open(file) as f:
        data = f.read()
  
    # reading the file and close it
    js = json.loads(data)
      
    return js


def get_files(path:str, type:str) -> list:
    """
    List all files from specific type from target path

    Args:
        path: target path
        type: extension type without dot (.), ex: pdf
    Returns:
        list with all files
    """
    
    files = [os.path.join(path, f) for f in os.listdir(path)] 
    files = [f for f in files if f[-len(type):] == type]

    return files


def extract_text_from_pdf(file:str) -> str:
    """
    Extract all text from pdf files

    """

    data = extract_text(file)
    data = data.lower()
    data = re.sub(" +", " ", data)
    data = unidecode(data)

    return data


def get_id(text, template = 'hospital_sao_paulo'):
    """Extract personal information from patient as name, sex, birthday etc"""

    name = re.findall(r"nome: (\w.+) sexo:", text)[0].strip()
    sex = re.findall(r"sexo: (\w)", text)[0].strip()
    birthday = re.findall(r"data de nascimento: (\w.+)", text)[0].strip()
    collection = re.findall(r"data de coleta: (\w.+)", text)[0].strip().split(" as ")
    result = {"nome" : name
              ,"sexo" : sex
              ,"dt_nascimento" : birthday
              ,"dt_coleta" : collection[0]
              ,"hora_coleta" : collection[1]
              }
    return result


def remove_header(text, template = 'hospital_sao_paulo'):
    """
    
    """

    start_header_list = []
    end_header_list = [0]
    new_text = ""

    for m in re.finditer(r"hospital sao paulo$", text, flags=re.MULTILINE):
        start_header_list.append(m.start())
    start_header_list.append(len(text))

    for m in re.finditer(r"^resultados$", text, flags=re.MULTILINE):
        end_header_list.append(m.end())

    for i, s,e in [*zip(list(range(len(start_header_list))),start_header_list, end_header_list)]:
        new_text = new_text + text[e:s]

    return new_text


def remove_trailer(text, template = 'hospital_sao_paulo'):
    """
    
    """

    start_trailer_list = []
    end_trailer_list = [0]
    new_text = ""

    for m in re.finditer(r"^a correta interpretacao ", text, flags=re.MULTILINE):
        start_trailer_list.append(m.start())
    start_trailer_list.append(len(text))

    for m in re.finditer(r"vila clementino, sao paulo - sp$", text, flags=re.MULTILINE):
        end_trailer_list.append(m.end())

    for i, s,e in [*zip(list(range(len(start_trailer_list))),start_trailer_list, end_trailer_list)]:
        new_text = new_text + text[e:s]

    return new_text


def remove_empty_lines(text):
    """
    
    """

    text = "\n".join([ll.rstrip() for ll in text.splitlines() if ll.strip()])

    return text


def remove_date_lines(text):
    """
    
    """

    text = "\n".join([line for line in text.splitlines() if line.startswith("data de coleta") == False])

    return text


def parse_results(text, clinical_tests):
    """
    
    """

    results = {}

    for test in clinical_tests['type_1']['tests']:
        for test_str in re.finditer(r"^" + test + "$", text, flags=re.MULTILINE):
            start_position = test_str.end()
            regex =  clinical_tests['type_1']['regex']
            try:
                resultado = re.findall(regex, (text[start_position:]))[0]
                resultado = float(re.sub(",", ".", resultado))
            except: 
                resultado = None
            results[test] = resultado
            break #this break is for run the inner for loop just once

    for test in clinical_tests['type_2']['tests']:
        for test_str in re.finditer(r"^" + test, text, flags=re.MULTILINE):
            start_position = test_str.end()
            regex =  clinical_tests['type_2']['regex']
            try:
                resultado = re.findall(regex, (text[start_position:]))[0]
                resultado = float(re.sub(",", ".", resultado))
            except: 
                resultado = None
            # print(f"teste:{test}, pos:{start_position}")
            # print(results)
            results[test] = resultado
            break #this break is for run the inner for loop just once

    for test in clinical_tests['type_3']['tests']:
        for test_str in re.finditer(r"^" + test + "$", text, flags=re.MULTILINE):
            start_position = test_str.end()
            regex =  clinical_tests['type_3']['regex']#r"resultado .+\n.*?(-?\d+\,?\d+)"
            try:
                resultado = re.findall(regex, (text[start_position:]))[0]
                resultado = float(re.sub(",", ".", resultado))
            except: 
                resultado = None
            # print(f"teste:{test}, pos:{start_position}")
            results[test] = resultado
            break #this break is for run the inner for loop just once
    
    return results

def build_table_with_results(files: list, clinical_tests: dict) -> pd.DataFrame:

    df_main = pd.DataFrame()

    for file in files:
        text = extract_text_from_pdf(file)
        id = get_id(text)
        clean_text = remove_date_lines(remove_empty_lines(remove_trailer(remove_header(text))))
        results = parse_results(clean_text, clinical_tests)
        id.update({"arquivo" : file.split('/')[-1]})
        id.update(results)
        df_id = pd.DataFrame.from_dict(data = id, orient='index').T
        df_main = pd.concat([df_main, df_id], axis = 0)
    
    return df_main


def handle_dates(df, birthday_col, collection_col, format):

    for col in [birthday_col, collection_col]:
        df[col] = [dt.datetime.strptime(str(x), format).date() for x in df[col].values]

    df['idade'] = [np.around((y-x)/dt.timedelta(365),1) for (x,y) in df[[birthday_col, collection_col]].values]

    return df


def combine_records(df, coalesce_cols=['nome', 'sexo', 'dt_nascimento', 'dt_coleta'], combine_col='arquivo'):
    """
    
    """
    
    df = (df.sort_values(coalesce_cols)
            .reset_index()
            .drop(['index'], axis = 1)
    )

    df_colaesce = df.groupby(coalesce_cols, sort=True).first()
    df_join = (df.groupby(coalesce_cols, sort=True)
                        .agg({combine_col: '||'.join})
    )

    df = (df_colaesce.drop([combine_col], axis = 1)
            .join(df_join).reset_index()
    )

    return df


def clean_col_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    
    """

    cols = df.columns.values
    df.columns = [re.sub(" ","_", re.sub("\s?-|:|\s\s+", "", str(x))) for x in cols]

    return df


def rebuild_table_by_time(df: pd.DataFrame, group_cols: list, time_col: str) -> pd.DataFrame:
    """
    """

    df['time'] = (df.groupby(group_cols, sort=time_col).cumcount())

    df = df.pivot(index = group_cols, columns='time')

    col_names = [str(x)+"_t"+str(y) for (x,y) in df.columns]

    df.columns = col_names

    return df

