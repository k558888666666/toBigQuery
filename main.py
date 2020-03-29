from google.cloud import bigquery
from google.oauth2 import service_account
from pandas.core.frame import DataFrame
import pandas as pd

columns = ['Years'] + ['MaleName','MaleCount','FemaleName','FemaleCount'] * 100
Fnames = []
Mnames = []
Fcount = []
Mcount = []
female_result=[]
male_result=[]
credentials = service_account.Credentials.from_service_account_file(
    './lustrous-spirit-271312-062aa83c7046.json')
project_id = 'lustrous-spirit-271312'
client = bigquery.Client(credentials= credentials,project=project_id)
years = ['2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017']

with open('result.csv', 'w+') as result:
    #result.write(','.join(columns)+'\n')
    for i in range(len(years)):
        query_job = client.query('SELECT * FROM `lustrous-spirit-271312.mm.'+years[i]+'`')
        Fcount = []
        Mcount = []
        results = query_job.result()
        result.write(years[i] + ',')
        for line in results:
            if line.gender == 'F' and len(Fcount) < 100 :
                Fcount.append(','.join([line.name, str(line.count)]))
            if line.gender == 'M' and len(Mcount) < 100 :
                Mcount.append(','.join([line.name, str(line.count)]))
            if len(Fcount) >= 100 and len(Mcount) >= 100:
                break
        for i in range(100):
            result.write(Mcount[i])
            result.write(',')
            result.write(Fcount[i])
            if i < 99:
                result.write(',')
        result.write('\n')


print(len(Fcount),len(Mcount))


data = pd.read_csv('./result.csv')
data=DataFrame(data)
    
data.columns = ['years', 'MaleName0', 'MaleCount0', 'FemaleName0', 'FemaleCount0', 'MaleName1', 'MaleCount1', 'FemaleName1', 'FemaleCount1', 'MaleName2', 'MaleCount2', 'FemaleName2', 'FemaleCount2', 'MaleName3', 'MaleCount3', 'FemaleName3', 'FemaleCount3', 'MaleName4', 'MaleCount4', 'FemaleName4', 'FemaleCount4', 'MaleName5', 'MaleCount5', 'FemaleName5', 'FemaleCount5', 'MaleName6', 'MaleCount6', 'FemaleName6', 'FemaleCount6', 'MaleName7', 'MaleCount7', 'FemaleName7', 'FemaleCount7', 'MaleName8', 'MaleCount8', 'FemaleName8', 'FemaleCount8', 'MaleName9', 'MaleCount9', 'FemaleName9', 'FemaleCount9', 'MaleName10', 'MaleCount10', 'FemaleName10', 'FemaleCount10', 'MaleName11', 'MaleCount11', 'FemaleName11', 'FemaleCount11', 'MaleName12', 'MaleCount12', 'FemaleName12', 'FemaleCount12', 'MaleName13', 'MaleCount13', 'FemaleName13', 'FemaleCount13', 'MaleName14', 'MaleCount14', 'FemaleName14', 'FemaleCount14', 'MaleName15', 'MaleCount15', 'FemaleName15', 'FemaleCount15', 'MaleName16', 'MaleCount16', 'FemaleName16', 'FemaleCount16', 'MaleName17', 'MaleCount17', 'FemaleName17', 'FemaleCount17', 'MaleName18', 'MaleCount18', 'FemaleName18', 'FemaleCount18', 'MaleName19', 'MaleCount19', 'FemaleName19', 'FemaleCount19', 'MaleName20', 'MaleCount20', 'FemaleName20', 'FemaleCount20', 'MaleName21', 'MaleCount21', 'FemaleName21', 'FemaleCount21', 'MaleName22', 'MaleCount22', 'FemaleName22', 'FemaleCount22', 'MaleName23', 'MaleCount23', 'FemaleName23', 'FemaleCount23', 'MaleName24', 'MaleCount24', 'FemaleName24', 'FemaleCount24', 'MaleName25', 'MaleCount25', 'FemaleName25', 'FemaleCount25', 'MaleName26', 'MaleCount26', 'FemaleName26', 'FemaleCount26', 'MaleName27', 'MaleCount27', 'FemaleName27', 'FemaleCount27', 'MaleName28', 'MaleCount28', 'FemaleName28', 'FemaleCount28', 'MaleName29', 'MaleCount29', 'FemaleName29', 'FemaleCount29', 'MaleName30', 'MaleCount30', 'FemaleName30', 'FemaleCount30', 'MaleName31', 'MaleCount31', 'FemaleName31', 'FemaleCount31', 'MaleName32', 'MaleCount32', 'FemaleName32', 'FemaleCount32', 'MaleName33', 'MaleCount33', 'FemaleName33', 'FemaleCount33', 'MaleName34', 'MaleCount34', 'FemaleName34', 'FemaleCount34', 'MaleName35', 'MaleCount35', 'FemaleName35', 'FemaleCount35', 'MaleName36', 'MaleCount36', 'FemaleName36', 'FemaleCount36', 'MaleName37', 'MaleCount37', 'FemaleName37', 'FemaleCount37', 'MaleName38', 'MaleCount38', 'FemaleName38', 'FemaleCount38', 'MaleName39', 'MaleCount39', 'FemaleName39', 'FemaleCount39', 'MaleName40', 'MaleCount40', 'FemaleName40', 'FemaleCount40', 'MaleName41', 'MaleCount41', 'FemaleName41', 'FemaleCount41', 'MaleName42', 'MaleCount42', 'FemaleName42', 'FemaleCount42', 'MaleName43', 'MaleCount43', 'FemaleName43', 'FemaleCount43', 'MaleName44', 'MaleCount44', 'FemaleName44', 'FemaleCount44', 'MaleName45', 'MaleCount45', 'FemaleName45', 'FemaleCount45', 'MaleName46', 'MaleCount46', 'FemaleName46', 'FemaleCount46', 'MaleName47', 'MaleCount47', 'FemaleName47', 'FemaleCount47', 'MaleName48', 'MaleCount48', 'FemaleName48', 'FemaleCount48', 'MaleName49', 'MaleCount49', 'FemaleName49', 'FemaleCount49', 'MaleName50', 'MaleCount50', 'FemaleName50', 'FemaleCount50', 'MaleName51', 'MaleCount51', 'FemaleName51', 'FemaleCount51', 'MaleName52', 'MaleCount52', 'FemaleName52', 'FemaleCount52', 'MaleName53', 'MaleCount53', 'FemaleName53', 'FemaleCount53', 'MaleName54', 'MaleCount54', 'FemaleName54', 'FemaleCount54', 'MaleName55', 'MaleCount55', 'FemaleName55', 'FemaleCount55', 'MaleName56', 
'MaleCount56', 'FemaleName56', 'FemaleCount56', 'MaleName57', 'MaleCount57', 'FemaleName57', 'FemaleCount57', 'MaleName58', 'MaleCount58', 'FemaleName58', 'FemaleCount58', 'MaleName59', 'MaleCount59', 'FemaleName59', 'FemaleCount59', 'MaleName60', 'MaleCount60', 'FemaleName60', 'FemaleCount60', 'MaleName61', 'MaleCount61', 'FemaleName61', 'FemaleCount61', 'MaleName62', 'MaleCount62', 'FemaleName62', 'FemaleCount62', 'MaleName63', 'MaleCount63', 'FemaleName63', 'FemaleCount63', 'MaleName64', 'MaleCount64', 'FemaleName64', 'FemaleCount64', 'MaleName65', 'MaleCount65', 'FemaleName65', 'FemaleCount65', 'MaleName66', 'MaleCount66', 'FemaleName66', 'FemaleCount66', 'MaleName67', 'MaleCount67', 'FemaleName67', 'FemaleCount67', 'MaleName68', 'MaleCount68', 'FemaleName68', 'FemaleCount68', 'MaleName69', 'MaleCount69', 'FemaleName69', 'FemaleCount69', 'MaleName70', 'MaleCount70', 'FemaleName70', 'FemaleCount70', 'MaleName71', 'MaleCount71', 'FemaleName71', 'FemaleCount71', 'MaleName72', 'MaleCount72', 'FemaleName72', 'FemaleCount72', 'MaleName73', 'MaleCount73', 'FemaleName73', 'FemaleCount73', 'MaleName74', 'MaleCount74', 'FemaleName74', 'FemaleCount74', 'MaleName75', 'MaleCount75', 'FemaleName75', 'FemaleCount75', 'MaleName76', 'MaleCount76', 'FemaleName76', 'FemaleCount76', 'MaleName77', 'MaleCount77', 'FemaleName77', 'FemaleCount77', 'MaleName78', 'MaleCount78', 'FemaleName78', 'FemaleCount78', 'MaleName79', 'MaleCount79', 'FemaleName79', 'FemaleCount79', 'MaleName80', 'MaleCount80', 'FemaleName80', 'FemaleCount80', 'MaleName81', 'MaleCount81', 'FemaleName81', 'FemaleCount81', 'MaleName82', 'MaleCount82', 'FemaleName82', 'FemaleCount82', 'MaleName83', 'MaleCount83', 'FemaleName83', 'FemaleCount83', 'MaleName84', 'MaleCount84', 'FemaleName84', 'FemaleCount84', 'MaleName85', 'MaleCount85', 'FemaleName85', 'FemaleCount85', 'MaleName86', 'MaleCount86', 'FemaleName86', 'FemaleCount86', 'MaleName87', 'MaleCount87', 'FemaleName87', 'FemaleCount87', 'MaleName88', 'MaleCount88', 'FemaleName88', 'FemaleCount88', 'MaleName89', 'MaleCount89', 'FemaleName89', 'FemaleCount89', 'MaleName90', 'MaleCount90', 'FemaleName90', 'FemaleCount90', 'MaleName91', 'MaleCount91', 'FemaleName91', 'FemaleCount91', 'MaleName92', 'MaleCount92', 'FemaleName92', 'FemaleCount92', 'MaleName93', 'MaleCount93', 'FemaleName93', 'FemaleCount93', 'MaleName94', 'MaleCount94', 'FemaleName94', 'FemaleCount94', 'MaleName95', 'MaleCount95', 'FemaleName95', 'FemaleCount95', 'MaleName96', 'MaleCount96', 'FemaleName96', 'FemaleCount96', 'MaleName97', 'MaleCount97', 'FemaleName97', 'FemaleCount97', 'MaleName98', 'MaleCount98', 'FemaleName98', 'FemaleCount98', 'MaleName99', 'MaleCount99', 'FemaleName99', 'FemaleCount99']

data.to_gbq(destination_table = 'mm.myblood',project_id=project_id)