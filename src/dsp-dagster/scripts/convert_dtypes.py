import pandas as pd

df = pd.read_excel("./data/Storm_Data_Small.xlsx")



print(df[df.isnull().any(axis=1)])