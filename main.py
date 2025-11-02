import pandas as pd
from utils.quality_check import QualityCheckExpectation

df = pd.read_csv('./data/airports.csv')
# print(df)

qc = QualityCheckExpectation()
result = qc.do_quality_check(df, "airports_quality_checks")

message, score = qc.generate(result, module="Airports Data")
print(message)