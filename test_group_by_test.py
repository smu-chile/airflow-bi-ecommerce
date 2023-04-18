import pandas as pd
df = pd.DataFrame({'a': [1,1,1,2,2,3,1,2,2,3,3,3],
                   'b': [1,2,3,1,2,3,1,2,3,1,2,3],
                   'c': [1,1,1,1,2,2,2,2,3,3,3,3]},
                   columns = ['a', 'b', 'c'])
print(df)
df = df.sort_values('a')
print(df)
