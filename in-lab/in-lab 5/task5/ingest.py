import pandas as pd

from sqlalchemy import create_engine

df = pd.read_csv('/titanic.csv')

engine = create_engine('postgresql://root:root@postgres:5432/titanic')

if(engine.connect()):
	print('connected succesfully')
else:
	print('failed to connect')


df.to_sql(name = 'titanic_dataset',con = engine,if_exists='replace')

