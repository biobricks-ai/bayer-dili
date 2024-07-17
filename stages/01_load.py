import pandas as pd
from pathlib import Path

# Directory containing the files
download_dir = Path('./downloads')

# PROCESS THE DATA FILES =============================================================
data_df = pd.read_excel(download_dir / 'MIE_multitask_model_data_20230601.xlsx')

# all of the data in data_df is in the first column and comma separated
# we fix this to get a normal dataframe
colnames = data_df.columns[0].split(',')
split_data_df = data_df.iloc[:, 0].str.split(',', expand=True)
split_data_df = split_data_df.iloc[:, :len(colnames)]
split_data_df.columns = colnames

# Pivot the dataframe longer on canonical_smiles and molid
long_data_df = split_data_df.melt(id_vars=['canonical_smiles', 'molid'], var_name='variable', value_name='value')

long_data_df['value'] = pd.to_numeric(long_data_df['value'], errors='coerce')
long_data_df = long_data_df.dropna(subset=['value'])

# Transform all the 0.0 values to 'negative' and all the 1.0 values to 'positive'
long_data_df['value'] = long_data_df['value'].replace({0.0: 'negative', 1.0: 'positive'})

# Ensure the correct data types for each column
long_data_df['canonical_smiles'] = long_data_df['canonical_smiles'].astype(str)
long_data_df['molid'] = long_data_df['molid'].astype(str)
long_data_df['variable'] = long_data_df['variable'].astype(str)
long_data_df['value'] = long_data_df['value'].astype(str)


# Write the result to the ./brick directory as a parquet file
output_dir = Path('./brick')
output_dir.mkdir(parents=True, exist_ok=True)
long_data_df.to_parquet(output_dir / 'bayer_dili_data.parquet', index=False)

# PROCESS THE ENDPOINT DEFINITIONS FILE
endpoints_df = pd.read_excel(download_dir / 'endpoint_names.xlsx')
endpoints_df.to_parquet(output_dir / 'bayer_dili_properties.parquet', index=False)




