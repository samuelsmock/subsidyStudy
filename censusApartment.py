import pandas as pd

# Read the CSV file into a pandas DataFrame
df = pd.read_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/2011census/csv_Wohnungen_100m_Gitter/Wohnungen100m.csv', encoding = 'cp1252')

# print(df.head(5))


# Filter the DataFrame to only include rows where MERKMAL of interest
df = df[df['Merkmal'].isin(['INSGESAMT'])]
       

df.reset_index(drop=True)
# Pivot the DataFrame using Gitter_ID_100m as the pivot column
df = pd.pivot_table(df, index='Gitter_ID_100m', columns='Auspraegung_Text', values='Anzahl', aggfunc='sum')


# Replace NaN with 0 and convert values to integers
df = df.fillna(0).astype(int)

# Convert all columns to integer except for "Gitter_ID_100m"
int_columns = [col for col in df.columns if col != "Gitter_ID_100m"]
df[int_columns] = df[int_columns].astype(int)

#print(df.columns)
#print(df.head(5))

# Write the pivoted DataFrame to a new CSV file
df.to_csv('/Users/sunshinedaydream/Desktop/thesis_data_local/spatial_data/2011census/csv_Wohnungen_100m_Gitter/apartment_totals_100m.csv')
