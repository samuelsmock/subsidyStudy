import pandas as pd

# Read only the first 10,000 rows of the CSV file
df = pd.read_csv('/Volumes/Untitled/germanyWideThesis/rawData/csv_Gebaeude_100m_Gitter/Geb100m.csv', encoding='cp1252', nrows=10000)

# Filter the DataFrame to only include rows with MERKMAL of interest
df = df[df['Merkmal'].isin(['HEIZTYP', 'GEBTYPGROESSE'])]

# Define a function to apply the appropriate prefix
def apply_prefix(x):
    if x == 'HEIZTYP':
        return 'he_'
    elif x == 'GEBTYPGROESSE':
        return 'si_'
    else:
        return ''

# Create a new column with prefixes
df['Prefix'] = df['Merkmal'].apply(apply_prefix)

# Pivot the DataFrame using Gitter_ID_100m as the pivot column
df_pivot = pd.pivot_table(df, index='Gitter_ID_100m', columns=['Prefix', 'Auspraegung_Text'], values='Anzahl', aggfunc='sum')

# Flatten the MultiIndex columns
df_pivot.columns = [f'{prefix}{value}' for prefix, value in df_pivot.columns]

# Replace NaN with 0 and convert values to integers
df_pivot = df_pivot.fillna(0).astype(int)

# Add sum columns for HEIZTYP and GEBTYPGROESSE
df_pivot['he_sum'] = df_pivot.filter(like='he_').sum(axis=1)
df_pivot['si_sum'] = df_pivot.filter(like='si_').sum(axis=1)

# Write the pivoted DataFrame to a new CSV file
df_pivot.to_csv('/Volumes/Untitled/germanyWideThesis/scratch/csv1.csv')

print(df_pivot.head(5))
