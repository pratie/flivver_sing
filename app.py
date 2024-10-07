import pandas as pd

# Create sample dataframes (replace these with your actual dataframes)
df1 = pd.DataFrame(columns=['ID', 'Name', 'Age'])
df2 = pd.DataFrame(columns=['id', 'NAME', 'Gender'])
df3 = pd.DataFrame(columns=['Id', 'name', 'City'])
df4 = pd.DataFrame(columns=['ID', 'Name', 'Salary'])

# List of dataframes
dataframes = [df1, df2, df3, df4]

# Function to get lowercase column names
def get_lowercase_columns(df):
    return [col.lower() for col in df.columns]

# Get all column names (lowercase) from all dataframes
all_columns = [get_lowercase_columns(df) for df in dataframes]

# Find common columns
common_columns = set(all_columns[0])
for columns in all_columns[1:]:
    common_columns.intersection_update(columns)

# Convert back to list
common_columns = list(common_columns)

print("Common columns across all dataframes (case-insensitive):")
print(common_columns)
