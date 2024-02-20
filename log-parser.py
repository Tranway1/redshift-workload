import pandas as pd
import openpyxl
# Read the pickle file
data = pd.read_pickle('workloads/c2_results.pkl')


def process_row(row):
    # Extracting attributes from the 'result' column, which is a dictionary
    result = row['result']
    for key in result:
        if key not in ['ResponseMetadata', 'HTTPHeaders']:
            row[key] = result[key]

    # Convert 'CreatedAt' and 'UpdatedAt' to UNIX time
    row['CreatedAt_UNIX'] = row['CreatedAt'].timestamp()
    row['UpdatedAt_UNIX'] = row['UpdatedAt'].timestamp()

    # Calculate unix_duration
    row['unix_duration'] = row['UpdatedAt_UNIX'] - row['CreatedAt_UNIX']

    # Extract profiler_id from 'QueryString'
    profiler_id = None
    if 'profiler_id=' in row['QueryString']:
        profiler_id = row['QueryString'].split('profiler_id=')[1].split()[0]
    row['ProfilerId'] = profiler_id

    # Convert timezone-aware datetimes to timezone-naive
    row['CreatedAt'] = row['CreatedAt'].replace(tzinfo=None)
    row['UpdatedAt'] = row['UpdatedAt'].replace(tzinfo=None)

    return row


# Apply the function to each row
expanded_data = data.apply(process_row, axis=1)

# Drop the original 'result' column and any other columns you don't need
expanded_data.drop(columns=['result'], inplace=True)

# Save the expanded DataFrame to an Excel file
expanded_data.to_excel('workloads/c2_results_parsed.xlsx', index=False)
