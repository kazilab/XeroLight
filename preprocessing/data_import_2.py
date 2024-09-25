import pandas as pd
import re
import os

def process_isotope_data(file_path, exp_id):
    """
    Processes isotope data from a CSV file, transforms the dataframe, 
    and returns a clean version with merged sample names, header, and values.
    
    Args:
        file_path (str): Path to the CSV file containing isotope data.

    Returns:
        pd.DataFrame: Processed DataFrame with sample names, parameters, and values.
    """
    # Load the data
    df = pd.read_csv(file_path, sep=';', index_col=None, header=None, low_memory=False)
    
    # Extract the file name from the file path
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    # Rename first two columns to 'sample_index' and 'sample_name', set index name
    df = df.rename(columns={0: 'sample_index', 1: 'sample_name'})
    df.index.name = 'row_index'
    
    # Extract sample information (starting from row 5)
    sample_name = df[['sample_index', 'sample_name']].iloc[4:, :]
    # Convert 'sample_index' to integers
    sample_name['sample_index'] = sample_name['sample_index'].astype(int)

    # Drop 'sample_index' and 'sample_name' from the main dataframe
    df.drop(columns=['sample_index', 'sample_name'], inplace=True)

    # Transpose the dataframe and rename first 4 columns to param0, param1, param2, param3
    dft = df.T
    dft = dft.rename(columns={0: 'param0', 1: 'param1', 2: 'param2', 3: 'param3'})
    dft.index.name = 'col_index'

    # Extract the parameter header names and drop them from the transposed DataFrame
    header_names = dft[['param0', 'param1', 'param2', 'param3']]
    dft.drop(columns=['param0', 'param1', 'param2', 'param3'], inplace=True)

    # Replace commas with dots in numeric-like strings
    dft = dft.apply(lambda x: x.apply(lambda val: re.sub(r'(?<=\d),', '.', val)
                                      if isinstance(val, str) else val))

    # Melt the dataframe to long format, with 'col_index' as identifier
    dfm = pd.melt(dft.reset_index(), id_vars=['col_index'], var_name='row_index', value_name='value')

    # Merge the melted DataFrame with sample names using 'row_index'
    dfm_spl = pd.merge(dfm, sample_name, how='left', left_on='row_index', right_on='row_index')

    # Merge the DataFrame with the parameter headers using 'col_index'
    dfm_spl_head = pd.merge(dfm_spl, header_names, how='left', left_on='col_index', right_on='col_index')

    # Drop unnecessary 'col_index' and 'row_index' columns
    dfm_spl_head.drop(columns=['col_index', 'row_index'], inplace=True)

    # Set sample_index as the index
    dfm_spl_head.set_index('sample_index', inplace=True)

    # set sample_index as the index for sample_name df
    sample_name.set_index('sample_index', inplace=True)
    
    # We retrive isotope names from header_names and place in a new table with a index for later use
    isotope_names = header_names.query('param0 == "Raw.Average"').drop(columns=['param0','param1','param3'])
    # Use str.extract to split the column 'param2' into 'isotopes' and 'method_id'
    isotope_names[['isotopes', 'method_id']] = isotope_names['param2'].str.extract(r'(\S+)\s\((.*)\)')
    # Update data table with isoropes names
    data = pd.merge(dfm_spl_head, isotope_names, how='left', left_on='param2', right_on='param2')
    data['file_name'] = file_name
    data['exp_id'] = exp_id
    # Reorder columns
    data = data[['sample_name', 'param0', 'param1', 'param2', 'param3','method_id', 'file_name', 'exp_id', 'isotopes', 'value']]
    # set index to iotopes
    isotope_names.set_index('isotopes', inplace=True)
    return data, sample_name, isotope_names
