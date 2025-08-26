import pandas as pd

def extract_adc_headers(hdr_file_path):
    """
    Extracts the ADCFileFormat headers from a .hdr file.
    """
    with open(hdr_file_path, 'r') as file:
        for line in file:
            if line.startswith("ADCFileFormat:"):
                header_line = line.strip().split(":", 1)[1]
                headers = [h.strip() for h in header_line.split(",")]
                return headers
    raise ValueError("ADCFileFormat not found in header file.")

def load_adc_data(adc_file_path, headers):
    """
    Loads the .adc file and applies the given headers to the DataFrame.
    """
    df = pd.read_csv(adc_file_path, header=None)
    df.columns = headers[:df.shape[1]]  # Handle case where fewer headers than columns
    return df

def main(hdr_path, adc_path):
    headers = extract_adc_headers(hdr_path)
    adc_df = load_adc_data(adc_path, headers)
    return adc_df

# Example usage:
if __name__ == "__main__":
    hdr_file = "D20240418T084427_IFCB124.hdr"
    adc_file = "D20240418T084427_IFCB124.adc"
    
    df = main(hdr_file, adc_file)
    print(df.head())  # or df.to_csv("output.csv", index=False) to save
