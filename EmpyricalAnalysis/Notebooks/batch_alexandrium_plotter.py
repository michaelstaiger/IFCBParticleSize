import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def extract_adc_headers(hdr_file_path):
    with open(hdr_file_path, 'r') as file:
        for line in file:
            if line.startswith('ADCFileFormat:'):
                header_line = line.strip().split(':', 1)[1]
                headers = [h.strip() for h in header_line.split(',')]
                return headers
    raise ValueError('ADCFileFormat not found in header file.')

def load_adc_data(adc_file_path, hdr_file_path):
    headers = extract_adc_headers(hdr_file_path)
    df = pd.read_csv(adc_file_path, header=None)
    df.columns = headers[:df.shape[1]]
    return df

def load_class_data(class_file_path):
    return pd.read_csv(class_file_path)

def process_pair(adc_df, class_df):
    # Make sure RoiNumber is an int extracted from PID
    class_df['RoiNumber'] = class_df['pid'].str.split('_').str[-1].astype(int)

    # Merge with ADC data on RoiNumber
    adc_df['RoiNumber'] = range(1, len(adc_df) + 1)
    merged_df = pd.merge(class_df, adc_df[['RoiNumber', 'RunTime', 'InhibitTime']], on='RoiNumber', how='left')

    # Compute VolumeAnalyzed
    merged_df['VolumeAnalyzed'] = (merged_df['RunTime'] - merged_df['InhibitTime']) / 240

    # Detect Alexandrium columns and compute flags
    alex_cols = [col for col in merged_df.columns if 'Alexandrium_catenella' in col]
    merged_df['isAlexandrium'] = (merged_df[alex_cols] > 0.95).any(axis=1).astype(int)
    merged_df['TotalAlexandrium'] = merged_df['isAlexandrium'].cumsum()

    return merged_df

def generate_plot(df, outpath):
    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(df['RunTime'], df['TotalAlexandrium'], color='green', label='Total Alexandrium')
    ax1.set_xlabel('Run Time (minutes)')
    ax1.set_ylabel('Cumulative Alexandrium Count', color='green')
    ax1.tick_params(axis='y', labelcolor='green')

    ax2 = ax1.twinx()
    ax2.plot(df['RunTime'], df['VolumeAnalyzed'], color='blue', linestyle='--', label='Volume Analyzed')
    ax2.set_ylabel('Volume Analyzed (mL)', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')

    fig.tight_layout()
    plt.title('Alexandrium Concentration vs Volume Analyzed')
    plt.savefig(outpath)
    plt.close()

def main_loop(data_dir):
    data_path = Path(data_dir)
    for hdr_file in data_path.rglob('*.hdr'):
        adc_file = hdr_file.with_suffix('.adc')
        class_file = hdr_file.with_name(hdr_file.stem + '_class_vNone.csv')

        if adc_file.exists() and class_file.exists():
            print(f'Processing: {hdr_file.stem}')
            adc_df = load_adc_data(adc_file, hdr_file)
            class_df = load_class_data(class_file)
            merged_df = process_pair(adc_df, class_df)
            plot_file = data_path / f'{hdr_file.stem}_alexandrium_plot.png'
            generate_plot(merged_df, plot_file)
        else:
            print(f'Skipping incomplete set for: {hdr_file.stem}')

# Example usage
if __name__ == '__main__':
    main_loop('./your_data_directory')  # <- Replace with actual path