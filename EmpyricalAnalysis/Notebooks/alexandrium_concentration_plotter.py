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
    merged_df['AlexandriumConcentration'] = merged_df['isAlexandrium'].cumsum()

    return merged_df

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(df['RunTime'], df['AlexandriumConcentration'], color='green', label='Total Alexandrium')
    ax1.set_xlabel('Run Time (minutes)')
    ax1.set_ylabel('Alexandrium Concentration (count/mL)', color='green')
    ax1.tick_params(axis='y', labelcolor='green')

    ax2 = ax1.twinx()
    ax2.plot(df['RunTime'], df['VolumeAnalyzed'], color='blue', linestyle='--', label='Volume Analyzed')
    ax2.set_ylabel('Volume Analyzed (mL)', color='blue')
    ax2.tick_params(axis='y', labelcolor='blue')

    fig.tight_layout()
    plt.title('Alexandrium Concentration vs Volume Analyzed')
    plt.savefig(outpath)
    plt.close()

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

# # Example usage
# if __name__ == '__main__':
#     main_loop('./your_data_directory')  # <- Replace with actual path

def main_loop(data_dir):
    data_path = Path(data_dir)
    all_data = []

    for hdr_file in data_path.rglob('*.hdr'):
        adc_file = hdr_file.with_suffix('.adc')
        class_file = hdr_file.with_name(hdr_file.stem + '_class_vNone.csv')

        if adc_file.exists() and class_file.exists():
            print(f'Processing: {hdr_file.stem}')
            adc_df = load_adc_data(adc_file, hdr_file)
            class_df = load_class_data(class_file)
            merged_df = process_pair(adc_df, class_df)
            merged_df = process_pair(adc_df, class_df)
            merged_df['Source'] = hdr_file.stem
            # Avoid divide-by-zero errors
            merged_df['AlexandriumConcentration'] = merged_df['AlexandriumConcentration'] / merged_df['VolumeAnalyzed'].replace(0, pd.NA)
            all_data.append(merged_df)
        else:
            print(f'Skipping incomplete set for: {hdr_file.stem}')

    # Combine all processed data into one DataFrame
    if not all_data:
        print('No valid data files found.')
        return

    combined_df = pd.concat(all_data, ignore_index=True)

    # Create the plot with dual y-axis and color-coded Source
    fig, ax1 = plt.subplots(figsize=(12, 7))

    colors = plt.cm.get_cmap('tab10', len(combined_df['Source'].unique()))
    source_to_color = {src: colors(i) for i, src in enumerate(combined_df['Source'].unique())}

    for src, group in combined_df.groupby('Source'):
        ax1.plot(group['RunTime'], group['AlexandriumConcentration'], label=f'{src} (Alex)', color=source_to_color[src], linestyle='-')

    ax1.set_xlabel('Run Time (minutes)')
    ax1.set_ylabel('Alexandrium Concentration (count/mL)')
    ax1.tick_params(axis='y')

    ax2 = ax1.twinx()
    for src, group in combined_df.groupby('Source'):
        ax2.plot(group['RunTime'], group['VolumeAnalyzed'], label=f'{src} (Vol)', color=source_to_color[src], linestyle='--')

    ax2.set_ylabel('Volume Analyzed (mL)')
    ax2.tick_params(axis='y')

    fig.tight_layout()
    plt.title('Cumulative Alexandrium vs Volume Analyzed')
    fig.legend(loc='upper left', bbox_to_anchor=(0.1, 0.95), fontsize='small')
    plt.grid(True)
    plt.savefig(data_path / 'combined_alexandrium_plot.png')
    plt.close()

# Example usage
if __name__ == '__main__':
    main_loop('AlexandriumTest')  # <- Replace with actual path