import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from scipy.stats import norm


def import_data(file_name):
    # Read data from an Excel file
    return pd.read_excel(file_name)


def plot_data(df, plot_type, ci):
    sns.set(style="whitegrid")
    plt.figure(figsize=(5, 3))

    if ci > 0:
        # Calculate mean and standard deviation
        mean = df['unix_duration'].mean()
        std = df['unix_duration'].std()
        n = len(df['unix_duration'])

        # Calculate the Z-score for the given confidence interval
        z_score = norm.ppf(1 - ((1 - (ci / 100)) / 2))
        margin_of_error = z_score * (std / np.sqrt(n))

        lower_bound = mean - margin_of_error
        upper_bound = mean + margin_of_error

        # Filter the data to include only the values within the confidence interval
        df = df[(df['unix_duration'] >= lower_bound) & (df['unix_duration'] <= upper_bound)]

    if plot_type == 'pdf':
        sns.kdeplot(df['unix_duration'], bw_adjust=0.5, fill=True)
        plt.title(f'Probability Density Function of Runtime ({ci}% CI)')
    elif plot_type == 'cdf':
        sns.kdeplot(df['unix_duration'], cumulative=True, bw_adjust=0.5, fill=True)
        plt.title(f'Cumulative Distribution Function of Runtime ({ci}% CI)')
    else:
        print('Invalid plot type. Please choose "pdf" or "cdf".')

    plt.xlabel('Runtime (seconds)')
    plt.ylabel('Density' if plot_type == 'pdf' else 'Cumulative')

    plt.tight_layout()  # Adjust layout to not overlap and fit everything
    plt.savefig(f'unix_duration_{plot_type}_{ci}_percent_CI.pdf')


def main(ci=95):
    file_name = 'workloads/c2_results_parsed.xlsx'  # Update this to the path of your Excel file
    df = import_data(file_name)
    plot_type = 'pdf'  # choose either 'pdf' or 'cdf'
    plot_data(df, plot_type, ci)


if __name__ == "__main__":
    # Example: Set CI to 95% when calling main
    main(ci=100)


