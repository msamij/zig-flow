import matplotlib.pyplot as plt  # type: ignore
import matplotlib  # type: ignore
import pandas as pd  # type: ignore
import numpy as np  # type: ignore

matplotlib.use('TkAgg')


def plot_combined_dataset_desc_stats() -> None:
    df = pd.read_csv('../output/combined_dataset_desc_stats.csv',
                     header=None, names=['Metric', 'Value'])

    _, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(df['Value'], df['Metric'], color='skyblue', s=100)

    ax.invert_yaxis()
    ax.set_xscale('log')
    ax.set_xlabel('Value (Log Scale)')
    ax.set_title('Scatter plot of Metrics on log scale')

    plt.show()


if __name__ == '__main__':
    plot_combined_dataset_desc_stats()
