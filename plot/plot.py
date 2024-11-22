import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path.cwd().parent
OUTPUT_FOLDER = PROJECT_ROOT.joinpath('output')


rating_stats = OUTPUT_FOLDER.joinpath('combinedDatasetRatingStats').joinpath(
    'combined_dataset_rating_stats.csv')

year_of_release_distribution = OUTPUT_FOLDER.joinpath(
    'yearOfReleaseDistribution').joinpath(
    'year_of_release_distribution.csv')

rating_distribution = OUTPUT_FOLDER.joinpath(
    'combinedDatasetRatingDistribution').joinpath(
    'combined_dataset_rating_distribution.csv')

matplotlib.use('TkAgg')


def plot_rating_desc_stats() -> None:
    """ Scatter plot of describing ratings summary such as mean, stdDev, min max.
    """

    data = pd.read_csv(rating_stats, header=None, names=['Summary', 'Rating'])

    _, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(data['Summary'], data['Rating'], color='blue', s=80)
    ax.set_xlabel('(Summaries)')
    ax.set_ylabel('(Rating values)')
    ax.set_title('Scatter plot of Rating for various summaries')
    plt.show()


def plot_year_of_release_distribution() -> None:
    """Line plot for year of release distribution, It shows how many movies are release for a given year.
    """

    data = pd.read_csv(year_of_release_distribution,
                       header=None, names=['YearOfRelease', 'Count'])

    plt.figure(figsize=(12, 6))
    plt.plot(data['YearOfRelease'], data['Count'])
    plt.xlabel('Year of Release')
    plt.ylabel('Number of Releases on that year')
    plt.title('Number of Releases per Year')
    plt.grid(True)
    plt.xticks(rotation=45, fontsize=6)
    plt.xticks(data['YearOfRelease'][10::3])
    plt.yticks(fontsize=6)
    plt.yticks(data['Count'][10::3])
    plt.show()


def plot_rating_distribution() -> None:
    """Box plot for ratings distribution.
    """

    data = pd.read_csv(rating_distribution,
                       header=None, names=['Rating', 'Count'])
    _, ax = plt.subplots()
    bar_colors = ['tab:red', 'tab:blue', 'tab:green', 'tab:orange']
    ax.bar(data['Rating'], data['Count'], width=0.2, color=bar_colors)
    ax.set_ylabel('Ratings count')
    ax.set_title('Ratings distribution')
    plt.show()


if __name__ == '__main__':
    plot_rating_desc_stats()
    plot_rating_distribution()
    plot_year_of_release_distribution()
