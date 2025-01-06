from pathlib import Path
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path.cwd()
OUTPUT_FOLDER = PROJECT_ROOT.joinpath('output')

rating_stats = OUTPUT_FOLDER.joinpath('combinedDatasetRatingStats').joinpath(
    'combined_dataset_rating_stats.csv')

year_of_release_distribution = OUTPUT_FOLDER.joinpath(
    'yearOfReleaseDistribution').joinpath(
    'year_of_release_distribution.csv')

rating_distribution = OUTPUT_FOLDER.joinpath(
    'combinedDatasetRatingDistribution').joinpath(
    'combined_dataset_rating_distribution.csv')


def plot_rating_desc_stats() -> None:
    df = pd.read_csv(rating_stats, header=None, names=['Summary', 'Rating'])
    st.scatter_chart(data=df, x='Rating', y='Summary',
                     color=['#c1121f'], size=150)


def plot_year_of_release_distribution() -> None:
    df = pd.read_csv(year_of_release_distribution,
                     header=None, names=['YearOfRelease', 'Count'])
    st.line_chart(data=df, x='YearOfRelease', y='Count')


def plot_rating_distribution() -> None:
    df = pd.read_csv(rating_distribution,
                     header=None, names=['Rating', 'Count'])
    st.line_chart(data=df, x='Rating', y='Count', color=['#f2e8cf'])


if __name__ == "__main__":
    plot_rating_desc_stats()
    plot_rating_distribution()
    plot_year_of_release_distribution()
