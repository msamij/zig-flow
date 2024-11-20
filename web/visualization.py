import streamlit as st
import pandas as pd

output_path = '../output/'

year_of_release_distribution_file_path = output_path + \
    'yearOfReleaseDistribution/year_of_release_distribution.csv'
combined_dataset_rating_stats = output_path + \
    'combinedDatasetRatingStats/combined_dataset_rating_stats.csv'
combined_dataset_rating_distribution = output_path + \
    'combinedDatasetRatingDistribution/combined_dataset_rating_distribution.csv'


def plot_combined_dataset_desc_stats() -> None:
    df = pd.read_csv(combined_dataset_rating_stats,
                     header=None, names=['Summary', 'Rating'])
    st.line_chart(data=df, x='Rating', y='Summary')


def plot_year_of_release_distribution() -> None:
    df = pd.read_csv(year_of_release_distribution_file_path,
                     header=None, names=['YearOfRelease', 'Count'])
    st.scatter_chart(data=df, x='YearOfRelease', y='Count', size=120)


def plot_combined_dataset_rating_distribution() -> None:
    df = pd.read_csv(combined_dataset_rating_distribution,
                     header=None, names=['Rating', 'Count'])
    st.bar_chart(data=df, x='Rating', y='Count', color=['#f6ae2d'])


if __name__ == "__main__":
    plot_year_of_release_distribution()
    plot_combined_dataset_desc_stats()
    plot_combined_dataset_rating_distribution()
