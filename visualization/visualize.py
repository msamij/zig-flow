import matplotlib.pyplot as plt  # type: ignore
import matplotlib  # type: ignore
import pandas as pd  # type: ignore
import numpy as np  # type: ignore

matplotlib.use('TkAgg')


def plot_combined_dataset_desc_stats() -> None:
    df = pd.read_csv('../output/combined_dataset_desc_stats.csv',
                     header=None, names=['Metric', 'Value'])

    fig, ax = plt.subplots(figsize=(8, 6))
    ax.scatter(df['Value'], df['Metric'], color='skyblue', s=100)

    ax.invert_yaxis()
    ax.set_xscale('log')
    ax.set_xlabel('Value (Log Scale)')
    ax.set_title('Scatter plot of Metrics on log scale')

    plt.show()


if __name__ == '__main__':
    plot_combined_dataset_desc_stats()

# fig, ax = plt.subplots()
# ax.pie(data['Value'], labels=data['Metric'], autopct='%1.1f%%', startangle=140)
# ax.set_title('Proportion of Metrics')

# plt.show()

# data['Value'] = pd.to_numeric(data['Value'], errors='coerce')

# fig, ax = plt.subplots(figsize=(10, 6))

# ax.barh(data['Metric'], data['Value'], color='skyblue', log=True)

# ax.set_xlabel('Value')
# ax.set_ylabel('Metric')
# ax.set_title('Horizontal Bar Plot of Metrics')

# ax.set_xlim(1, data['Value'].max() * 1.1)  # Adjust x-axis range

# for i, v in enumerate(data['Value']):
#     ax.text(v + data['Value'].max() * 0.01, i,
#             f'{v:,.0f}', color='black', va='center')

# plt.tight_layout()
# plt.show()

#     ax.text(v + 0.1, i, str(v), color='black', va='center')
# plt.savefig('text.png')

# y_axis = []
# row_names = []
# row_values = []

# for i, row_name in enumerate(data['Metric']):
#     row_names.insert(i, row_name)

# for i, row_value in enumerate(data['Value']):
#     row_values.insert(i, row_value)

# y_axis = np.arange(row_values['min'], row_values['max'])

# performance = 3 + 10 * np.random.rand(len(row_names))
# error = np.random.rand(len(row_names))

# print(y_axis)
# print(row_names)
# print(row_values)

# values = 3+10*np.random.rand(len(row_names))

# data['Value'] = pd.to_numeric(data['Value'])

# metrics = data['Metric']
# values = data['Value']

# plt.figure(figsize=(8, 5))
# plt.bar(metrics, values, color='skyblue')

# plt.xlabel('Metrics')
# plt.ylabel('Values')
# plt.title('Descriptive Statistics')
# plt.xticks(rotation=45)

# for i, value in enumerate(values):
#     plt.text(i, value + max(values) * 0.02, str(value),
#              ha='center', va='bottom', fontsize=9)

# plt.tight_layout()
# plt.savefig('desc.png')
