import pandas as pd
import numpy as np
import glob
import matplotlib.pyplot as plt
import os

def main():
    # 1. Load the data from all files to a single pandas DataFrame

    usa_files = glob.glob(os.path.join('data', 'names', 'yob*.txt'))
    usa_df = pd.DataFrame(columns=['name', 'sex', 'count', 'year'])

    for file in usa_files:
        year = int(file.split('yob')[1].split('.txt')[0])
        df = pd.read_csv(file, names=['name', 'sex', 'count'])
        df['year'] = year
        usa_df = pd.concat([usa_df, df], ignore_index=True)

    # 2. Determine the number of unique names in the whole dataset

    print("-------------------------------------------------")
    print("2. Number of unique names:", usa_df['name'].nunique())

    ### Output:
    ### Number of unique names: 103564

    # 3. Determine the number of unique names for each sex

    print("-------------------------------------------------")
    print("3. Number of unique names by sex:", usa_df.groupby('sex')['name'].nunique())

    ### Output:
    ### Number of unique names by sex: Sex
    ### F    70903
    ### M    44261

    # 4. Create new columns ("frequency_male" and "frequency female"), and determine the popularity of each name in each
    #    year by dividing number of times the name was given by the total number of births for that gender in that year
    usa_df['total_births_by_sex'] = usa_df.groupby(['year', 'sex'])['count'].transform('sum')
    usa_df['frequency_male'] = np.where(usa_df['sex'] == 'M', usa_df['count'] / usa_df['total_births_by_sex'], 0)
    usa_df['frequency_female'] = np.where(usa_df['sex'] == 'F', usa_df['count'] / usa_df['total_births_by_sex'], 0)

    # 5. Determine and display a plot consisting of two subplot, where the x-axis is the time scale and the y-axis represents:
    #    - the number of births for each year (top subplot)
    #    - the ratio of the number of female to male births in each year (bottom subplot)
    #    Which year had the smallest and largest difference in the number of births between male and female
    #    (the question concerns the subplot showing the ratio of births)? Determine and display the answer on the screen

    fig, axs = plt.subplots(2)
    axs[0].plot(usa_df.groupby('year')['count'].sum())
    axs[0].set_title('Number of births per year')
    axs[0].set_xlabel('Year')
    axs[0].set_ylabel('Number of births')

    usa_df['total_births_by_year'] = usa_df.groupby('year')['count'].transform('sum')
    # use pivot_table to get the ratio

    plt.show()
    

if __name__ == '__main__':
    main()