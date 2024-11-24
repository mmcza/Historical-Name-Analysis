import pandas as pd
import numpy as np
import glob
import matplotlib.pyplot as plt
import os

def calculate_top_n_names(df, n):
    # Task 6
    number_of_years = df['year'].nunique()
    names_df = df.groupby(['name', 'sex'])[['frequency_male', 'frequency_female']].sum() / number_of_years

    names_df['frequency_male'] = pd.to_numeric(names_df['frequency_male'])
    names_df['frequency_female'] = pd.to_numeric(names_df['frequency_female'])

    top_male_names = names_df.nlargest(n, 'frequency_male')
    top_female_names = names_df.nlargest(n, 'frequency_female')
    top_names = pd.concat([top_male_names, top_female_names])

    # Task 8
    df = df.set_index(['name', 'sex'])
    df['in_top'] = df.index.isin(top_names.index)
    df.reset_index(inplace=True)
    return top_names, df

def calculate_name_gender_ratio(df, start_year, end_year):
    # Drop rows where year is not in the specified range
    filtered_df = df[df['year'].between(start_year, end_year)]

    grouped_birth_name_df = filtered_df.groupby(['name', 'sex'])[['count', 'in_top']].sum().fillna(0)
    grouped_birth_name_df = grouped_birth_name_df.unstack(level='sex')
    grouped_birth_name_df = grouped_birth_name_df[grouped_birth_name_df[('in_top', 'F')] + grouped_birth_name_df[('in_top', 'M')] > 0]
    grouped_birth_name_df['p_m'] = grouped_birth_name_df[('count', 'M')] / (grouped_birth_name_df[('count', 'M')] + grouped_birth_name_df[('count', 'F')])
    grouped_birth_name_df['p_f'] = 1 - grouped_birth_name_df['p_m']
    grouped_birth_name_df = grouped_birth_name_df.drop(columns=[('count', 'F'), ('count', 'M'), ('in_top', 'F'), ('in_top', 'M')])
    grouped_birth_name_df.to_csv('names_ratio_old.csv')

    return grouped_birth_name_df

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

    fig, axs = plt.subplots(2, 1, figsize=(10, 9))
    axs[0].plot(usa_df.groupby('year')['count'].sum())
    axs[0].set_title('Number of births per year')
    axs[0].set_xlabel('Year')
    axs[0].set_ylabel('Number of births')

    plt.subplots_adjust(hspace=0.5)

    birth_ratio_df = usa_df.groupby(['year', 'sex'])[['count']].sum().unstack()
    birth_ratio_df['ratio'] = birth_ratio_df['count']['F'] / birth_ratio_df['count']['M']
    birth_ratio_df = birth_ratio_df.reset_index()

    min_diff_year = birth_ratio_df.loc[birth_ratio_df['ratio'].idxmin()]['year'].values[0]
    max_diff_year = birth_ratio_df.loc[birth_ratio_df['ratio'].idxmax()]['year'].values[0]

    axs[1].plot(birth_ratio_df['year'], birth_ratio_df['ratio'])
    axs[1].plot(min_diff_year, birth_ratio_df.loc[birth_ratio_df['ratio'].idxmin()]['ratio'],
                'go', markerfacecolor='none',  label='Min ratio')
    axs[1].plot(max_diff_year, birth_ratio_df.loc[birth_ratio_df['ratio'].idxmax()]['ratio'],
                'ro', markerfacecolor='none', label='Max ratio')
    axs[1].legend()
    axs[1].set_title('Ratio of the number of female to male births per year')
    axs[1].set_xlabel('Year')
    axs[1].set_ylabel('Ratio')

    print("-------------------------------------------------")
    print("5. Year with the smallest difference in the ratio of births between female and male:", min_diff_year,
          "and the ratio was:", birth_ratio_df.loc[birth_ratio_df['ratio'].idxmin()]['ratio'].values[0])
    print("Year with the largest difference in the ratio of births between female and male:", max_diff_year,
          "and the ratio was:", birth_ratio_df.loc[birth_ratio_df['ratio'].idxmax()]['ratio'].values[0])

    ### Output:
    ### 5. Year with the smallest difference in the ratio of births between female and male: 1880 and the ratio was: 0.8235496425015838
    ### Year with the largest difference in the ratio of births between female and male: 1901 and the ratio was: 2.2480674763072126

    # 6. Determine the 1000 most popular names for each gender in the entire time range, the method should consist
    #    in determining the 1000 most popular names for each year and for each gender separately. The most popular names
    #    should be those that have been high on the ranking list for the longest time, to avoid the influence
    #    of the number of births in a given year on the result (the number of births is decreasing, an incorrectly performed
    #    procedure may cause names given during the baby boom and used at that time to dominate the ranking)
    #    Please define the Top1000 ranking as a weighted sum of the relative popularity of a given name in a given year

    # The function is implemented in the calculate_top_n_names function (above the main function)

    top_1000_usa_names, usa_df = calculate_top_n_names(usa_df, 1000)

    # 7. Display the changes for the male name John and the first female name in the top-1000 ranking on a single graph
    #    (provide the graph with an appropriate legend):
    #    - on the Y-axis on the left, the number of times the name was given in each year
    #    (display how many times this name was given in 1934, 1980, and 2022)?
    #    - on the Y-axis on the right, the popularity of these names in each of these years

    top_female_name_usa = top_1000_usa_names.nlargest(1, 'frequency_female').reset_index()['name'].values[0]
    plt_title = "Count and popularity of the names John and " + top_female_name_usa

    plt.figure(figsize=(10, 4))
    plt.title(plt_title)
    plt.xlabel('Year')

    # Left y-axis
    plt.plot(usa_df.loc[(usa_df['name'] == 'John') & (usa_df['sex'] == 'M')]['year'],
             usa_df.loc[(usa_df['name'] == 'John') & (usa_df['sex'] == 'M')]['count'], 'b', label='John count')
    plt.plot(usa_df.loc[(usa_df['name'] == top_female_name_usa) & (usa_df['sex'] == 'F')]['year'],
             usa_df.loc[(usa_df['name'] == top_female_name_usa) & (usa_df['sex'] == 'F')]['count'], 'r',
             label=f'{top_female_name_usa} count')
    plt.ylabel('Number of times the name was given in each year')
    plt.legend(loc = 'center left')

    # Right y-axis
    plt.twinx()
    plt.plot(usa_df.loc[(usa_df['name'] == 'John') & (usa_df['sex'] == 'M')]['year'],
             usa_df.loc[(usa_df['name'] == 'John') & (usa_df['sex'] == 'M')]['frequency_male'], 'b--', label='John frequency')
    plt.plot(usa_df.loc[(usa_df['name'] == top_female_name_usa) & (usa_df['sex'] == 'F')]['year'],
             usa_df.loc[(usa_df['name'] == top_female_name_usa) & (usa_df['sex'] == 'F')]['frequency_female'], 'r--',
             label=f'{top_female_name_usa} frequency')
    plt.ylabel('Popularity of the name in each year')
    plt.legend(loc = 'center right')

    # Display the count of the names in 1934, 1980, and 2022
    print("-------------------------------------------------")
    print("7. Count of the name John in 1934, 1980 and 2022 was respectively:",
          usa_df.loc[(usa_df['name'] == 'John') & (usa_df['year'] == 1934) & (usa_df['sex'] == 'M')]['count'].values[0],
          usa_df.loc[(usa_df['name'] == 'John') & (usa_df['year'] == 1980) & (usa_df['sex'] == 'M')]['count'].values[0],
          usa_df.loc[(usa_df['name'] == 'John') & (usa_df['year'] == 2022) & (usa_df['sex'] == 'M')]['count'].values[0])
    print("Count of the name", top_female_name_usa, "in 1934, 1980 and 2022 was respectively:",
          usa_df.loc[(usa_df['name'] == top_female_name_usa) & (usa_df['year'] == 1934) & (usa_df['sex'] == 'F')]['count'].values[0],
          usa_df.loc[(usa_df['name'] == top_female_name_usa) & (usa_df['year'] == 1980) & (usa_df['sex'] == 'F')]['count'].values[0],
          usa_df.loc[(usa_df['name'] == top_female_name_usa) & (usa_df['year'] == 2022) & (usa_df['sex'] == 'F')]['count'].values[0])

    ### Output:
    ### 7. Count of the name John in 1934, 1980 and 2022 was respectively: 46739 35280 7978
    ### Count of the name Mary in 1934, 1980 and 2022 was respectively: 56931 11474 2114

    # 8. Plot a graph divided by year and gender, containing information about the percentage of names in a given year
    #    that belonged to the top1000 ranking (determined for the entire set from Task 6).
    #    This graph should present the change in the diversity of names in subsequent years, divided by gender.
    #    Highlight on the graph and display in the console the year in which the greatest difference in diversity
    #    between male and female names was observed.
    #    Answer the question by displaying the appropriate text in the script:
    #    “What has changed over the last 140 years in terms of name diversity? Does diversity depend on gender?”

    diversity_usa_pt = usa_df.pivot_table(index=['year', 'sex'], columns='in_top', values='count', aggfunc='sum', fill_value=0)
    diversity_usa_pt['top_percentage'] = diversity_usa_pt[True] / (diversity_usa_pt[True] + diversity_usa_pt[False])

    reshaped = diversity_usa_pt['top_percentage'].unstack(level='sex')
    reshaped['difference'] = abs(reshaped['M'] - reshaped['F'])

    max_diff_year = reshaped['difference'].idxmax()
    max_diff_value = reshaped.loc[max_diff_year, 'difference']

    plt.figure(figsize=(10, 4))
    plt.plot(reshaped.index, reshaped['M'], 'b', label='Male names')
    plt.plot(reshaped.index, reshaped['F'], 'r', label='Female names')
    plt.axvline(max_diff_year, color='g', label='Year with the greatest difference in diversity')
    plt.title('Percentage of names in the top1000 ranking')
    plt.xlabel('Year')
    plt.ylabel('Percentage')
    plt.legend()

    print("-------------------------------------------------")
    print("8. Year with the greatest difference in diversity was:", max_diff_year, "and the difference was:", max_diff_value)

    ### Output:
    ### 8. Year with the greatest difference in diversity was: 2008 and the difference was: 0.15926580322775485

    # 9. Verify the hypothesis: is it true that the distribution of the last letters of male names has changed
    #    significantly in the observed period? For this purpose:
    #    - aggregate all births in the full data set by year, gender and last letter,
    #    - extract data for years 1910, 1970, 2023
    #    - normalize the data by the total number of birthdays in a given year
    #    - display letter popularity data for men in the form of a bar chart containing individual years and where the bars
    #      are grouped by letter. View which letter experienced the greatest increase/decrease between 1910 and 2023)
    #    - for the 3 letters for which the greatest change was observed, display the popularity trend over
    #      the entire period of time

    usa_df['last_letter'] = usa_df['name'].str[-1]

    # # After comparing the execution time of the two methods, the first method (with groupby and normalizing by dividing)
    # # was chosen because it is faster
    # # Method 1
    # start_time = pd.Timestamp.now()
    # for i in range(100):
    #     last_letter_df = usa_df.groupby(['year', 'sex', 'last_letter'])['count'].sum().unstack(level='last_letter', fill_value=0)
    #     last_letter_df = last_letter_df.div(last_letter_df.sum(axis=1), axis=0)
    #     end_time = pd.Timestamp.now()
    # print("Method 1 execution time:", end_time - start_time)
    #
    # # Method 2
    # start_time = pd.Timestamp.now()
    # for i in range(100):
    #     last_letter_ct = pd.crosstab([usa_df['year'], usa_df['sex']], usa_df['last_letter'], values=usa_df['count'],
    #                                  aggfunc='sum', normalize='index').fillna(0)
    # end_time = pd.Timestamp.now()
    # print("Method 2 execution time:", end_time - start_time)
    # Method 1 execution time: 0 days 00:00:29.487999
    # Method 2 execution time: 0 days 00:00:33.662000

    last_letter_df = usa_df.groupby(['year', 'sex', 'last_letter'])['count'].sum().unstack(level='last_letter', fill_value=0)
    last_letter_df = last_letter_df.div(last_letter_df.sum(axis=1), axis=0)
    last_letter_selected_years_df = last_letter_df.loc[last_letter_df.index.get_level_values('year').isin([1910, 1970, 2023])]

    last_letter_selected_years_male = last_letter_selected_years_df.xs(key='M', level='sex').T
    last_letter_selected_years_male.plot(kind='bar', figsize=(10, 4), width=0.8)
    plt.title('Popularity of the last letters of male names in 1910, 1970 and 2023')
    plt.xlabel('Last letter')
    plt.ylabel('Popularity')

    last_letter_diff = last_letter_selected_years_male[2023] - last_letter_selected_years_male[1910]
    greatest_increase_letter = last_letter_diff.idxmax()
    greatest_increase_letter_value = last_letter_diff.max()
    greatest_decrease_letter = last_letter_diff.idxmin()
    greatest_decrease_letter_value = last_letter_diff.min()

    print("-------------------------------------------------")
    print("9. Letter with the greatest increase between 1910 and 2023:", greatest_increase_letter,
          "and the increase was:", greatest_increase_letter_value)
    print("Letter with the greatest decrease between 1910 and 2023:", greatest_decrease_letter,
            "and the decrease was:", greatest_decrease_letter_value)

    last_letter_diff_abs = pd.to_numeric(last_letter_diff.abs())
    max_diff_letters = last_letter_diff_abs.nlargest(3).index.to_list()

    last_letter_selected_letters_df = last_letter_df.loc[:, max_diff_letters]

    last_letter_selected_letters_df.xs(key='M', level='sex').plot(kind='line', figsize=(10, 4))
    plt.title('Popularity trend of the 3 last letters of male names with the greatest change between 1910 and 2023')
    plt.xlabel('Year')
    plt.ylabel('Popularity')
    plt.legend()

    ### Output:
    ### 9. Letter with the greatest increase between 1910 and 2023: n and the increase was: 0.14847000759222678
    ### Letter with the greatest decrease between 1910 and 2023: d and the decrease was: -0.09149648988599028

    # 10. Find names in the top1000 ranking that were given to both girls and boys (the ratio of male and female names
    # given). Choose 2 names (one that used to be typically male and is now a female name and the other that used to be
    # typically female and is currently typically male). A typically male name is one for which the quotient of the
    # names given to boys to the total number of names is close to 1 (p_m), similarly the quotient can be defined for
    # girls (p_f). The largest change between year X and year Y can be defined as the average of the sum
    # (p_m(X)+p_f(Y))/2.
    # To analyze the change in name connotations, use two ranges: aggregated data up to 1920 and from 2000.
    # - display these names
    # - plot the trend for these names illustrating the change in the connotation of a given name over the years

    name_ratios_1880_1920 = calculate_name_gender_ratio(usa_df, 1880, 1920)
    name_ratios_2000_2023 = calculate_name_gender_ratio(usa_df, 2000, 2023)

    m2f_change = (name_ratios_1880_1920['p_m'] + name_ratios_2000_2023['p_f']) / 2
    max_m2f_change_name = m2f_change.idxmax()
    max_m2f_change_value = m2f_change.max()

    max_f2m_change_name = m2f_change.idxmin()
    max_f2m_change_value = -(1 - m2f_change.min())
    print(max_m2f_change_name, max_m2f_change_value)
    print(max_f2m_change_name, max_f2m_change_value)

    plt.show()
    

if __name__ == '__main__':
    main()