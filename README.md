# Historical-Name-Analysis

The aim of the project is analyzing the statistics of names in the United States and in Poland. The project was created as a part of the course "Exploratory Data Analysis" at Poznan University of Technology.

## Requirements

To run the project it's necessary to have `Python 3.11` installed with libraries listed in `requirements.txt`. To install them, run the following command:
```bash
pip install -r requirements.txt
```

## Data

The data can be found on the following websites:
- [United States](https://www.ssa.gov/oact/babynames/names.zip)
- [Poland](https://dane.gov.pl/pl/dataset/219,imiona-nadawane-dzieciom-w-polsce)

\*The data for Poland was downloaded from university's website in form of a sqlite database, but values were the same as on the mentioned website.

## Tasks

The tasks that were performed in the project are available [here](https://put-jug.github.io/lab-ead/Lab%2005%20-%20Projekt%20blok1.html).

## Analysis

### United States

#### Number of unique names

#### Count of births by year and ratio of count of born girls to born boys every year

![](/figures/1_Number_of_births_per_year.png)

Year with the smallest difference in the ratio of births between female and male: 1880 and the ratio was: 0.8235496425015838
Year with the largest difference in the ratio of births between female and male: 1901 and the ratio was: 2.2480674763072126

#### Most popular names in the United States

Most popular names given in the USA overall were:
- John for males,
- Mary for females.

![](/figures/2_Count_and_popularity_John_Mary.png)

Count of the name John in 1934, 1980 and 2022 was respectively: 46739 35280 7978
Count of the name Mary in 1934, 1980 and 2022 was respectively: 56931 11474 2114

#### Diversity of names

![](/figures/3_Diversity_USA.png)

The diversity of names for both genders increased between 1880s and 1910s, then decreased between 1920s and 1950s.
Since 1960s the percentage of names in the top1000 ranking has been decreasing, with 2 exceptions between 1978 and 1983,
and between 2010 and 2013.
The rate of change in diversity depends on gender, with a bigger rate observed for female names.
However, the overall shape of the trend is similar for both male and female names - the overall diversity of names
has increased over the years, and when the diversity was decreasing, it was decreasing for both genders.
The greatest difference in diversity was observed in 2008.

The conclusions obtained from the analysis are consistent with the information provided on
https://en.wikipedia.org/wiki/Naming_in_the_United_States#Gender, which confirms their validity and reliability.

#### Distribution of last letters in male names

![](/figures/4_Last_letters_of_male_names.png)
![](/figures/5_Popularity_trend_for_last_names.png)

Based on the graphs from this task, it's possible to confirm that the distribution of the last letters
of male names has changed significantly. In 1910 there were 4 letters ('d', 'e', 'n', 's') with popularity of around
0.1-0.15, and in 2023 the popularity graph was dominated by the letter 'n',
which was the last letter of names given to almost 30% of boys, and second most popular last letter was 'r'.

#### Connotation of names in the US

![](/figures/6_p_m_biggest_change.png)

### Poland

#### Diversity of names in Poland

![](/figures/7_Percentage_of_names_in_ranking_PL.png)
![](/figures/8_Histograms_for_polish_names.png)

#### Connotation of names in Poland