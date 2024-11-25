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

Number of unique names: 103564.
Number of unique names by gender:
 - F:    70903
 - M:    44261

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
The greatest difference in diversity was observed in 2008 and the difference was 0.16952209755307035.

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

Name with the largest change from being a female name to a male name is: Donnie and the change of p_m is: 0.8817976850735472.
Name with the largest change from being a male name to a female name is: Ashley and the change of p_m is: -0.9933546933773039.

### Poland

#### Diversity of names in Poland

![](/figures/7_Percentage_of_names_in_ranking_PL.png)

Overall, name diversity has been increasing over the years, with a slight decline observed between 2015 and 2018.
The greatest difference in diversity between genders occurred in the year 2000, after which the gap gradually
narrowed (with a slight uptick in 2023). The overall percentage of female names in the top 200 rankings
was slightly lower than that of male names, but the difference was not significant.

![](/figures/8_Histograms_for_polish_names.png)

The observed trend can be attributed to the fact that prior to 2013, a minimum of 5 children had to be given
a certain name for it to be recorded, whereas since 2013, the threshold was reduced to 2.
What's interesting, is that for the American dataset, the threshold was 5, as according to [SSA's policy](https://www.ssa.gov/OACT/babynames/background.html), it's not not safe for privacy reasons to release names
that were given to less than 5 kids.

#### Connotation of names in Poland

Names that were relatively often given to both boys and girls in Poland: ANDREA and YUVAL

## Optimization

The code was optimized by using `dask` library to parallelize the computations. This allowed to speed up the computations by around 3 times.

Additionally, 2 methods for normalization were implemented:
- using `crosstab` method from `pandas` library,
- using `groupby` method and normalizing by calling `df.div(df.sum(axis=1), axis=0)` from `pandas` library.
The results was a 12% speedup when using the second method.