# Movies SparkSQL

this application is to use SparkSQL to do some queries on a movie data set.
## Table of contents
* [Requirements](#Requirements)
  * [Technologies](#Technologies)
  * [Data Set](#Data-Set)
  * [Installation](#Installation)
* [Queries](#Queries)
  * [Top shows](#Top-shows)
  * [Average rating](#Average-rating)
  * [Latest shows](#Latest-shows)
  * [Number of shows yearly](#Number-of-shows-yearly)
  * [Filtering shows](#Filtering-shows)
## Requirements

### Technologies
* Spark version 3.3.0
* Scala version 2.12.11
* Jdk 8
* Intelij community edition 2022.2

### Data Set
Movies Data Set from IMDB Downloaded from kaggle [here](https://www.kaggle.com/datasets/anasmahmood000/imdb-movies-dataset?resource=download)

The movie data set contains:
* Movies names
* Their rating
* tv rating
* genre
* year
### Installation
* download and setup java JDK 8
* download and setup Spark environment
* download and setup intelij community edition 2022.2
## Queries
* #### Top shows

   the first query in the application gets the top shows sorted by rating.
* #### Average rating

   the second query gets the average rating and count for each tv rating group ie. Age 
   restriction.
![alt text][showsByAge]
   from the graph we notice that a lot of shows didn't have tv rating in our data set and 
   the highest number of shows made are the rated shows but the not rated, PG-13, PG-14 
   and PG together will make around 3x the rated shows, also from the graph there is few 
   kids shows made
* #### Latest shows

   the third query gets latest shows that within each year the shows are sorted by 
   rating.
* #### Number of shows yearly

   the fourth query gets the number of shows per year sorted by year from oldest to 
   newest.
![alt text][yearsGraph]
   from the graph we can see that every year the number of shows being made are 
   increasing almost in exponential manner, so we can expect that by 2022 there would be 
   around 2000 new shows made this year, also year 2022 is not in the graph because the 
   year has not ended yet.
* #### Filtering shows

   the last query gets the shows by specific genre sorted by rating from best to worst

[yearsGraph]: https://github.com/AhmedAssem1/Movie-SQL/blob/main/images/ShowsByAge.PNG?raw=true "Number of shows by year"
[showsByAge]: https://github.com/AhmedAssem1/Movie-SQL/blob/main/images/ShowsByAge.PNG?raw=true "Number of shows by age"
