# HadoopRecommendationScripts

sample practice (?) recommendation scripts.. to be rewritten in PostgreSQL

Works on Hadoop ver 2.7.1

You can use jobone.jar, jobtwo.jar in sequence with correct input/output paths to yield a item-similarity file.

jobthree.jar requires a rating file for a single user and will retrieve a predicted ratings for items he hasn't rated.

Algorithm from this book: http://www.amazon.com/Programming-Collective-Intelligence-Building-Applications/dp/0596529325

**The initial file should be in the form

user :: item :: rating
