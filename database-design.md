# Importing Wolf Pack into an SQL database

## One possible design:

* tables for each N value, collecting all the ngrams with that n value
    * columns: 
        * `ID`
        * n-gram string
        * total occurrences?
        * months repeated?
    * possibly create [indexes](https://use-the-index-luke.com/sql/anatomy) for these tables

* tables for each N value *for each publication*, collecting its ngram occurrences
    * columns: 
        * N-gram `ID`
        * columns for each date indexed by that publication






Some notes:

- I was wondering whether it would make sense to have a "month" column, so each row is the count for one month, for one term. But that would multiply the number of rows by as much as 300 (25 years x 12 months), so I'm inclined to think it makes more sense to have them as columns, and just retrieve a range of columns