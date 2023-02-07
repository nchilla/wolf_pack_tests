The original (`des-moines-register.json`) file size is `56.4MB`.

After removing the superfluous nesting, the cleaned-up JSON (`des-moines-register-clean.json`) comes out to `29.5MB`, or *52.3% of its original size*.

In `des-moines-register-abbrev.json`, I also tried abbreviating the n-gram keys, i.e. instead of "1 gram", naming the key "1". But surprisingly this only reduced the file size by like 400 bytes? 

The last thing I tried was removing all grams with only 1 occurrence. Google ngram does this as well, much more drastically:

> we only consider ngrams that occur in at least 40 books. Otherwise the dataset would balloon in size

— via [their info page](https://books.google.com/ngrams/info)

You can find this file at `des-moines-register-trim.json`. The file size goes down to `1.4MB`, or *2.4%* of its original size and *4.7%* of the cleaned-up file size.