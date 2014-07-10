This is a trimmed, more readable version of the analysis code used in the article: 

```
@article{josephcheck,
  title={Check-ins in “Blau space”: Applying Blau’s macrosociological theory to foursquare check-ins from New York City},
  author={Joseph, K. and Carley, Kathleen M. and Hong, Jason I.}
   journal={ACM Transactions on Intelligent Systems and Technology (TIST)},
  year={2014}
}
```


Please refer to this article if you use the code.

However, please note that code was adopted from the following sources:

1. MALLET tutorial: http://mallet.cs.umass.edu/import-devel.php

2. Normalized Mutual Information code: https://sites.google.com/site/andrealancichinetti/mutual

3. Haversine distance: https://github.com/johnmetta/adcp/blob/master/src/gislib.py

4. Walker random sampling class: https://gist.github.com/ntamas

If you use [1] or [2], please also cite the related articles (found at the websites listed)

This is in addition to all of the amazing libraries used in Java, Python and R 
and the use of the census API to pull down the demographic data.

The code is unfortunately not explicitly runnable because I can't give away the data
and, possibly, because I used way too many absolute paths and I might have made a mistake cleaning them up . 
 Apologies in advance, but I've done some work to make the code more readable. 
 If you actually want to use pieces, shoot me an email and I can clarify.

The process used by paper was as follows:

1. Pull the data out of the mysql database where it was stored. Check-in data is then in the form
```
2011-08-07 00:27:45  31862406	4b9add62f964a52078dd35e3	Food::Mexican Restaurant	-74.032	40.6218	Trace
```

2. Run "process.R", which did the following:
-generated cleaned check-in files and a file that included demographic information for all of the venues 
-ran the experiments with the different parameters of LDA 
-ran the NMI code to find the agreement between the two data partitions
-generated the figures in the article

3. There is a comment in process.R at the point at which bootstrap.py was run to generate the
bootstrapped CIs

The LDAs are run using a little Java program that interfaces with the MALLET toolkit.  I've provided the jar 
that's used in process.R as well as the source code.

The file lda_experiment.csv holds the results of the experiment run to determine which parameterization
of LDA to use

The file topicWordWeights5_5_1_true_20_0.01_0.6_5.txt holds the output of the LDA used in the 
analysis in the article.
