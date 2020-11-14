# Analyze how much and how a *word* or a *regular expression* is mentioned in Wikipedia pages across languages

The proposed scripts allow you to study the mentions of a certain *word* across languages using *Wikipedia* articles. 

## Get data to analyze

### Download sets of articles <a name ="data"></a>

The data in usage are available on [Wikimedia Downloads](https://dumps.wikimedia.org/backup-index.html) page. Once you are on the Web Site you can choose the language and the date (data referring to *Wikipedia* data until that date)you are interested in to carry out the analysis (i.e. suppose you want to have all the Italian articles you follow the link [`itwiki`](https://dumps.wikimedia.org/ltwiki/20161201/) - referring to a certain date-  and then you proceed downloading the dump `itwiki-DATE-pages-articles-multistream.xml.bz2 `, where `DATE` will be the date that corresponds to your interest. The dump contains articles, templates, media/file descriptions, and primary meta-pages). This data will be the *corpus* for your analysis.

### Additional data

For an analysis that wants to take into account other factors, like the page views of the articles, through this [link](https://dumps.wikimedia.org/other/pagecounts-ez/merged/) it is possible to get the pageviews for the whole *Wikipedia* corpus for each month since 2011. The documentation related to this data is provided [here](https://dumps.wikimedia.org/other/pagecounts-raw/).

## Script descriptions

1. __`main.ipynb`__: 
	> The main script provides the code to parse the [downloaded data](#data). A detailed documentation is furnished for each function. It gives as output:
	- The `.json` files contained in `Corpus` directory - related to the [example](#example). 

2. __`helpers_parser.py`__: 
	> It gathers some support functions for the parsing of the `XML` files.

3. __`pageviews.py`__: 
	> Defines functions used to carry out analysis related to the page views of the articles of interest.
	
4. __`across_languages.py`__: 

> Contains functions to make comparisons across languages.

5. __`plots.py`__:

> Gathers functions to draw plots.


## `IPython Notebook` example - Matteo Renzi mentions across Italian and Portuguese Wikipedia <a name ="example"></a>

__*Remark:*__ Since interactive plots are present open [this](http://nbviewer.jupyter.org/github/CriMenghini/Wikipedia/blob/master/Mention/Mention_draft.ipynb) link to read the `Notebook` correctly.
The goal of the `Notebook` is to provide an example that shows how to use the implemented code and to carry out a small analysis having as the object of interest 'Matteo Renzi'. We proceed with the following steps:

1. Find all articles in Italian and Portuguese that mention Matteo Renzi.

2. Rank them by how frequently they were viewed in November.

Then to play a bit with data:

* Explore the differences between IT and PT in terms of numbers and plots. Are there distinct differences between the languages in terms of what kinds of articles mention Renzi? What's the distribution of number of Renzi mentions per article in IT vs. PT? 