# -*- coding: utf-8 -*-
"""
Created on Sat Jul 27 17:18:21 2024

@author: User
"""
import pandas as pd
from collections import Counter
from functools import reduce
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import time
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from nltk.corpus import stopwords
from PIL import Image

starttime=time.time()
# Download necessary NLTK data
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')

# File path
file_path = r"C:\Users\User\Downloads\archive (1)\song_lyrics.csv"

# Load the dataset
df = pd.read_csv(file_path)

# End the timer and calculate execution time
end_time = time.time()
execution_time = end_time - starttime

# Display the execution time
print(f"\nExecution Time for Rap: {execution_time:.2f} seconds")
#-------------------------------Pie Chart------------------------------
import matplotlib.pyplot as plt
# Ensure the 'tag' column exists
if 'tag' in df.columns:
    # Count the occurrences of each unique tag
    tag_counts = df['tag'].value_counts()

    # Create the pie chart
    plt.figure(figsize=(10, 7))
    plt.pie(tag_counts, labels=tag_counts.index, autopct='%1.1f%%', startangle=140)
    plt.title('Distribution of Tags in Song Lyrics')
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    # Display the pie chart
    plt.show()
else:
    print("The 'tag' column is missing in the dataset.")

#---------------------------Rap-----------------------------------
starttime=time.time()
# File path
file_path = r"C:\Users\User\Downloads\archive (1)\song_lyrics.csv"

# Load the dataset
df = pd.read_csv(file_path)
# List of artists to filter
artists = ['Eminem', 'Jay-Z', 'Kendrick Lamar', 'Nas', '2Pac']

# Normalize function to handle spacing and capitalization differences
def normalize(text):
    return ''.join(text.split()).lower()

# Normalize the artist column in the DataFrame
df['normalized_artist'] = df['artist'].apply(normalize)

# Normalize the target artist names
normalized_artists = [normalize(artist) for artist in artists]

# Filter the DataFrame
filtered_df = df[df['normalized_artist'].isin(normalized_artists)]

# Drop the normalized column as it's no longer needed
filtered_df = filtered_df.drop(columns=['normalized_artist'])

# Create separate DataFrames for each artist
eminem_df = filtered_df[filtered_df['artist'].str.lower().str.replace(' ', '') == 'eminem']
jayz_df = filtered_df[filtered_df['artist'].str.lower().str.replace(' ', '') == 'jay-z']
kendrick_lamar_df = filtered_df[filtered_df['artist'].str.lower().str.replace(' ', '') == 'kendricklamar']
nas_df = filtered_df[filtered_df['artist'].str.lower().str.replace(' ', '') == 'nas']
tupac_shakur_df = filtered_df[filtered_df['artist'].str.lower().str.replace(' ', '') == '2pac']

# End the timer and calculate execution time
end_time = time.time()
execution_time = end_time - starttime

# Display the execution time
print(f"\nExecution Time for Rap: {execution_time:.2f} seconds")

starttime=time.time()
punctuation_pattern = re.compile(r'[^\w\s]')

# Function to clean and normalize lyrics
def clean_lyrics(lyrics):
    # Lowercase the text
    lyrics = lyrics.lower()
    # Remove punctuation
    lyrics = punctuation_pattern.sub('', lyrics)
    # Tokenize the text
    words = word_tokenize(lyrics)
    return words

# Function to perform map-reduce on lyrics
def map_reduce_lyrics(df):
    # Map step: Clean lyrics and count each word
    mapped = df['lyrics'].dropna().apply(lambda x: Counter(clean_lyrics(x))).tolist()
    # Reduce step: Aggregate counts for each word
    reduced = reduce(lambda a, b: a + b, mapped, Counter())
    return reduced

# Perform map-reduce on each artist's lyrics
eminem_word_count = map_reduce_lyrics(eminem_df)
jayz_word_count = map_reduce_lyrics(jayz_df)
kendrick_lamar_word_count = map_reduce_lyrics(kendrick_lamar_df)
nas_word_count = map_reduce_lyrics(nas_df)
tupac_shakur_word_count = map_reduce_lyrics(tupac_shakur_df)
# End the timer and calculate execution time
end_time = time.time()
execution_time = end_time - starttime

# Display the execution time
print(f"\nExecution Time for Rap: {execution_time:.2f} seconds")

# Step 1: Merge the word counts
combined_word_count = eminem_word_count + jayz_word_count + kendrick_lamar_word_count + nas_word_count + tupac_shakur_word_count

# Step 2: Remove stopwords
stop_words = set(stopwords.words('english'))
filtered_word_count = Counter({word: count for word, count in combined_word_count.items() if word.lower() not in stop_words})

# Adding the additional words to the stopwords set
additional_stopwords = {
    "im", "like", "got", "dont", "get", "know", "verse", "aint", "cause",
    "see", "back", "one", "na", "yeah", "thats", "go", "life", "love",
    "time", "never", "cant", "man", "em", "chorus", "make", "say", "ya",
    "yall", "come"
}

# Update stop_words to include the additional stopwords
stop_words.update(additional_stopwords)

filtered_word_count = Counter({word: count for word, count in combined_word_count.items() if word.lower() not in stop_words})

# Step 3: Create and display the WordCloud
wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(filtered_word_count)

plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()

#----------------------------Pop-------------------------------------
starttime=time.time()
# File path
file_path = r"C:\Users\User\Downloads\archive (1)\song_lyrics.csv"

# Load the dataset
df = pd.read_csv(file_path)
# List of artists to filter
artists = ['Beyonc', 'Michael Jackson', 'Taylor Swift', 'Britney Spears', 'Lady Gaga']

# Normalize function to handle spacing and capitalization differences
def normalize(text):
    return ''.join(text.split()).lower()

# Normalize the artist column in the DataFrame
df['normalized_artist'] = df['artist'].apply(normalize)

# Normalize the target artist names
normalized_artists = [normalize(artist) for artist in artists]

# Filter the DataFrame
filtered_df = df[df['normalized_artist'].isin(normalized_artists)]

# Drop the normalized column as it's no longer needed
filtered_df = filtered_df.drop(columns=['normalized_artist'])

# Create separate DataFrames for each artist
beyonce_df = filtered_df[filtered_df['artist'].str.lower().str.replace(' ', '') == 'beyonc']
michael_jackson_df = filtered_df[filtered_df['artist'].str.lower().str.replace(' ', '') == 'michaeljackson']
taylor_swift_df = filtered_df[filtered_df['artist'].str.lower().str.replace(' ', '') == 'taylorswift']
britney_spears_df = filtered_df[filtered_df['artist'].str.lower().str.replace(' ', '') == 'britneyspears']
lady_gaga_df = filtered_df[filtered_df['artist'].str.lower().str.replace(' ', '') == 'ladygaga']

end_time = time.time()
execution_time = end_time - starttime

# Display the execution time
print(f"\nExecution Time for pop: {execution_time:.2f} seconds")

starttime=time.time()
punctuation_pattern = re.compile(r'[^\w\s]')

# Function to clean and normalize lyrics
def clean_lyrics(lyrics):
    # Lowercase the text
    lyrics = lyrics.lower()
    # Remove punctuation
    lyrics = punctuation_pattern.sub('', lyrics)
    # Tokenize the text
    words = word_tokenize(lyrics)
    return words

# Function to perform map-reduce on lyrics
def map_reduce_lyrics(df):
    # Map step: Clean lyrics and count each word
    mapped = df['lyrics'].dropna().apply(lambda x: Counter(clean_lyrics(x))).tolist()
    # Reduce step: Aggregate counts for each word
    reduced = reduce(lambda a, b: a + b, mapped, Counter())
    return reduced

# Perform map-reduce on each artist's lyrics
beyonce_word_count = map_reduce_lyrics(beyonce_df)
michael_jackson_word_count = map_reduce_lyrics(michael_jackson_df)
taylor_swift_word_count = map_reduce_lyrics(taylor_swift_df)
britney_spears_word_count = map_reduce_lyrics(britney_spears_df)
lady_gaga_word_count = map_reduce_lyrics(lady_gaga_df)
# End the timer and calculate execution time
end_time = time.time()
execution_time = end_time - starttime

# Display the execution time
print(f"\nExecution Time for pop: {execution_time:.2f} seconds")

# Step 1: Merge the word counts
combined_word_count = beyonce_word_count + michael_jackson_word_count + taylor_swift_word_count + britney_spears_word_count + lady_gaga_word_count

# Step 2: Remove stopwords
stop_words = set(stopwords.words('english'))
filtered_word_count = Counter({word: count for word, count in combined_word_count.items() if word.lower() not in stop_words})

# Adding the additional words to the stopwords set
additional_stopwords = {
'im', 'dont', 'oh', 'na', 'yeah', 'got', 'youre', 'wan', 'get'
}

# Update stop_words to include the additional stopwords
stop_words.update(additional_stopwords)

filtered_word_count = Counter({word: count for word, count in combined_word_count.items() if word.lower() not in stop_words})

# Step 3: Create and display the WordCloud
wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(filtered_word_count)

plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation="bilinear")
plt.axis("off")
plt.show()
#---------------------------------------------------------------------
import pandas as pd
from collections import Counter
from functools import reduce
import re
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import time


# Download necessary NLTK data
nltk.download('punkt')
nltk.download('wordnet')

# Start the timer
start_time = time.time()

# File path
file_path = r"C:\Users\User\Downloads\archive (1)\song_lyrics.csv"

# Load the dataset
df = pd.read_csv(file_path)

starttime = time.time()

# Ensure the 'year' column exists and is of the correct type
if 'year' in df.columns:
    df['year'] = pd.to_numeric(df['year'], errors='coerce')  # Convert to numeric, forcing errors to NaN

    # Timer for filtering data between 1980 and 1999
    start_80_90_time = time.time()
    year_80_90_df = df[(df['year'] >= 1980) & (df['year'] <= 1999)]
    end_80_90_time = time.time()
    execution_80_90_time = end_80_90_time - start_80_90_time

    # Timer for filtering data between 2000 and 2020
    start_2000_time = time.time()
    year_2000_df = df[(df['year'] >= 2000) & (df['year'] <= 2020)]
    end_2000_time = time.time()
    execution_2000_time = end_2000_time - start_2000_time

    # Ensure the 'tag' column exists and filter based on genres
    if 'tag' in df.columns and 'language' in df.columns:
        # Normalize the 'tag' column to lowercase
        df['tag'] = df['tag'].str.lower()
        df['language'] = df['language'].str.lower()

        # Filter for 'pop' and 'rap' and English language
        genre_filter = df['tag'].isin(['pop', 'rap']) & (df['language'] == 'en')

        # Apply genre and language filter to the year-based DataFrames
        year_80_90_df = year_80_90_df[genre_filter]
        year_2000_df = year_2000_df[genre_filter]

# End the timer and calculate total execution time
end_time = time.time()
total_execution_time = end_time - starttime

# Display the execution times
print(f"\nExecution Time for 1980-1999 filtering: {execution_80_90_time:.2f} seconds")
print(f"Execution Time for 2000-2020 filtering: {execution_2000_time:.2f} seconds")
print(f"Total Execution Time: {total_execution_time:.2f} seconds")

# Initialize stopwords, lemmatizer, and punctuation pattern

punctuation_pattern = re.compile(r'[^\w\s]')

# Function to clean and normalize lyrics
def clean_lyrics(lyrics):
    # Lowercase the text
    lyrics = lyrics.lower()
    # Remove punctuation
    lyrics = punctuation_pattern.sub('', lyrics)
    # Tokenize the text
    words = word_tokenize(lyrics)


# Function to perform map-reduce on lyrics
def map_reduce_lyrics(df):
    # Map step: Clean lyrics and count each word
    mapped = df['lyrics'].dropna().apply(lambda x: Counter(clean_lyrics(x))).tolist()
    # Reduce step: Aggregate counts for each word
    reduced = reduce(lambda a, b: a + b, mapped, Counter())
    return reduced

start_time = time.time()
# Perform map-reduce on the filtered data
compare1_count = map_reduce_lyrics(year_80_90_df)
compare2_count = map_reduce_lyrics(year_2000_df)

# End the timer and calculate execution time
end_time = time.time()
execution_time = end_time - start_time

# Display the execution time
print(f"\nExecution Time: {execution_time:.2f} seconds")
