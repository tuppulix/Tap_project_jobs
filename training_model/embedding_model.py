import gensim.downloader
from nltk.tokenize import word_tokenize
import nltk
import pandas as pd

# Download necessary NLTK data
nltk.download('punkt')
nltk.download('stopwords')

# Set the number of components for GloVe vectors
n_comps = 100
# Get the list of English stopwords
stop_words = nltk.corpus.stopwords.words('english')
# Load GloVe vectors from gensim
glove_vectors = gensim.downloader.load(f'glove-wiki-gigaword-{n_comps}')

# Function to compute embeddings for benefits
def embedding_benefits(benefits):
    benefit_embeddings = []
    for benefit in benefits:
        # Tokenize the benefit text
        benefit = word_tokenize(benefit)
        # Remove stopwords
        benefit = [word for word in benefit if word not in stop_words]
        words = []
        for word in benefit:
            try:
                # Get the GloVe vector for the word
                words.append(glove_vectors[word])
            except:
                pass
        if len(words) > 0:
            # Compute the mean of the vectors
            mean_benefit = sum(words) / len(words)
        else:
            # If no words found, use a zero vector
            mean_benefit = [0]*n_comps
        benefit_embeddings.append(mean_benefit)
    if len(benefit_embeddings) > 0:
        # Compute the mean of the benefit embeddings
        return sum(benefit_embeddings) / len(benefit_embeddings)
    else:
        return [0]*n_comps    

# Function to compute embeddings for general text fields
def desc_embeddings(text):
    # Tokenize the text
    text = word_tokenize(text)
    # Remove stopwords
    text = [word for word in text if word not in stop_words]
    embeddings = []
    for word in text:
        try:
            # Get the GloVe vector for the word
            embeddings.append(glove_vectors[word])
        except:
            pass
    if len(embeddings) > 0:
        # Compute the mean of the vectors
        mean_embedding = sum(embeddings) / len(embeddings)
    else:
        # If no words found, use a zero vector
        mean_embedding = [0]*n_comps
    return mean_embedding

# Load the CSV file into a pandas DataFrame
ds = pd.read_csv("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions2_categorized.csv")

# Apply embedding functions to relevant columns
ds["Job Description"] = ds["Job Description"].apply(desc_embeddings)
ds["Job Title"] = ds["Job Title"].apply(desc_embeddings)
ds["location"] = ds["location"].apply(desc_embeddings)
ds["Role"] = ds["Role"].apply(desc_embeddings)
ds["skills"] = ds["skills"].apply(desc_embeddings)
ds["Responsibilities"] = ds["Responsibilities"].apply(desc_embeddings)
ds["Benefits"] = ds["Benefits"].apply(embedding_benefits)

# Print the first few rows of the DataFrame
print(ds.head())

# Save the DataFrame to a Parquet file
ds.to_parquet("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions2_embedded.parquet", index=False)
