import gensim.downloader
from nltk.tokenize import word_tokenize
import nltk
import pandas as pd

nltk.download('punkt')
nltk.download('stopwords')

n_comps = 100
stop_words = nltk.corpus.stopwords.words('english')
glove_vectors = gensim.downloader.load(f'glove-wiki-gigaword-{n_comps}')

def embedding_benefits(benefits):
    benefit_embeddings = []
    for benefit in benefits:
        benefit = word_tokenize(benefit)
        benefit = [word for word in benefit if word not in stop_words]
        words = []
        for word in benefit:
            try:
                words.append(glove_vectors[word])
            except:
                pass
        if len(words) > 0:
            mean_benefit = sum(words) / len(words)
        else:
            mean_benefit = [0]*n_comps
        benefit_embeddings.append(mean_benefit)
    if len(benefit_embeddings) > 0:
        return sum(benefit_embeddings) / len(benefit_embeddings)
    else:
        return [0]*n_comps    

def desc_embeddings(text):
    text = word_tokenize(text)
    text = [word for word in text if word not in stop_words]
    embeddings = []
    for word in text:
        try:
            embeddings.append(glove_vectors[word])
        except:
            pass
    if len(embeddings) > 0:
        mean_embedding = sum(embeddings) / len(embeddings)
    else:
        mean_embedding = [0]*n_comps
    return mean_embedding

ds = pd.read_csv("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions2_categorized.csv")
ds["Job Description"] = ds["Job Description"].apply(desc_embeddings)
ds["Job Title"] = ds["Job Title"].apply(desc_embeddings)
ds["location"] = ds["location"].apply(desc_embeddings)
ds["Role"] = ds["Role"].apply(desc_embeddings)
ds["skills"] = ds["skills"].apply(desc_embeddings)
ds["Responsibilities"] = ds["Responsibilities"].apply(desc_embeddings)
ds["Benefits"] = ds["Benefits"].apply(embedding_benefits)

print(ds.head())


ds.to_parquet("/home/melo/Desktop/TAP/Tap_project_jobs/archive/archive/job_descriptions2_embedded.parquet", index=False)

