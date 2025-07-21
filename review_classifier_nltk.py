import pandas as pd
import spacy
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Download Ressourcen für nltk
nltk.download('vader_lexicon')
sia = SentimentIntensityAnalyzer()

# Spacy Modell für Deutsch laden
try:
    nlp = spacy.load("de_core_web_sm")
except:
    from spacy.cli import download
    download("de_core_web_sm")
    nlp = spacy.load("de_core_web_sm")

# Schlüsselwörter pro Kategorie
keywords = {
    "food": ["essen", "food", "gericht", "speise", "menu"],
    "service": ["service", "bedienung", "kellner", "personal"],
    "atmosphere": ["atmosphäre", "ambiente", "musik", "einrichtung"]
}

def classify_sentiment(text):
    sentiment_score = sia.polarity_scores(text)["compound"]
    if sentiment_score >= 0.1:
        return "positive"
    elif sentiment_score <= -0.1:
        return "negative"
    elif -0.1 < sentiment_score < 0.1:
        return "neutral"
    else:
        return "None"

def classify_review(review):
    review_preprocessed = nlp(review.lower())
    result = {"food": "None", "service": "None", "atmosphere": "None"}
    for sent in review_preprocessed.sents:
        sent_text = sent.text
        for category, word_list in keywords.items():
            if any(word in sent_text for word in word_list):
                sentiment = classify_sentiment(sent_text)
                result[category] = sentiment
    return result

# CSV einlesen
df = pd.read_csv("task_1_google_maps_comments.csv")

# Klassifikation anwenden
classified = df['review'].apply(lambda x: pd.Series(classify_review(str(x))))

# Neue Spalten an bestimmten Index-Positionen einfügen
df.insert(3, "food_rating", classified["food"])
df.insert(4, "service_rating", classified["service"])
df.insert(5, "atmosphere_rating", classified["atmosphere"])

# Ergebnis speichern
df.to_csv("classified_reviews.csv", index=False)

print("Klassifikation abgeschlossen. Ergebnis in 'classified_reviews.csv' gespeichert.")
