import os
import json
import time
import pandas as pd
import logging
from openai import OpenAI
from typing import Dict

MODEL_NAME = "gpt-4"
DEFAULT_RESULT = {"food": "None", "service": "None", "atmosphere": "None"}
INPUT_FILE = "task_1_google_maps_comments.csv"

# Logging konfigurieren
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

class ReviewClassifier:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("API key not found. Please set the OPENAI_API_KEY environment variable.")
        self.client = OpenAI(api_key=api_key)

    def classify_reviews(self, text: str) -> Dict[str, str]:
        response = self.client.responses.create(model=MODEL_NAME, input=self.build_prompt(text))
        time.sleep(1)
        return self.parse_response(response)

    def build_prompt(self, text: str) -> str:
        return f"""
        Du bist ein Analyse-Tool für Google-Bewertungen. 
        Analysiere den folgenden Kommentar und gib für jede Kategorie (Essen, Service, Atmosphäre) an, ob der Ton positiv, neutral, negativ. Wird die Kategorie nicht erwähnt oder sie ist nicht klassifizierbar soll der Ton None sein.

        Kommentar: "{text}"

        Antwortformat (JSON):
        {{
        "food": "...",
        "service": "...",
        "atmosphere": "..."
        }}
        """

    def parse_response(self, response) -> Dict[str, str]:
        output_text = response.output_text
        if output_text:
            return json.loads(output_text)
        else:
            logger.warning("Keine Ausgabe erhalten.")
            return DEFAULT_RESULT

def main():
    # API-Schlüssel laden
    api_key = os.getenv("OPENAI_API_KEY")
    analyzer = ReviewClassifier(api_key)

    # CSV einlesen
    google_maps_comments = pd.read_csv("test.csv")

    # Reviews analysieren
    classified_reviews = google_maps_comments["review"].apply(lambda review_text: pd.Series(analyzer.classify_reviews(str(review_text))))

    # Rating-Spalten einfügen
    google_maps_comments.insert(3, "food_rating", classified_reviews["food"])
    google_maps_comments.insert(4, "service_rating", classified_reviews["service"])
    google_maps_comments.insert(5, "atmosphere_rating", classified_reviews["atmosphere"])

    # Ergebnis speichern
    with open(INPUT_FILE, "w", encoding="utf-8", newline="") as f:
        google_maps_comments.to_csv(f, index=False)

    logger.info("Klassifizierung abgeschlossen.")

if __name__ == "__main__":
    main()
