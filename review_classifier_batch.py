import os
import json
import time
import pandas as pd
import logging
from openai import OpenAI
from typing import Dict, List

MODEL_NAME = "gpt-4-turbo"
DEFAULT_RESULT = {"food": "None", "service": "None", "atmosphere": "None"}
CSV_INPUT = "test_reviews.csv"
JSONL_OUTPUT = "batch_input.jsonl"
CSV_OUTPUT = "classified_reviews.csv"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class BatchJsonBuilder:
    def build_prompt(self, text: str) -> str:
        return (
            
        "Analysiere den Ton des folgenden Kommentars zu Essen, Service und Atmosphäre. "
        "Gib für jede Kategorie an: positiv, neutral, negativ oder None (wenn nicht erwähnt).\n\n"
            f"Kommentar: \"{text}\"\n\n"
            "Antwortformat (JSON), kein Markdown-Syntax:\n"
            "{\n"
            "  \"food\": \"...\",\n"
            "  \"service\": \"...\",\n"
            "  \"atmosphere\": \"...\"\n"
            "}"
        )
    
    def truncate_review(self, text: str, max_words: int = 200) -> str:
        words = text.split()
        return " ".join(words[:max_words])
    
    def generate_batch_jsonl(self, csv_path: str, jsonl_path: str) -> None:
        df = pd.read_csv(csv_path)
        with open(jsonl_path, "w", encoding="utf-8") as file:
            for index, row in df.iterrows():
                review = self.truncate_review(str(row["review"]))
                prompt = self.build_prompt(review)
                entry = {
                    "custom_id": str(index),
                    "method": "POST",
                    "url": "/v1/chat/completions",
                    "body": {
                        "model": MODEL_NAME,
                        "messages": [
                            {"role": "system", "content": "Du bist ein hilfsbereiter Assistent."},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0
                    }
                }
                file.write(json.dumps(entry, ensure_ascii=False) + "\n")
        logger.info("Batch-Input-Datei erfolgreich erstellt.")

class OpenAIBatchRunner:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("API key not found. Please set the OPENAI_API_KEY environment variable.")
        self.client = OpenAI(api_key=api_key)

    def run_batch_job(self, jsonl_path: str) -> List[str]:
        uploaded_file = self.upload_batch_file(jsonl_path)
        batch_job = self._tart_batch(uploaded_file.id)
        self.wait_for_completion(batch_job.id)
        return self.download_results(batch_job.id)

    def upload_batch_file(self, jsonl_path: str):
        with open(jsonl_path, "rb") as file:
            uploaded_file = self.client.files.create(file=file, purpose="batch")
        logger.info(f"Datei hochgeladen: {uploaded_file.id}")
        return uploaded_file

    def start_batch(self, file_id: str):
        batch_job = self.client.batches.create(
            input_file_id=file_id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        logger.info(f"Batch gestartet: {batch_job.id}")
        return batch_job

    def wait_for_completion(self, batch_id: str):
        while True:
            batch_job = self.client.batches.retrieve(batch_id)
            logger.info(f"Aktueller Status: {batch_job.status}")
            logger.info(f"Aktueller Status: {batch_job.errors}")
            if batch_job.status == "completed":
                logger.info(f"Batch {batch_id} abgeschlossen.")
                break
            time.sleep(10)

    def download_results(self, batch_id: str) -> List[str]:
        batch_job = self.client.batches.retrieve(batch_id)
        result_file = self.client.files.content(batch_job.output_file_id).content
        return result_file.decode("utf-8").strip().split("\n")

    def parse_results(self, result_entries: List[str]) -> pd.DataFrame:
        results = []
        for entry in result_entries:
            try:
                item = json.loads(entry)
                content = item["response"]["body"]["choices"][0]["message"]["content"]
                results.append(json.loads(content))
            except Exception as e:
                logger.warning(f"Fehler beim Parsen eines Eintrags: {e}")
                results.append(DEFAULT_RESULT)
        return pd.DataFrame(results)

def main():
    api_key = os.getenv("OPENAI_API_KEY")
    builder = BatchJsonBuilder()
    runner = OpenAIBatchRunner(api_key)

    builder.generate_batch_jsonl(CSV_INPUT, JSONL_OUTPUT)
    result_entries = runner.run_batch_job(JSONL_OUTPUT)
    result_df = runner.parse_results(result_entries)

    input_df = pd.read_csv(CSV_INPUT)
    input_df.insert(3, "food_rating", result_df["food"])
    input_df.insert(4, "service_rating", result_df["service"])
    input_df.insert(5, "atmosphere_rating", result_df["atmosphere"])

    input_df.to_csv(CSV_OUTPUT, index=False)
    logger.info("Bewertungen gespeichert in classified_reviews.csv.")

if __name__ == "__main__":
    main()
