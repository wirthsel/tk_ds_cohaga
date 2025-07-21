import os
import json
import time
import pandas as pd
import logging
from openai import OpenAI
from typing import Dict, List
from pathlib import Path

MODEL_NAME = "gpt-4-turbo"
DEFAULT_RESULT = {"food": "None", "service": "None", "atmosphere": "None"}
INPUT_FILE = "task_1_google_maps_comments.csv"
OUTPUT_FILE = "classified_reviews.csv"

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
    
    def truncate_review(self, text: str, max_words: int = 50) -> str:
        words = text.split()
        return " ".join(words[:max_words])
    
    def generate_batch_jsonl_with_chunking(self, csv_path: str, output_dir: str, chunk_size: int = 50) -> list:
        df = pd.read_csv(csv_path)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        chunk_paths = []
        for chunk_index in range(0, len(df), chunk_size):
            chunk_df = df.iloc[chunk_index:chunk_index + chunk_size]
            chunk_file = Path(output_dir) / f"batch_chunk_{chunk_index // chunk_size}.jsonl"
            with open(chunk_file, "w", encoding="utf-8") as file:
                for index, row in chunk_df.iterrows():
                    review = self.truncate_review(str(row.get("review", "")))
                    if not review.strip() or review.strip().lower() in {"nan", "none"}:
                        continue
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
            chunk_paths.append(str(chunk_file))

        logger.info(f"{len(chunk_paths)} Batch-Dateien wurden erstellt.")
        return chunk_paths

class OpenAIBatchRunner:
    def __init__(self, api_key: str):
        if not api_key:
            raise ValueError("API key not found. Please set the OPENAI_API_KEY environment variable.")
        self.client = OpenAI(api_key=api_key)

    def run_batch_job(self, jsonl_path: str) -> List[str]:
        uploaded_file = self.upload_batch_file(jsonl_path)
        batch_job = self.start_batch(uploaded_file.id)
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

    # Chunks erzeugen
    chunk_paths = builder.generate_batch_jsonl_with_chunking(INPUT_FILE, output_dir="batches", chunk_size=50)

    all_results = []

    # Chunk einzeln verarbeiten
    for i, chunk_path in enumerate(chunk_paths):
        logger.info(f"Starte Verarbeitung für {chunk_path} ({i+1}/{len(chunk_paths)})")

        try:
            result_entries = runner.run_batch_job(chunk_path)
            result_df = runner.parse_results(result_entries)
            all_results.append(result_df)
        except Exception as e:
            logger.error(f"Fehler beim Verarbeiten von {chunk_path}: {e}")

    # Ergebnisse zusammenführen
    if all_results:
        final_results = pd.concat(all_results, ignore_index=True)
        input_df = pd.read_csv(INPUT_FILE)
        input_df.insert(3, "food_rating", final_results["food"])
        input_df.insert(4, "service_rating", final_results["service"])
        input_df.insert(5, "atmosphere_rating", final_results["atmosphere"])
        input_df.to_csv(OUTPUT_FILE, index=False)
        logger.info("Klassifizierte Reviews gespeichert in classified_reviews.csv.")

if __name__ == "__main__":
    main()
