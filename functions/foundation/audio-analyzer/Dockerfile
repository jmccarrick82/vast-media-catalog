FROM media-catalog-base:latest

RUN pip install --no-cache-dir pyacoustid speechbrain torchaudio

COPY functions/foundation/audio-analyzer/main.py /app/main.py

ENTRYPOINT ["python", "/app/main.py"]
