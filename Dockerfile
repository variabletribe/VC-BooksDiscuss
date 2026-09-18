# Optional: use this instead of the native Python runtime if you want ASSISTANT_JOIN_VC=1
# (the assistant joining voice chats itself). It guarantees ffmpeg is present, unlike
# Render's native Python runtime buildCommand (apt-get access there isn't guaranteed).
#
# On Render: change this service's runtime to "docker" (or create a new Web Service and
# pick "Docker" as the environment) so it builds from this file instead of render.yaml's
# buildCommand/startCommand.
#
# If you don't want the VC-join feature at all, ignore this file — the native Python
# runtime in render.yaml works fine for everything else.

FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "bot.py"]
