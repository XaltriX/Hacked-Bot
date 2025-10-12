# Use official Python image
FROM python:3.12

# Set work directory
WORKDIR /app

# Copy your bot code
COPY . /app

# Install dependencies
RUN pip install --no-cache-dir pyTelegramBotAPI

# If you use other dependencies, add them here:
# RUN pip install -r requirements.txt

# The main script to run
CMD ["python", "bot.py"]
