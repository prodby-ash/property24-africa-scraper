FROM apify/actor-python:3.11

# Copy and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY . ./

# Precompile Python code
RUN python3 -m compileall -q src/ 2>/dev/null || true

# Create non-root user
RUN useradd --create-home apify && chown -R apify:apify ./
USER apify

# Launch the actor
CMD ["python3", "-m", "src"]