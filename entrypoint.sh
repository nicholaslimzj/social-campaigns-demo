#!/bin/bash
echo "Meta Demo Application"

# Always ensure packages are installed
echo "Ensuring all packages are installed..."
pip install --no-cache-dir -r /requirements.txt

echo "Available commands:"
echo "  python -m app.main check     - Check data files and environment"
echo "  python -m app.main process   - Process data (CSV to parquet)"
echo "  python -m app.main dbt       - Run dbt models to create views and tables"
echo "  python -m app.main dashboard - Start the dashboard"
echo "  python -m app.main serve     - Start a simple web server"
echo "  python -m app.main vanna     - Start Vanna natural language to SQL CLI"
echo "  python -m app.main llamaindex - Start LlamaIndex natural language to SQL CLI"
echo ""
echo "dbt commands:"
echo "  dbt run               - Run all dbt models"
echo "  dbt run --select name - Run a specific model"
echo "  dbt docs generate     - Generate documentation"
echo ""
echo "Starting with default command (check environment)..."
python -m app.main check
echo ""
echo "Container is now running. Use docker exec to run commands."
echo "Press Ctrl+C to stop the container"
echo ""
# Keep container running
tail -f /dev/null