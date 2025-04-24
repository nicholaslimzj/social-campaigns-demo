# Campaign Analytics Dashboard

A weekend project I built while exploring advertising analytics. I wanted to get hands-on with the challenges of extracting value from campaign data through a working prototype.

## What I Built

This prototype explores a few ideas for working with campaign data:

### 💰 Budget Allocation Optimizer
Suggests how to redistribute spend for better overall performance.

### 🤖 AI Insight Generator
Uses LLMs to automatically spot trends and opportunities in campaign metrics.

### 💬 Ask Questions, Get Answers
Ask about your data in plain English and get immediate answers - no SQL knowledge, no data expertise, no dashboard skills needed.

I worked with a dataset of 200,000 campaign rows containing metrics like ROI, conversion rates, and target audience information. While limited in scope, it got me thinking about the possibilities when combining Meta's campaign data with partner ecosystem data.

The Text-to-SQL interface could help cut through data silos, allowing users to query across previously disconnected datasets – making insights accessible even as data complexity increases.

---

## Technical Setup

### Prerequisites

- Python 3.10+
- Node.js 18+
- DuckDB 1.2+

### Backend Setup

```bash
# Clone the repository
git clone <repository-url>
cd meta-demo

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your API keys

# Run the backend server
python -m app.main
```

### Frontend Setup

```bash
cd meta-demo-web
npm install
npm run dev
```

### Using the Dashboard

1. Open your browser to http://localhost:3000
2. Select a company from the dropdown
3. Explore the different tabs: Overview, Cohort Analysis, Channel Analysis, and Campaign Analysis
4. Try the natural language query feature at the bottom of the page

## Architecture

- **Backend**: Python Flask API with DuckDB for data storage and analysis
- **Frontend**: Next.js with TypeScript and Tailwind CSS
- **Data Pipeline**: dbt for transformations and analytics
- **AI Features**: LangChain for insights generation, Vanna for natural language queries

## License

MIT