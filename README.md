# Oscar Award Winner Search

A full-stack application for searching Oscar Best Picture award winners using Elasticsearch, FastAPI, and React.

## Project Structure

```
just-to-learn-search-tech/
├── server/                 # Backend FastAPI application
│   ├── main.py            # Main FastAPI application
│   ├── app/               # Application modules
│   │   └── search/        # Elasticsearch client
│   └── data/              # Oscar movies JSON data
├── web-app/               # Frontend React application
│   ├── src/               # React source code
│   └── package.json       # Frontend dependencies
├── Procfile              # Process definitions for running both services
└── docker-compose.yml    # Docker configuration
```

## Prerequisites

Before running the application, make sure you have the following installed:

- **Python 3.12+** 
- **uv** (Python package manager)
- **Node.js** and **bun** (for frontend)
- **node-foreman** (`npm install -g foreman`) 
- **Elasticsearch** (running on localhost:9200)

## Quick Start

### Option 1: Using Procfile (Recommended)

2. **Start both services at once**
   ```bash
   nf start
   ```

   This will start:
   - Backend API at `http://localhost:8000`
   - Frontend at `http://localhost:5173`

### Option 2: Manual Setup

#### Backend Setup

1. **Navigate to server directory**
   ```bash
   cd server
   ```

2. **Install Python dependencies**
   ```bash
   uv sync
   ```

3. **Start the FastAPI server**
   ```bash
   uv run fastapi dev
   ```

   The API will be available at `http://localhost:8000`

#### Frontend Setup

1. **Navigate to web-app directory**
   ```bash
   cd web-app
   ```

2. **Install dependencies**
   ```bash
   bun install
   ```

3. **Start the development server**
   ```bash
   bun run dev
   ```

   The frontend will be available at `http://localhost:5173`

## API Setup

### 1. Create Elasticsearch Index
```bash
curl -X POST http://localhost:8000/create-index
```

### 2. Load Movie Data
```bash
curl -X POST http://localhost:8000/load-data
```

## API Documentation

Once the backend is running, you can access:

- **API Documentation**: `http://localhost:8000/docs`
- **Alternative API Docs**: `http://localhost:8000/redoc`

## Available Endpoints

- `GET /` - API information and available endpoints
- `POST /create-index` - Create Elasticsearch index
- `POST /load-data` - Load Oscar movies data
- `GET /search/{query}` - Basic search
- `GET /fuzzy-search/{query}` - Fuzzy search with typo tolerance
- `GET /wildcard-search` - Wildcard pattern search
- `GET /suggest/{text}` - Get search suggestions
- `GET /movies` - Get all movies
- `GET /movies/by-year/{year}` - Get movies by year
- `GET /movies/genres` - Get all genres

## Search Examples

- **Exact search**: `http://localhost:8000/search/parasite`
- **Fuzzy search**: `http://localhost:8000/fuzzy-search/parasit`
- **Wildcard search**: `http://localhost:8000/wildcard-search?pattern=para*&field=name`
- **Suggestions**: `http://localhost:8000/suggest/parasit`



## Development

### Backend Development
- The FastAPI server auto-reloads on file changes
- Check logs in the terminal for any errors
- Use the `/docs` endpoint for interactive API testing

### Frontend Development
- The React app auto-reloads on file changes
- Built with Vite for fast development experience
- Uses Tailwind CSS for styling

## Tech Stack

- **Backend**: FastAPI, Python, Elasticsearch
- **Frontend**: React, TypeScript, Vite, Tailwind CSS
- **Package Managers**: uv (Python), bun (Node.js)
- **Process Management**: node-foreman

## License

This project is for learning purposes.
