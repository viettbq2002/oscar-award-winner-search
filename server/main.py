from fastapi import FastAPI, HTTPException
from typing import List, Dict, Any
import json
import os
from pathlib import Path

from app.search.es_client import EsClient

app = FastAPI(title="Movie Search API", description="API for searching Oscar Best Picture winners")
es_client = EsClient()

# Index name for our movie data
MOVIE_INDEX = "oscar_movies"


def create_movie_mapping() -> Dict:
    """Create mapping for movie documents."""
    return {
        "properties": {
            "name": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "oscar": {
                "type": "integer"
            },
            "released_year": {
                "type": "integer"
            },
            "poster": {
                "type": "keyword"
            },
            "rating": {
                "type": "keyword"
            },
            "duration": {
                "type": "text"
            },
            "genre": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "summary": {
                "type": "text",
                "analyzer": "standard"
            },
            "directors": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "stars": {
                "type": "text",
                "analyzer": "standard",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            }
        }
    }


@app.get("/")
async def root():
    return {
        "message": "Oscar Movies Search API", 
        "endpoints": {
            "create_index": "/create-index",
            "load_data": "/load-data", 
            "search": "/search/{query}",
            "fuzzy_search": "/fuzzy-search/{query}?fuzziness=AUTO",
            "advanced_fuzzy_search": "/advanced-fuzzy-search/{query}?fuzziness=AUTO&boost_exact=2.0",
            "wildcard_search": "/wildcard-search?pattern=*query*&field=name",
            "suggestions": "/suggest/{text}?field=name",
            "movies": "/movies",
            "movies_by_year": "/movies/by-year/{year}",
            "genres": "/movies/genres",
            "health": "/health"
        },
        "search_examples": {
            "exact": "/search/parasite",
            "fuzzy": "/fuzzy-search/parasit (handles typos)",
            "wildcard": "/wildcard-search?pattern=para*&field=name",
            "suggestions": "/suggest/parasit"
        }
    }


@app.post("/create-index")
async def create_index():
    """Create the movies index with proper mapping."""
    try:
        mapping = create_movie_mapping()
        success = es_client.create_index(MOVIE_INDEX, mapping)
        
        if success:
            return {"message": f"Index '{MOVIE_INDEX}' created successfully", "status": "success"}
        else:
            raise HTTPException(status_code=500, detail="Failed to create index")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating index: {str(e)}")


@app.post("/load-data")
async def load_movie_data():
    """Load Oscar movie data from JSON file into Elasticsearch."""
    try:
        # Path to the JSON file
        json_file_path = Path("data/oscar-best-picture-award-winners.json")
        
        if not json_file_path.exists():
            raise HTTPException(status_code=404, detail="JSON file not found")
        
        # Read the JSON data
        with open(json_file_path, 'r', encoding='utf-8') as file:
            movies = json.load(file)
        
        # Bulk index the documents
        success = es_client.bulk_index_documents(MOVIE_INDEX, movies)
        
        if success:
            return {
                "message": f"Successfully loaded {len(movies)} movies into index '{MOVIE_INDEX}'",
                "status": "success",
                "count": len(movies)
            }
        else:
            raise HTTPException(status_code=500, detail="Failed to load data")
            
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="JSON file not found")
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON format")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading data: {str(e)}")


@app.get("/search/{query}")
async def search_movies(query: str, size: int = 10):
    """Search for movies by title, genre, director, or summary."""
    try:
        # Search across multiple fields
        results = es_client.simple_search(
            MOVIE_INDEX, 
            query, 
            fields=["name^2", "genre", "directors", "stars", "summary"]
        )
        
        if results is None:
            return {"movies": [], "count": 0, "query": query}
        
        return {
            "movies": results[:size],
            "count": len(results),
            "query": query,
            "search_type": "exact"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")


@app.get("/fuzzy-search/{query}")
async def fuzzy_search_movies(query: str, size: int = 10, fuzziness: str = "AUTO"):
    """Fuzzy search for movies - tolerates typos and variations."""
    try:
        # Fuzzy search across multiple fields
        results = es_client.fuzzy_search(
            MOVIE_INDEX, 
            query, 
            fields=["name^2", "genre", "directors", "stars", "summary"],
            fuzziness=fuzziness,
            size=size
        )
        
        if results is None:
            return {"movies": [], "count": 0, "query": query}
        
        return {
            "movies": results,
            "count": len(results),
            "query": query,
            "fuzziness": fuzziness,
            "search_type": "fuzzy"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Fuzzy search error: {str(e)}")


@app.get("/advanced-fuzzy-search/{query}")
async def advanced_fuzzy_search_movies(query: str, size: int = 10, fuzziness: str = "AUTO", boost_exact: float = 2.0):
    """Advanced fuzzy search combining exact and fuzzy matching."""
    try:
        # Advanced fuzzy search with boosting
        results = es_client.advanced_fuzzy_search(
            MOVIE_INDEX, 
            query, 
            fields=["name^3", "genre^2", "directors^2", "stars", "summary"],
            fuzziness=fuzziness,
            boost_exact=boost_exact,
            size=size
        )
        
        if results is None:
            return {"movies": [], "count": 0, "query": query}
        
        return {
            "movies": results,
            "count": len(results),
            "query": query,
            "fuzziness": fuzziness,
            "boost_exact": boost_exact,
            "search_type": "advanced_fuzzy"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Advanced fuzzy search error: {str(e)}")


@app.get("/wildcard-search")
async def wildcard_search_movies(pattern: str, field: str = "name", size: int = 10):
    """Wildcard search using * and ? characters."""
    try:
        results = es_client.wildcard_search(MOVIE_INDEX, pattern, field, size)
        
        if results is None:
            return {"movies": [], "count": 0, "pattern": pattern}
        
        return {
            "movies": results,
            "count": len(results),
            "pattern": pattern,
            "field": field,
            "search_type": "wildcard"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Wildcard search error: {str(e)}")


@app.get("/suggest/{text}")
async def get_suggestions(text: str, field: str = "name", size: int = 5):
    """Get search suggestions for misspelled terms."""
    try:
        suggestions = es_client.suggest_search(MOVIE_INDEX, text, field, size)
        
        if suggestions is None:
            return {"suggestions": [], "text": text}
        
        return {
            "suggestions": suggestions,
            "text": text,
            "field": field,
            "count": len(suggestions)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Suggestion error: {str(e)}")


@app.get("/movies")
async def get_all_movies(limit: int = 20, offset: int = 0):
    """Get all movies with pagination."""
    try:
        # Search for all documents
        query = {"match_all": {}}
        response = es_client.search(MOVIE_INDEX, query, size=limit, from_=offset)
        
        if response is None:
            return {"movies": [], "total": 0}
        
        movies = [hit["_source"] for hit in response["hits"]["hits"]]
        total = response["hits"]["total"]["value"] if "total" in response["hits"] else len(movies)
        
        return {
            "movies": movies,
            "total": total,
            "limit": limit,
            "offset": offset
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving movies: {str(e)}")


@app.get("/movies/by-year/{year}")
async def get_movies_by_year(year: int):
    """Get movies by Oscar year."""
    try:
        query = {
            "term": {
                "oscar": year
            }
        }
        
        response = es_client.search(MOVIE_INDEX, query)
        
        if response is None:
            return {"movies": [], "year": year}
        
        movies = [hit["_source"] for hit in response["hits"]["hits"]]
        
        return {
            "movies": movies,
            "year": year,
            "count": len(movies)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving movies by year: {str(e)}")


@app.get("/movies/genres")
async def get_genres():
    """Get all unique genres."""
    try:
        query = {
            "aggs": {
                "genres": {
                    "terms": {
                        "field": "genre.keyword",
                        "size": 100
                    }
                }
            },
            "size": 0
        }
        
        response = es_client.client.search(index=MOVIE_INDEX, body=query)
        
        if response and "aggregations" in response:
            genres = [bucket["key"] for bucket in response["aggregations"]["genres"]["buckets"]]
            return {"genres": genres, "count": len(genres)}
        
        return {"genres": [], "count": 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving genres: {str(e)}")


@app.delete("/delete-index")
async def delete_index():
    """Delete the movies index."""
    try:
        success = es_client.delete_index(MOVIE_INDEX)
        
        if success:
            return {"message": f"Index '{MOVIE_INDEX}' deleted successfully", "status": "success"}
        else:
            raise HTTPException(status_code=500, detail="Failed to delete index")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting index: {str(e)}")


@app.get("/health")
async def health_check():
    """Check the health of Elasticsearch connection."""
    try:
        if es_client.client.ping():
            return {"status": "healthy", "elasticsearch": "connected"}
        else:
            return {"status": "unhealthy", "elasticsearch": "disconnected"}
    except Exception as e:
        return {"status": "error", "error": str(e)}
