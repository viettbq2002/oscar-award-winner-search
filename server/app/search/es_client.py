from elasticsearch import Elasticsearch
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class EsClient:
    """
    A bridge class to interact with Elasticsearch for indexing and searching documents.
    """
    
    def __init__(self, host: str = "localhost", port: int = 9200, **kwargs):
        """
        Initialize Elasticsearch connection.
        
        Args:
            host: Elasticsearch host (default: localhost)
            port: Elasticsearch port (default: 9200)
            **kwargs: Additional Elasticsearch client parameters
        """
        self.host = host
        self.port = port
        
        # Create Elasticsearch client
        try:
            self.client = Elasticsearch(
                hosts=[f"http://{host}:{port}"],
                verify_certs=False,
                ssl_show_warn=False,
                request_timeout=30,
                **kwargs
            )
            # Test connection
            if self.client.ping():
                logger.info(f"Connected to Elasticsearch at {host}:{port}")
                logger.info(self.client.info())
            else:
                logger.info(self.client.info())
                logger.error("Failed to connect to Elasticsearch")
        except Exception as e:
            logger.error(f"Error connecting to Elasticsearch: {e}")
            raise
    
    def create_index(self, index_name: str, mapping: Optional[Dict] = None) -> bool:
        """
        Create an index with optional mapping.
        
        Args:
            index_name: Name of the index to create
            mapping: Optional index mapping configuration
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if self.client.indices.exists(index=index_name):
                logger.info(f"Index '{index_name}' already exists")
                return True
                
            body = {}
            if mapping:
                body["mappings"] = mapping
                
            self.client.indices.create(index=index_name, body=body)
            logger.info(f"Index '{index_name}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Error creating index '{index_name}': {e}")
            return False
    
    def delete_index(self, index_name: str) -> bool:
        """
        Delete an index.
        
        Args:
            index_name: Name of the index to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self.client.indices.exists(index=index_name):
                logger.info(f"Index '{index_name}' does not exist")
                return True
                
            self.client.indices.delete(index=index_name)
            logger.info(f"Index '{index_name}' deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Error deleting index '{index_name}': {e}")
            return False
    
    def index_document(self, index_name: str, document: Dict, doc_id: Optional[str] = None) -> Optional[str]:
        """
        Index a single document.
        
        Args:
            index_name: Name of the index
            document: Document to index
            doc_id: Optional document ID
            
        Returns:
            str: Document ID if successful, None otherwise
        """
        try:
            # Add timestamp if not present
            if "timestamp" not in document:
                document["timestamp"] = datetime.now().isoformat()
                
            response = self.client.index(
                index=index_name,
                body=document,
                id=doc_id
            )
            logger.debug(f"Document indexed with ID: {response['_id']}")
            return response["_id"]
        except Exception as e:
            logger.error(f"Error indexing document: {e}")
            return None
    
    def bulk_index_documents(self, index_name: str, documents: List[Dict]) -> bool:
        """
        Bulk index multiple documents.
        
        Args:
            index_name: Name of the index
            documents: List of documents to index
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            from elasticsearch.helpers import bulk
            
            actions = []
            for doc in documents:
                # Add timestamp if not present
                if "timestamp" not in doc:
                    doc["timestamp"] = datetime.now().isoformat()
                    
                action = {
                    "_index": index_name,
                    "_source": doc
                }
                actions.append(action)
            
            success, failed = bulk(self.client, actions)
            logger.info(f"Bulk indexing completed: {success} successful, {len(failed)} failed")
            return len(failed) == 0
        except Exception as e:
            logger.error(f"Error in bulk indexing: {e}")
            return False
    
    def search(self, index_name: str, query: Dict, size: int = 10, from_: int = 0) -> Optional[Dict]:
        """
        Search documents in an index.
        
        Args:
            index_name: Name of the index to search
            query: Elasticsearch query DSL
            size: Number of results to return
            from_: Starting offset for pagination
            
        Returns:
            Dict: Search results or None if error
        """
        try:
            response = self.client.search(
                index=index_name,
                body={"query": query},
                size=size,
                from_=from_
            )
            return response
        except Exception as e:
            logger.error(f"Error searching index '{index_name}': {e}")
            return None
    
    def simple_search(self, index_name: str, search_term: str, fields: Optional[List[str]] = None) -> Optional[List[Dict]]:
        """
        Simple text search across specified fields.
        
        Args:
            index_name: Name of the index to search
            search_term: Text to search for
            fields: List of fields to search in (if None, searches all fields)
            
        Returns:
            List[Dict]: List of matching documents or None if error
        """
        try:
            if fields:
                query = {
                    "multi_match": {
                        "query": search_term,
                        "fields": fields
                    }
                }
            else:
                query = {
                    "query_string": {
                        "query": search_term
                    }
                }
            
            response = self.search(index_name, query)
            if response:
                return [hit["_source"] for hit in response["hits"]["hits"]]
            return None
        except Exception as e:
            logger.error(f"Error in simple search: {e}")
            return None
    
    def fuzzy_search(self, index_name: str, search_term: str, fields: Optional[List[str]] = None, 
                     fuzziness: str = "AUTO", size: int = 10) -> Optional[List[Dict]]:
        """
        Fuzzy search that tolerates typos and variations in search terms.
        
        Args:
            index_name: Name of the index to search
            search_term: Text to search for
            fields: List of fields to search in (if None, searches all fields)
            fuzziness: Controls how fuzzy the search is. Options:
                      - "AUTO" (recommended): automatically adjusts based on term length
                      - "0", "1", "2": exact number of character edits allowed
                      - "AUTO:3,6": AUTO with custom min/max term lengths
            size: Number of results to return
            
        Returns:
            List[Dict]: List of matching documents with scores or None if error
        """
        try:
            if fields:
                query = {
                    "multi_match": {
                        "query": search_term,
                        "fields": fields,
                        "fuzziness": fuzziness,
                        "prefix_length": 1,  # First character must match exactly
                        "max_expansions": 50  # Limit term expansions for performance
                    }
                }
            else:
                # For single field fuzzy search, use fuzzy query
                query = {
                    "fuzzy": {
                        "_all": {
                            "value": search_term,
                            "fuzziness": fuzziness,
                            "prefix_length": 1,
                            "max_expansions": 50
                        }
                    }
                }
            
            response = self.search(index_name, query, size=size)
            if response:
                # Return results with scores for fuzzy matching
                results = []
                for hit in response["hits"]["hits"]:
                    result = hit["_source"].copy()
                    result["_score"] = hit["_score"]
                    result["_id"] = hit["_id"]
                    results.append(result)
                return results
            return None
        except Exception as e:
            logger.error(f"Error in fuzzy search: {e}")
            return None
    
    def advanced_fuzzy_search(self, index_name: str, search_term: str, fields: Optional[List[str]] = None,
                             fuzziness: str = "AUTO", boost_exact: float = 2.0, size: int = 10) -> Optional[List[Dict]]:
        """
        Advanced fuzzy search that combines exact and fuzzy matching with different scoring.
        
        Args:
            index_name: Name of the index to search
            search_term: Text to search for
            fields: List of fields to search in
            fuzziness: Fuzziness parameter
            boost_exact: Boost factor for exact matches
            size: Number of results to return
            
        Returns:
            List[Dict]: List of matching documents with scores
        """
        try:
            if not fields:
                fields = ["_all"]
                
            query = {
                "bool": {
                    "should": [
                        # Exact match with high boost
                        {
                            "multi_match": {
                                "query": search_term,
                                "fields": fields,
                                "type": "phrase",
                                "boost": boost_exact
                            }
                        },
                        # Fuzzy match
                        {
                            "multi_match": {
                                "query": search_term,
                                "fields": fields,
                                "fuzziness": fuzziness,
                                "prefix_length": 1,
                                "max_expansions": 50
                            }
                        },
                        # Partial match
                        {
                            "multi_match": {
                                "query": search_term,
                                "fields": fields,
                                "type": "phrase_prefix"
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            }
            
            response = self.search(index_name, query, size=size)
            if response:
                results = []
                for hit in response["hits"]["hits"]:
                    result = hit["_source"].copy()
                    result["_score"] = hit["_score"]
                    result["_id"] = hit["_id"]
                    results.append(result)
                return results
            return None
        except Exception as e:
            logger.error(f"Error in advanced fuzzy search: {e}")
            return None
    
    def wildcard_search(self, index_name: str, pattern: str, field: str, size: int = 10) -> Optional[List[Dict]]:
        """
        Wildcard search using * and ? characters.
        
        Args:
            index_name: Name of the index to search
            pattern: Search pattern with wildcards (* for multiple chars, ? for single char)
            field: Field to search in
            size: Number of results to return
            
        Returns:
            List[Dict]: List of matching documents
        """
        try:
            query = {
                "wildcard": {
                    f"{field}.keyword": {
                        "value": pattern,
                        "case_insensitive": True
                    }
                }
            }
            
            response = self.search(index_name, query, size=size)
            if response:
                return [hit["_source"] for hit in response["hits"]["hits"]]
            return None
        except Exception as e:
            logger.error(f"Error in wildcard search: {e}")
            return None
    
    def regexp_search(self, index_name: str, pattern: str, field: str, size: int = 10) -> Optional[List[Dict]]:
        """
        Regular expression search.
        
        Args:
            index_name: Name of the index to search
            pattern: Regular expression pattern
            field: Field to search in
            size: Number of results to return
            
        Returns:
            List[Dict]: List of matching documents
        """
        try:
            query = {
                "regexp": {
                    f"{field}.keyword": {
                        "value": pattern,
                        "case_insensitive": True
                    }
                }
            }
            
            response = self.search(index_name, query, size=size)
            if response:
                return [hit["_source"] for hit in response["hits"]["hits"]]
            return None
        except Exception as e:
            logger.error(f"Error in regexp search: {e}")
            return None
    
    def suggest_search(self, index_name: str, text: str, field: str, size: int = 5) -> Optional[List[str]]:
        """
        Get search suggestions/corrections for misspelled terms.
        
        Args:
            index_name: Name of the index to search
            text: Text to get suggestions for
            field: Field to base suggestions on
            size: Number of suggestions to return
            
        Returns:
            List[str]: List of suggested terms
        """
        try:
            suggest_body = {
                "suggest": {
                    "text": text,
                    "simple_phrase": {
                        "phrase": {
                            "field": field,
                            "size": size,
                            "gram_size": 3,
                            "direct_generator": [{
                                "field": field,
                                "suggest_mode": "missing",
                                "min_word_length": 1
                            }]
                        }
                    }
                }
            }
            
            response = self.client.search(index=index_name, body=suggest_body)
            
            if response and "suggest" in response:
                suggestions = []
                for option in response["suggest"]["simple_phrase"][0]["options"]:
                    suggestions.append(option["text"])
                return suggestions
            return []
        except Exception as e:
            logger.error(f"Error in suggest search: {e}")
            return None
    
    def get_document(self, index_name: str, doc_id: str) -> Optional[Dict]:
        """
        Get a specific document by ID.
        
        Args:
            index_name: Name of the index
            doc_id: Document ID
            
        Returns:
            Dict: Document data or None if not found
        """
        try:
            response = self.client.get(index=index_name, id=doc_id)
            return response["_source"]
        except Exception as e:
            logger.error(f"Error getting document '{doc_id}': {e}")
            return None
    
    def update_document(self, index_name: str, doc_id: str, updates: Dict) -> bool:
        """
        Update a document by ID.
        
        Args:
            index_name: Name of the index
            doc_id: Document ID
            updates: Fields to update
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.client.update(
                index=index_name,
                id=doc_id,
                body={"doc": updates}
            )
            logger.debug(f"Document '{doc_id}' updated successfully")
            return True
        except Exception as e:
            logger.error(f"Error updating document '{doc_id}': {e}")
            return False
    
    def delete_document(self, index_name: str, doc_id: str) -> bool:
        """
        Delete a document by ID.
        
        Args:
            index_name: Name of the index
            doc_id: Document ID
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.client.delete(index=index_name, id=doc_id)
            logger.debug(f"Document '{doc_id}' deleted successfully")
            return True
        except Exception as e:
            logger.error(f"Error deleting document '{doc_id}': {e}")
            return False
    
    def get_index_stats(self, index_name: str) -> Optional[Dict]:
        """
        Get statistics for an index.
        
        Args:
            index_name: Name of the index
            
        Returns:
            Dict: Index statistics or None if error
        """
        try:
            response = self.client.indices.stats(index=index_name)
            return response
        except Exception as e:
            logger.error(f"Error getting stats for index '{index_name}': {e}")
            return None
    
    def close(self):
        """
        Close the Elasticsearch connection.
        """
        try:
            self.client.close()
            logger.info("Elasticsearch connection closed")
        except Exception as e:
            logger.error(f"Error closing Elasticsearch connection: {e}")


# Example usage and helper functions
def create_text_mapping() -> Dict:
    """
    Create a basic text mapping for documents with title and content.
    
    Returns:
        Dict: Elasticsearch mapping configuration
    """
    return {
        "properties": {
            "title": {
                "type": "text",
                "analyzer": "standard"
            },
            "content": {
                "type": "text",
                "analyzer": "standard"
            },
            "tags": {
                "type": "keyword"
            },
            "timestamp": {
                "type": "date"
            },
            "category": {
                "type": "keyword"
            }
        }
    }


