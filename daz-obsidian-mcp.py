#!/usr/bin/env python3
"""
MCP Server for Obsidian ChromaDB Search
Provides search_snippets and search_full functions via Model Context Protocol
"""

import json
import logging
import asyncio
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

import chromadb
from chromadb.config import Settings
from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.types import Tool, TextContent, ServerCapabilities, ToolsCapability
from mcp.server.stdio import stdio_server

# Set up logging to stderr so it doesn't interfere with stdio communication
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger(__name__)

# Constants from the watcher script
COLLECTION_NAME = "note_chunks"
DEFAULT_RESULTS_LIMIT = 10


class ObsidianSearchServer:
    def __init__(self, db_path: Path):
        self.db_path = db_path

        # Initialize ChromaDB client
        self.client = chromadb.PersistentClient(
            path=str(db_path), settings=Settings(anonymized_telemetry=False)
        )

        # Get collection
        try:
            self.collection = self.client.get_collection(COLLECTION_NAME)
            logger.info(f"Connected to collection: {COLLECTION_NAME}")
        except Exception as e:
            logger.error(f"Failed to connect to collection: {e}")
            raise

    def search_snippets(
        self, query: str, limit: int = DEFAULT_RESULTS_LIMIT
    ) -> List[Dict[str, Any]]:
        """
        Search for relevant chunks and return them
        """
        try:
            # Perform vector search
            results = self.collection.query(
                query_texts=[query],
                n_results=limit,
                include=["documents", "metadatas", "distances"],
            )

            if not results["ids"][0]:
                return []

            # Format results
            snippets = []
            for i in range(len(results["ids"][0])):
                snippet = {
                    "chunk_id": results["ids"][0][i],
                    "content": results["documents"][0][i],
                    "metadata": results["metadatas"][0][i],
                    "distance": results["distances"][0][i],
                }
                snippets.append(snippet)

            return snippets

        except Exception as e:
            logger.error(f"Error searching snippets: {e}")
            return []

    def search_full(
        self, query: str, limit: int = DEFAULT_RESULTS_LIMIT
    ) -> List[Dict[str, Any]]:
        """
        Search for relevant chunks and return full articles
        """
        try:
            # First get relevant chunks
            chunk_results = self.collection.query(
                query_texts=[query],
                n_results=limit * 3,  # Get more chunks to find unique articles
                include=["metadatas", "distances"],
            )

            if not chunk_results["ids"][0]:
                return []

            # Collect unique file paths with their best distances
            file_scores = {}
            for i in range(len(chunk_results["ids"][0])):
                metadata = chunk_results["metadatas"][0][i]
                file_path = metadata["file_path"]
                distance = chunk_results["distances"][0][i]

                if file_path not in file_scores or distance < file_scores[file_path]:
                    file_scores[file_path] = distance

            # Sort by distance and limit
            sorted_files = sorted(file_scores.items(), key=lambda x: x[1])[:limit]

            # Get all chunks for each file and reconstruct
            articles = []
            for file_path, best_distance in sorted_files:
                # Get all chunks for this file
                file_chunks = self.collection.get(
                    where={"file_path": file_path}, include=["documents", "metadatas"]
                )

                if not file_chunks["ids"]:
                    continue

                # Sort chunks by index
                chunks_data = []
                for i in range(len(file_chunks["ids"])):
                    chunks_data.append(
                        {
                            "index": file_chunks["metadatas"][i]["chunk_index"],
                            "content": file_chunks["documents"][i],
                            "metadata": file_chunks["metadatas"][i],
                        }
                    )

                chunks_data.sort(key=lambda x: x["index"])

                # Skip title chunk (index 0) and reconstruct content
                content_parts = []
                for chunk in chunks_data[1:]:  # Skip title chunk
                    content_parts.append(chunk["content"])

                # Use simple concatenation since chunks overlap
                # In a production system, you'd want to deduplicate the overlap
                full_content = "".join(content_parts)

                # Get metadata from first chunk
                first_chunk_metadata = chunks_data[0]["metadata"]

                article = {
                    "file_path": file_path,
                    "title": first_chunk_metadata["title"],
                    "full_path": first_chunk_metadata["full_path"],
                    "content": full_content,
                    "relevance_score": 1 - best_distance,  # Convert distance to score
                }
                articles.append(article)

            return articles

        except Exception as e:
            logger.error(f"Error searching full articles: {e}")
            return []


# Initialize global variables
app = Server("obsidian-search")
search_server: Optional[ObsidianSearchServer] = None


@app.list_tools()
async def list_tools() -> List[Tool]:
    """List available tools"""
    return [
        Tool(
            name="search_snippets",
            description="Search for relevant chunks/snippets from Obsidian notes",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query string"},
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return",
                        "default": DEFAULT_RESULTS_LIMIT,
                    },
                },
                "required": ["query"],
            },
        ),
        Tool(
            name="search_full",
            description="Search for relevant notes and return full article content",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query string"},
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of articles to return",
                        "default": DEFAULT_RESULTS_LIMIT,
                    },
                },
                "required": ["query"],
            },
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls"""
    global search_server

    if search_server is None:
        # Initialize search server on first use
        script_dir = Path(__file__).parent
        db_path = script_dir / "obsidian_chromadb"

        if not db_path.exists():
            return [
                TextContent(
                    type="text",
                    text="Error: ChromaDB database not found. Please run the indexer first.",
                )
            ]

        try:
            search_server = ObsidianSearchServer(db_path)
        except Exception as e:
            return [
                TextContent(
                    type="text", text=f"Error initializing search server: {str(e)}"
                )
            ]

    if name == "search_snippets":
        query = arguments.get("query", "")
        limit = arguments.get("limit", DEFAULT_RESULTS_LIMIT)

        results = search_server.search_snippets(query, limit)

        if not results:
            return [TextContent(type="text", text="No results found for your query.")]

        # Format results for display
        formatted_results = []
        for i, result in enumerate(results):
            formatted = f"**Result {i+1}:**\n"
            formatted += f"File: {result['metadata']['title']} ({result['metadata']['file_path']})\n"
            formatted += f"Chunk: {result['metadata']['chunk_index']}\n"
            formatted += f"Relevance: {1 - result['distance']:.3f}\n"
            formatted += f"Content:\n{result['content']}\n"
            formatted += "-" * 40
            formatted_results.append(formatted)

        return [TextContent(type="text", text="\n".join(formatted_results))]

    elif name == "search_full":
        query = arguments.get("query", "")
        limit = arguments.get("limit", DEFAULT_RESULTS_LIMIT)

        results = search_server.search_full(query, limit)

        if not results:
            return [TextContent(type="text", text="No results found for your query.")]

        # Format results for display
        formatted_results = []
        for i, article in enumerate(results):
            formatted = f"**Article {i+1}: {article['title']}**\n"
            formatted += f"Path: {article['file_path']}\n"
            formatted += f"Relevance Score: {article['relevance_score']:.3f}\n"
            formatted += f"\n{article['content']}\n"
            formatted += "=" * 60
            formatted_results.append(formatted)

        return [TextContent(type="text", text="\n\n".join(formatted_results))]

    else:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]


async def main():
    """Run the MCP server"""
    logger.info("Starting Obsidian Search MCP Server...")

    # Run the server
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="obsidian-search",
                server_version="1.0.0",
                capabilities=ServerCapabilities(tools=ToolsCapability()),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
