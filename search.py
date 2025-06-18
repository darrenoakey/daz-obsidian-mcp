#!/usr/bin/env python3
"""
Test script to verify Obsidian search functionality
Run this after the indexer has completed initial indexing
"""

from pathlib import Path
import chromadb
from chromadb.config import Settings


def test_search():
    # Connect to ChromaDB
    script_dir = Path(__file__).parent
    db_path = script_dir / "obsidian_chromadb"

    if not db_path.exists():
        print("Error: ChromaDB database not found. Please run the indexer first.")
        return

    client = chromadb.PersistentClient(
        path=str(db_path), settings=Settings(anonymized_telemetry=False)
    )

    try:
        collection = client.get_collection("note_chunks")

        # Get collection stats
        count = collection.count()
        print(f"Total chunks in database: {count}")

        if count == 0:
            print("No chunks found. Make sure the indexer has run.")
            return

        # Test search
        test_query = "obsidian"  # Change this to test different queries
        print(f"\nSearching for: '{test_query}'")

        results = collection.query(
            query_texts=[test_query],
            n_results=5,
            include=["documents", "metadatas", "distances"],
        )

        if results["ids"][0]:
            print(f"\nFound {len(results['ids'][0])} results:")
            for i in range(len(results["ids"][0])):
                metadata = results["metadatas"][0][i]
                distance = results["distances"][0][i]
                content_preview = results["documents"][0][i][:100] + "..."

                print(f"\n--- Result {i+1} ---")
                print(f"File: {metadata['title']} ({metadata['file_path']})")
                print(f"Chunk: {metadata['chunk_index']}")
                print(f"Relevance: {1 - distance:.3f}")
                print(f"Preview: {content_preview}")
        else:
            print("No results found.")

        # Show sample of indexed files
        print("\n\nSample of indexed files:")
        all_chunks = collection.get(limit=100, include=["metadatas"])

        unique_files = set()
        for metadata in all_chunks["metadatas"]:
            unique_files.add(metadata["file_path"])
            if len(unique_files) >= 10:
                break

        for file_path in list(unique_files)[:10]:
            print(f"  - {file_path}")

        print(f"\n(Showing {len(unique_files)} of all indexed files)")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    test_search()
